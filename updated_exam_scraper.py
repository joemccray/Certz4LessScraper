#!/usr/bin/env python3
"""
exam_scraper_v9.1_postgres.py

Hybrid Exam Scraper with direct Postgres DB support.

✅ Changes:
- Removed Supabase dependency
- Added psycopg2 database connection
- Automatic table creation
- Logs inserted MCQs
"""

import os
import sys
import json
import time
import signal
import logging
import argparse
import asyncio
import yaml
import shutil
import httpx
import psycopg2
from psycopg2.extras import execute_values
from logging.handlers import RotatingFileHandler
from concurrent.futures import ProcessPoolExecutor
from typing import List, Dict, Any

from dotenv import load_dotenv
from pydantic import ValidationError

# --- Modular Imports ---
from vendors import WORKER_MAP, DISCOVERY_MAP
from utils import (
    setup_logging, send_notification, Question,
    get_task_lists, save_state
)
from config_schema import ConfigSchema

# --- Global State ---
CONFIG = {}
terminate = False
checkpoint = {"processed_tasks": {}}
db_retry_queue = []
main_logger = logging.getLogger("main")

DATABASE_URL = os.getenv("DATABASE_URL", "postgres://postgres:admin@localhost:5432/QuestionsScrapper")
db_connection = None

# ---------------------------
# DB Connection
# ---------------------------
def get_db_connection():
    global db_connection
    if db_connection is None or db_connection.closed:
        try:
            db_connection = psycopg2.connect(DATABASE_URL)
            db_connection.autocommit = True
            main_logger.info("✅ Connected to Postgres database successfully.")
        except Exception as e:
            main_logger.critical(f"❌ Could not connect to Postgres DB: {e}")
            sys.exit(1)
    return db_connection

# ---------------------------
# Create Table if not exists
# ---------------------------
def init_db():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS mcqs (
            question_id TEXT PRIMARY KEY,
            question TEXT NOT NULL,
            options TEXT[],
            answer TEXT,
            source TEXT
        );
        """)
        conn.commit()
        cursor.close()
        main_logger.info("✅ Table 'mcqs' is ready in the database.")
    except Exception as e:
        main_logger.critical(f"❌ Failed to initialize DB table: {e}")
        sys.exit(1)

# ---------------------------
# Insert Batch of MCQs
# ---------------------------
async def validate_and_upsert_batch(records: List[Dict]):
    if not records:
        return 0

    valid_records = []
    validation_errors = []
    for rec in records:
        try:
            Question(**rec)
            # Convert answers to a format suitable for database storage
            answers_text = []
            correct_answer = ""
            for answer in rec.get('answers', []):
                answer_text = f"{answer.get('option', '')}. {answer.get('text', '')}"
                answers_text.append(answer_text)
                if answer.get('is_correct', False):
                    correct_answer = answer.get('option', '')
            
            valid_records.append((
                rec.get('question_id'),
                rec.get('question_text'),
                answers_text,
                correct_answer,
                rec.get('source_url')
            ))
        except ValidationError as e:
            validation_errors.append({"record_id": rec.get('question_id'), "error": e.errors()})

    if validation_errors:
        main_logger.warning(f"{len(validation_errors)} records failed validation.")

    if not valid_records:
        return 0

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        insert_query = """
            INSERT INTO mcqs (question_id, question, options, answer, source)
            VALUES %s
            ON CONFLICT (question_id) DO NOTHING
        """
        execute_values(cursor, insert_query, valid_records)
        cursor.close()

        main_logger.info(f"[DB-ENTRY] ✅ Inserted {len(valid_records)} MCQs into Postgres.")
        return len(valid_records)

    except Exception as e:
        main_logger.error(f"❌ Failed inserting MCQs batch: {e}")
        db_retry_queue.extend(valid_records)
        return 0

# ---------------------------
# Load Config
# ---------------------------
def load_config():
    global CONFIG
    try:
        with open("config.yaml", "r") as f:
            raw_config = yaml.safe_load(f)
            CONFIG.update(ConfigSchema(**raw_config).model_dump())
    except Exception as e:
        main_logger.critical(f"❌ Failed to load and validate config.yaml: {e}")
        sys.exit(1)

# ---------------------------
# Signal Handler
# ---------------------------
def handle_signal(sig, frame):
    global terminate
    if not terminate:
        main_logger.warning("SIGINT caught! Graceful shutdown initiated...")
        terminate = True

# ---------------------------
# Pre-flight checks
# ---------------------------
async def run_preflight_checks() -> bool:
    main_logger.info("🚀 Running pre-flight checks...")
    try:
        init_db()
    except Exception as e:
        main_logger.critical(f"❌ Database preflight check failed: {e}")
        return False

    if shutil.which("tesseract") is None:
        main_logger.warning("⚠️ Tesseract OCR engine not found. OCR fallback will fail.")

    main_logger.info("✅ Pre-flight checks passed!")
    return True

# ---------------------------
# Main Async Task
# ---------------------------
async def main():
    parser = argparse.ArgumentParser(description="Exam Scraper v9.1 - Postgres Edition")
    parser.add_argument("--sites", nargs='+', choices=['itexams', 'allfreedumps'],
                        default=['itexams', 'allfreedumps'], help="Run crawlers for specified sites.")
    parser.add_argument("--vendor", type=str, help="Target a specific vendor (e.g., 'Amazon').")
    parser.add_argument("--save-pdfs", action="store_true", help="Archive all downloaded PDFs.")
    args = parser.parse_args()

    load_config()
    load_dotenv()
    setup_logging(CONFIG.get("LOGS_DIR", "logs"))

    if not await run_preflight_checks():
        sys.exit(1)

    whitelist, blacklist = get_task_lists()

    async with httpx.AsyncClient(timeout=CONFIG['request_timeout']) as client:
        discovery_coroutines = [
            DISCOVERY_MAP[site](client, CONFIG) for site in args.sites
        ]
        task_results = await asyncio.gather(*discovery_coroutines, return_exceptions=True)

    all_tasks = []
    for res in task_results:
        if isinstance(res, list):
            all_tasks.extend(res)
        else:
            main_logger.error(f"Task discovery failed: {res}")

    if args.vendor:
        all_tasks = [t for t in all_tasks if t.get('vendor', '').lower() == args.vendor.lower()]
    if whitelist:
        all_tasks = [t for t in all_tasks if t['exam_code'] in whitelist]
    if blacklist:
        all_tasks = [t for t in all_tasks if t['exam_code'] not in blacklist]

    main_logger.info(f"Discovered {len(all_tasks)} tasks to process.")
    if not all_tasks:
        return

    metrics = {
        "start_time": time.time(), "total_tasks": len(all_tasks),
        "processed_tasks": 0, "questions_found": 0, "errors": 0,
        "vendor_counts": {}
    }

    with ProcessPoolExecutor(max_workers=CONFIG['max_cpu_workers']) as process_pool:
        async with httpx.AsyncClient(timeout=CONFIG['request_timeout']) as client:
            # Create a semaphore to limit concurrent requests
            semaphore = asyncio.Semaphore(CONFIG.get('max_concurrent_requests', 10))
            worker_tasks = []
            for task in all_tasks:
                worker_func = WORKER_MAP.get(task['site'])
                if worker_func:
                    coro = worker_func(
                        client=client, task=task, logger=logging.getLogger(task['site']),
                        process_pool=process_pool, config=CONFIG,
                        save_pdfs=args.save_pdfs, semaphore=semaphore
                    )
                    worker_tasks.append(asyncio.create_task(coro))

            for future in asyncio.as_completed(worker_tasks):
                try:
                    result = await future
                    if result:
                        metrics["questions_found"] += len(result)
                        await validate_and_upsert_batch(result)
                except Exception as e:
                    metrics["errors"] += 1
                    main_logger.error(f"A worker task failed: {e}")
                finally:
                    metrics["processed_tasks"] += 1

    metrics["end_time"] = time.time()
    metrics["duration_seconds"] = metrics["end_time"] - metrics["start_time"]
    save_state(checkpoint, metrics, {}, False)
    main_logger.info(f"Scrape finished. Report: {json.dumps(metrics, indent=2)}")

if __name__ == "__main__":
    try:
        signal.signal(signal.SIGINT, handle_signal)
        asyncio.run(main())
    except Exception as e:
        main_logger.exception(f"❌ Fatal error in scraper: {e}")
        send_notification(f"❌ Fatal error in scraper: {e}", CONFIG)
