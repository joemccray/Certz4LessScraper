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
from collections import Counter
from pathlib import Path
from dotenv import load_dotenv
from pydantic import ValidationError

load_dotenv()

# --- Modular Imports ---
from vendors import WORKER_MAP, DISCOVERY_MAP
from utils import setup_logging, send_notification, Question, get_task_lists, save_state
from config_schema import ConfigSchema

# --- Global State ---
CONFIG = {}
terminate = False
checkpoint = {"processed_tasks": {}}
db_retry_queue = []
main_logger = logging.getLogger("main")

DATABASE_URL = os.getenv("DATABASE_URL")
db_connection = None


# ---------------------------
# DB Connection
# ---------------------------
def get_db_connection():
    global db_connection
    if db_connection is None or db_connection.closed:
        try:
            main_logger.info(f"===== the database url is {DATABASE_URL} =====")
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

        # Create questions table
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS questions (
            question_id TEXT PRIMARY KEY,
            vendor TEXT NOT NULL,
            exam_code TEXT NOT NULL,
            exam_name TEXT NOT NULL,
            question_text TEXT NOT NULL,
            explanation TEXT,
            tags TEXT[],
            source_url TEXT,
            source_type TEXT,
            version TEXT
        );
        """
        )

        # Create answers table
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS answers (
            id SERIAL PRIMARY KEY,
            question_id TEXT REFERENCES questions(question_id) ON DELETE CASCADE,
            option TEXT NOT NULL,
            text TEXT NOT NULL,
            is_correct BOOLEAN DEFAULT FALSE
        );
        """
        )

        cursor.execute(
            """
        CREATE INDEX IF NOT EXISTS idx_questions_vendor ON questions(vendor);
        """
        )

        cursor.execute(
            """
        CREATE INDEX IF NOT EXISTS idx_questions_exam_code ON questions(exam_code);
        """
        )

        cursor.execute(
            """
        CREATE INDEX IF NOT EXISTS idx_answers_question_id ON answers(question_id);
        """
        )

        conn.commit()
        cursor.close()
        main_logger.info(
            "✅ Tables 'questions' and 'answers' are ready in the database."
        )
    except Exception as e:
        main_logger.critical(f"❌ Failed to initialize DB table: {e}")
        sys.exit(1)


# ---------------------------
# Insert Batch of MCQs
# ---------------------------
async def validate_and_upsert_batch(records: List[Dict]):
    if not records:
        return 0

    valid_questions = []
    valid_answers = []
    validation_errors = []

    for rec in records:
        try:
            q = Question(**rec)

            # Prepare question record
            valid_questions.append(
                (
                    q.question_id,
                    q.vendor,
                    q.exam_code,
                    q.exam_name,
                    q.question_text,
                    q.explanation,
                    q.tags,
                    q.source_url,
                    q.source_type,
                    q.version,
                )
            )

            # Prepare associated answers
            for ans in q.answers:
                valid_answers.append(
                    (q.question_id, ans.option, ans.text, ans.is_correct)
                )

        except ValidationError as e:
            validation_errors.append(
                {"record_id": rec.get("question_id"), "error": e.errors()}
            )

    if validation_errors:
        main_logger.warning(f"{len(validation_errors)} records failed validation.")

    if not valid_questions:
        return 0

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Insert into questions table
        insert_questions_query = """
            INSERT INTO questions (
                question_id, vendor, exam_code, exam_name, question_text,
                explanation, tags, source_url, source_type, version
            ) VALUES %s
            ON CONFLICT (question_id) DO NOTHING
        """
        execute_values(cursor, insert_questions_query, valid_questions)

        # Insert into answers table (skip if no answers)
        if valid_answers:
            insert_answers_query = """
                INSERT INTO answers (question_id, option, text, is_correct)
                VALUES %s
            """
            execute_values(cursor, insert_answers_query, valid_answers)

        cursor.close()

        main_logger.info(
            f"[DB-ENTRY] ✅ Inserted {len(valid_questions)} questions and {len(valid_answers)} answers."
        )
        return len(valid_questions)

    except Exception as e:
        main_logger.error(f"❌ Failed inserting batch into Postgres: {e}")
        db_retry_queue.extend(valid_questions)
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


def summarize_vendors(tasks: List[Dict[str, Any]]) -> Dict[str, int]:
    counts = Counter((t.get("vendor") or "Unknown").strip() for t in tasks)
    return dict(sorted(counts.items(), key=lambda kv: (-kv[1], kv[0].lower())))

def log_vendor_summary(counts: Dict[str, int]):
    if not counts:
        main_logger.info("No vendors discovered.")
        return
    total = sum(counts.values())
    unique = len(counts)
    main_logger.info("📦 Vendors discovered: %d unique across %d tasks.", unique, total)

    # Pretty log table
    width_vendor = max(6, max(len(v) for v in counts.keys()))
    main_logger.info("+" + "-"*(width_vendor+2) + "+--------+")
    main_logger.info("| %-*s | Count |", width_vendor, "Vendor")
    main_logger.info("+" + "-"*(width_vendor+2) + "+--------+")
    for vendor, cnt in counts.items():
        main_logger.info("| %-*s | %5d |", width_vendor, vendor, cnt)
    main_logger.info("+" + "-"*(width_vendor+2) + "+--------+")

def dump_vendor_summary(counts: Dict[str, int], path: str = "vendors_discovered.json"):
    try:
        Path(path).write_text(json.dumps(counts, indent=2))
        main_logger.info("💾 Wrote vendor summary to %s", path)
    except Exception as e:
        main_logger.warning("Could not write vendor summary to %s: %s", path, e)

# ---------------------------
# Main Async Task
# ---------------------------
async def main():
    parser = argparse.ArgumentParser(description="Exam Scraper v9.1 - Postgres Edition")
    parser.add_argument(
        "--list-vendors",
        action="store_true",
        help="Discover tasks, print vendors found (with counts), then exit.",
    )
    parser.add_argument(
        "--sites",
        nargs="+",
        choices=["itexams", "allfreedumps"],
        default=["itexams", "allfreedumps"],
        help="Run crawlers for specified sites.",
    )
    parser.add_argument(
        "--vendor", type=str, help="Target a specific vendor (e.g., 'Amazon')."
    )
    parser.add_argument(
        "--save-pdfs", action="store_true", help="Archive all downloaded PDFs."
    )
    args = parser.parse_args()

    load_config()
    load_dotenv()
    setup_logging(CONFIG.get("LOGS_DIR", "logs"))

    if not await run_preflight_checks():
        sys.exit(1)

    whitelist, blacklist = get_task_lists()

    async with httpx.AsyncClient(timeout=CONFIG["request_timeout"]) as client:
        discovery_coroutines = [
            DISCOVERY_MAP[site](client, CONFIG) for site in args.sites
        ]
        task_results = await asyncio.gather(
            *discovery_coroutines, return_exceptions=True
        )

    all_tasks = []
    for res in task_results:
        if isinstance(res, list):
            all_tasks.extend(res)
        else:
            main_logger.error(f"Task discovery failed: {res}")

    if args.vendor:
        all_tasks = [
            t for t in all_tasks if t.get("vendor", "").lower() == args.vendor.lower()
        ]
    if whitelist:
        all_tasks = [t for t in all_tasks if t["exam_code"] in whitelist]
    if blacklist:
        all_tasks = [t for t in all_tasks if t["exam_code"] not in blacklist]

    main_logger.info(f"Discovered {len(all_tasks)} tasks to process.")
    vendor_counts = summarize_vendors(all_tasks)
    log_vendor_summary(vendor_counts)
    dump_vendor_summary(vendor_counts)

    # If the user only wants to list vendors, stop here
    if args.list_vendors:
        return
    
    if not all_tasks:
        return

    metrics = {
        "start_time": time.time(),
        "total_tasks": len(all_tasks),
        "processed_tasks": 0,
        "questions_found": 0,
        "errors": 0,
        "vendor_counts": {},
    }

    with ProcessPoolExecutor(max_workers=CONFIG["max_cpu_workers"]) as process_pool:
        async with httpx.AsyncClient(timeout=CONFIG["request_timeout"]) as client:
            # Create a semaphore to limit concurrent requests
            semaphore = asyncio.Semaphore(CONFIG.get("max_concurrent_requests", 10))
            worker_tasks = []
            for task in all_tasks:
                worker_func = WORKER_MAP.get(task["site"])
                if worker_func:
                    coro = worker_func(
                        client=client,
                        task=task,
                        logger=logging.getLogger(task["site"]),
                        process_pool=process_pool,
                        config=CONFIG,
                        save_pdfs=args.save_pdfs,
                        semaphore=semaphore,
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
    metrics["vendor_counts"] = vendor_counts
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
