#!/usr/bin/env python3
"""
exam_scraper_v9.1.py

Modular, Enterprise-Grade Hybrid Exam Scraper (v9.1)

This version refactors the architecture into a modular, plug-and-play system
for vendor-specific workers. It incorporates advanced features for data quality,
resilience, and operational control.

Key Features:
- Modular Worker Architecture: Vendor-specific logic is now in `vendors/`
  making the system easily extensible.
- Completed HTML Worker: Full asynchronous implementation for itexams.com.
- Advanced OCR Confidence Filtering: Improves data quality by discarding
  low-confidence OCR results.
- Resilient DB Writes: Uses tenacity to retry failed Supabase upserts.
- External Configuration: Manages all operational parameters via `config.yaml`.
- Local PDF Archival: Optional `--save-pdfs` flag to archive downloaded files.
- Detailed Metrics Export: Generates `run_metrics.json` with key stats.
- CLI Vendor Targeting: `--vendor` flag to run scrapes for a specific vendor.
"""

import os
import sys
import json
import time
import hashlib
import signal
import logging
import re
import argparse
import asyncio
import yaml
from logging.handlers import RotatingFileHandler
from concurrent.futures import ProcessPoolExecutor, TimeoutError
from typing import List, Dict, Set, Any, Optional
from urllib.parse import urljoin
from collections import deque
import shutil

import httpx
from dotenv import load_dotenv
from supabase import create_client, Client
from postgrest.exceptions import APIError
from pydantic import ValidationError
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError

# --- Modular Imports ---
from vendors import WORKER_MAP, DISCOVERY_MAP
from utils import (
    setup_logging, smart_hash, send_notification, Question, Answer,
    parse_pdf_task, get_task_lists, save_state
)
from normalize_vendor import normalize_vendor

# --- Global State ---
CONFIG = {}
supabase: Optional[Client] = None
terminate = False
checkpoint = {"processed_tasks": {}}
processed_question_ids: Set[str] = set()
METADATA_MAPPING: Dict[str, Any] = {}
EXAM_ALIASES: Dict[str, str] = {}
db_retry_queue = []
metadata_changed = False
main_logger = logging.getLogger("main")
# Update load_config in exam_scraper_v9.py
from config_schema import ConfigSchema


def load_config():
    global CONFIG
    try:
        with open("config.yaml", "r") as f:
            raw_config = yaml.safe_load(f)
            CONFIG = ConfigSchema(**raw_config).model_dump()
    except Exception as e:
        main_logger.critical(f"❌ Failed to load and validate config.yaml: {e}")
        sys.exit(1)

def handle_signal(sig, frame):
    """Gracefully handle Ctrl+C interrupts."""
    global terminate
    if not terminate:
        main_logger.warning("SIGINT caught! Graceful shutdown initiated...")
        terminate = True

@retry(stop=stop_after_attempt(CONFIG.get('max_retries', 3)), wait=wait_exponential(multiplier=2, min=10, max=50), retry_error_callback=lambda retry_state: None)
async def validate_and_upsert_batch(records: List[Dict]):
    """Validates and upserts a batch of records to Supabase with retries."""
    if not records or supabase is None:
        return 0

    valid_records = []
    validation_errors = []
    for rec in records:
        try:
            Question(**rec)
            valid_records.append(rec)
        except ValidationError as e:
            validation_errors.append({"record_id": rec.get('question_id'), "error": e.errors()})

    if validation_errors:
        main_logger.warning(f"{len(validation_errors)} records failed validation.")
        # Optionally save validation_errors to a file here

    if not valid_records:
        return 0

    try:
        response = await asyncio.to_thread(
            supabase.table("questions").upsert,
            valid_records,
            on_conflict='question_id'
        )
        main_logger.info(f"======= the response is {response.data}")
        if response.data:
            main_logger.info(f"✅ Supabase upsert confirmed with {len(response.data)} entries.")
        else:
            main_logger.warning("⚠️ Supabase upsert returned no data, check schema or payload.")
        return len(valid_records)
    except APIError as e:
        main_logger.error(f"❌ Supabase API error during upsert: {e.message}. Retrying...")
        raise
    except Exception as e:
        main_logger.error(f"❌ An unexpected error occurred during upsert: {e}")
        db_retry_queue.extend(valid_records)
        return 0

async def run_preflight_checks() -> bool:
    """Performs all startup checks for dependencies, config, and database."""
    global supabase, METADATA_MAPPING, EXAM_ALIASES
    main_logger.info("🚀 Running pre-flight checks...")

    if not all([os.getenv("SCRAPINGBEE_API_KEY"), os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY")]):
        main_logger.critical("❌ Missing critical environment variables.")
        return False

    supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

    try:
        await asyncio.to_thread(lambda: supabase
            .table("questions")
            .select("question_id", count='exact')
            .limit(1)
            .execute()
        )
        main_logger.info("✅ Supabase connection successful and 'questions' table found.")
    except APIError as e:
        if "relation \"public.questions\" does not exist" in e.message:
            main_logger.warning("⚠️ Table 'questions' not found. Attempting to create it via RPC...")
            try:
                await asyncio.to_thread(supabase.rpc, 'create_questions_table_if_not_exists')
                main_logger.info("✅ Table 'questions' created successfully via RPC.")
            except Exception as ex:
                main_logger.critical(f"❌ Failed to create table via RPC: {ex}")
                return False
        else:
            main_logger.critical(f"❌ Supabase API error: {e.message}"); return False

    if shutil.which("tesseract") is None:
        main_logger.warning("⚠️ Tesseract OCR engine not found in PATH. OCR fallback will fail.")

    main_logger.info("✅ Pre-flight checks passed!")
    return True

async def main():
    """Main async execution function."""
    parser = argparse.ArgumentParser(description="Exam Scraper v9.1 - The Definitive Edition")
    parser.add_argument("--sites", nargs='+', choices=['itexams', 'allfreedumps'], default=['itexams', 'allfreedumps'], help="Run crawlers for specified sites.")
    parser.add_argument("--vendor", type=str, help="Target a specific vendor (e.g., 'Amazon').")
    parser.add_argument("--save-pdfs", action="store_true", help="Archive all downloaded PDFs to data/pdf_archive.")
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

    logging.getLogger(f"===== all tasks {all_tasks}")
    # Filtering logic
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
            semaphore = asyncio.Semaphore(CONFIG['max_concurrent_requests'])
            worker_tasks = []
            for task in all_tasks:
                worker_func = WORKER_MAP.get(task['site'])
                if worker_func:
                    logging.getLogger(f"============ tasks vendor is {task['vendor']}")
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
    save_state(checkpoint, metrics) # Save final state
    main_logger.info(f"Scrape finished. Report: {json.dumps(metrics, indent=2)}")

if __name__ == "__main__":
    try:
        signal.signal(signal.SIGINT, handle_signal)
        asyncio.run(main())
    except Exception as e:
        main_logger.exception(f"❌ Fatal error in scraper: {e}")
        send_notification(f"❌ Fatal error in scraper: {e}", webhook_url=CONFIG.get("slack_webhook_url", ""))