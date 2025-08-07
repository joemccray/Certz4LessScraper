# vendors/allfreedumps.py

import asyncio
import hashlib
import logging
import os
from typing import List, Dict, Optional
from urllib.parse import urljoin
import re
import httpx
from bs4 import BeautifulSoup

from utils import fetch_with_retry, parse_pdf_task
from normalize_vendor import normalize_vendor

async def discover_allfreedumps_tasks(client: httpx.AsyncClient, config: Dict) -> List[Dict]:
    """Crawls allfreedumps.com to find PDF exam tasks."""
    logger = logging.getLogger("main")
    logger.info("🔍 Discovering tasks from allfreedumps.com...")
    tasks = []
    base_url = "https://www.allfreedumps.com/"
    res = await fetch_with_retry(client, urljoin(base_url, "certs.html"), logger, config)
    if not res:
        return []

    soup = BeautifulSoup(res.text, 'html.parser')
    selector = config.get("selectors", {}).get("allfreedumps", {}).get("exam_link", "a[href$='-dumps.html']")
    
    for a_tag in soup.select(selector):
        href = a_tag['href']
        match = re.search(r'/([A-Za-z0-9\-]+)-dumps\.html$', href)
        if match:
            exam_code = match.group(1).upper()
            vendor = normalize_vendor(a_tag.text, exam_code)
            exam_name = a_tag.text.strip()

            tasks.append({
                "site": "allfreedumps",
                "pdf_page_url": urljoin(base_url, href),
                "exam_code": exam_code,
                "vendor": vendor,
                "exam_name": exam_name,
                "version": "latest" # Default version
            })
    logger.info(f"Discovered {len(tasks)} potential tasks from allfreedumps.com.")
    return tasks

async def worker_allfreedumps(
    client: httpx.AsyncClient,
    task: Dict,
    logger,
    process_pool,
    config: Dict,
    save_pdfs: bool = False,
    semaphore: asyncio.Semaphore = None
) -> List[Dict]:
    """Async worker to process a single exam from allfreedumps.com."""
    async with semaphore:
        logger.info(f"🧠 Processing PDF task: {task['exam_code']}")
        page_res = await fetch_with_retry(client, task['pdf_page_url'], logger, config)
        if not page_res:
            return []

        soup = BeautifulSoup(page_res.text, 'html.parser')
        selector = config.get("selectors", {}).get("allfreedumps", {}).get("pdf_download_link", "div.downbtn a")
        pdf_link_tag = soup.select_one(selector)
        if not pdf_link_tag:
            logger.warning(f"No PDF download link found for {task['exam_code']}")
            return []

        pdf_url = urljoin(task['pdf_page_url'], pdf_link_tag['href'])
        pdf_res = await fetch_with_retry(client, pdf_url, logger, config, is_pdf=True)
        if not pdf_res:
            return []

        pdf_content = pdf_res.content

        if save_pdfs:
            archive_dir = os.path.join("data", "pdf_archive", task['exam_code'])
            os.makedirs(archive_dir, exist_ok=True)
            content_hash = hashlib.sha256(pdf_content).hexdigest()
            archive_path = os.path.join(archive_dir, f"{content_hash}.pdf")
            if not os.path.exists(archive_path):
                with open(archive_path, "wb") as f:
                    f.write(pdf_content)
                logger.info(f"Archived PDF to {archive_path}")

        metadata = {**task, "source_url": pdf_url}
        
        loop = asyncio.get_running_loop()
        questions = await loop.run_in_executor(
            process_pool, parse_pdf_task, pdf_content, metadata, config
        )
        return questions
