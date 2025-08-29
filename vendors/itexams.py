import asyncio
import logging
import re
import json
from typing import List, Dict
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from utils import fetch_with_retry, smart_hash, Question
from normalize_vendor import normalize_vendor


async def discover_itexams_tasks(client: httpx.AsyncClient, config: Dict) -> List[Dict]:
    """
    Discovers all exam tasks by navigating to each vendor's page and scraping
    metadata from each exam's '/info/' page.
    """
    logger = logging.getLogger("main")
    logger.info("🔍 Discovering tasks from itexams.com via high-fidelity metadata crawl...")
    tasks = []
    master_url = "https://www.itexams.com/all-exams/"

    # Step 1: Fetch the vendor index page
    res = await fetch_with_retry(client, master_url, logger, config)
    if not res:
        logger.error("❌ Failed to fetch the master exam index page.")
        return []

    soup = BeautifulSoup(res.text, 'html.parser')

    # Step 2: Parse vendor links
    vendor_tags = soup.select("a[href^='/vendor/']")
    print('-------- vendor tags ', vendor_tags)
    vendor_links = []
    vendor_names = []

    for a in vendor_tags:
        href = a.get('href')
        name = a.get_text(strip=True)
        print('================= href ', href , "=============== name ", name)
        if href and name:
            vendor_links.append(href)
            vendor_names.append(name)

    logger.info(f"🗂️ Found {len(vendor_links)} vendors.")
    for name, href in zip(vendor_names, vendor_links):
        logger.debug(f"🔗 Vendor: {name} → {href}")

    # (Optional) Save vendor list for later inspection
    try:
        with open("discovered_vendors.json", "w") as f:
            json.dump(
                [{"name": n, "url": urljoin(master_url, h)} for n, h in zip(vendor_names, vendor_links)],
                f, indent=2
            )
    except Exception as e:
        logger.warning(f"⚠️ Failed to save vendor list: {e}")

    # Step 3: Fetch each vendor page to extract /info/ links
    vendor_page_responses = await asyncio.gather(
        *[fetch_with_retry(client, urljoin(master_url, v_link), logger, config) for v_link in vendor_links]
    )

    info_links_to_fetch = []
    for vendor_res in vendor_page_responses:
        if vendor_res:
            vendor_soup = BeautifulSoup(vendor_res.text, 'html.parser')
            info_links_to_fetch.extend([
                urljoin(master_url, a['href'])
                for a in vendor_soup.select("a[href*='/info/']")
            ])

    logger.info(f"🔍 Found {len(info_links_to_fetch)} '/info/' pages across all vendors.")

    # Step 4: Fetch all /info/ pages in parallel
    info_page_responses = await asyncio.gather(
        *[fetch_with_retry(client, info_link, logger, config) for info_link in info_links_to_fetch]
    )

    # Step 5: Parse metadata from each info page
    for info_res in info_page_responses:
        if not info_res:
            continue

        info_soup = BeautifulSoup(info_res.text, 'html.parser')
        metadata_items = info_soup.select("div.qaExam__info-item")

        exam_data = {}
        for item in metadata_items:
            key_tag = item.select_one("span:nth-of-type(1)")
            value_tag = item.select_one("span:nth-of-type(2)")
            if key_tag and value_tag:
                key = key_tag.get_text(strip=True).replace(":", "").lower().replace(" ", "_")
                value = value_tag.get_text(strip=True)
                exam_data[key] = value

        vendor = normalize_vendor(exam_data.get("vendor", ""), exam_data.get("exam_code", ""))
        logger.info(f"======= vendor is after normalize {vendor}")
        exam_code = exam_data.get("exam_code", "")
        logger.info(f"====== exam code  {exam_code}")
        exam_name = exam_data.get("exam_name", "")
        logger.info(f"===== exam name {exam_name}")
        vendor_name = exam_data.get('vendor', '')
        logger.info(f"===== vendor name {vendor_name}")
        if not vendor or vendor == "Unknown" or not exam_code or not exam_name:
            logger.warning(f"⚠️ Incomplete metadata found on {info_res.url}. Skipping.")
            continue

        tasks.append({
            "site": "itexams",
            "info_url": str(info_res.url),
            "exam_code": exam_code,
            "vendor": vendor_name,
            "exam_name": exam_name,
            "version": "latest",
            "source_site": "itexams.com"
        })

    logger.info(f"✅ Discovered {len(tasks)} potential tasks from the itexams.com master index.")
    return tasks


async def worker_itexams(
    client: httpx.AsyncClient,
    task: Dict,
    logger,
    process_pool, # Not used by this worker
    config: Dict,
    save_pdfs: bool = False,
    semaphore: asyncio.Semaphore = None
) -> List[Dict]:
    """Async worker that starts from an '/info/' page to find and process an exam."""
    async with semaphore:
        try:
            logger.info(f"🧠 Processing itexams task: {task['exam_code']}")
            
            res_info = await fetch_with_retry(client, task['info_url'], logger, config)
            if not res_info:
                return []

            soup_info = BeautifulSoup(res_info.text, 'html.parser')
            exam_link_tag = soup_info.select_one("a.qaExam__item-link[href*='/exam/']")
            if not exam_link_tag:
                logger.warning(f"No exam page link found on info page: {task['info_url']}")
                return []

            current_url = urljoin(task['info_url'], exam_link_tag['href'])
            all_questions = []
            page_count = 0

            while current_url and page_count < 50: # Safety break
                page_count += 1
                
                res_exam = await fetch_with_retry(client, current_url, logger, config)
                if not res_exam:
                    break

                soup_exam = BeautifulSoup(res_exam.text, 'html.parser')
                selector = config.get("selectors", {}).get("itexams", {})
                
                q_containers = soup_exam.select(selector.get("question_container"))
                if not q_containers:
                    break

                for q_container in q_containers:
                    body_tag = q_container.select_one(selector.get("question_body"))
                    if not body_tag:
                        continue

                    question_text = body_tag.get_text(strip=True)
                    answers, correct_options = [], set()

                    correct_elem = q_container.select_one(selector.get("correct_answer"))
                    if correct_elem:
                        correct_options = {opt.strip() for opt in correct_elem.text.split(',')}

                    ans_elems = q_container.select(selector.get("answer_option"))
                    for ans_elem in ans_elems:
                        ans_text = ans_elem.text.strip()
                        opt_match = re.match(r'([A-Z])\.', ans_text)
                        if opt_match:
                            opt_label = opt_match.group(1)
                            opt_text = ans_text[len(opt_label) + 1:].strip()
                            answers.append({
                                "option": opt_label,
                                "text": opt_text,
                                "is_correct": (opt_label in correct_options)
                            })

                    if not answers:
                        continue
                    
                    qid = smart_hash(question_text, answers, version=task["version"])
                    logger.info(f"---------- task in worker_itexams {task}")
                    all_questions.append(Question(
                        question_id=qid, question_text=question_text, answers=answers,
                        vendor=task["vendor"], exam_code=task["exam_code"], exam_name=task["exam_name"],
                        tags=[task["vendor"], task["exam_code"]], source_url=current_url,
                        source_type="html", version=task["version"]
                    ).dict())

                next_page_tag = soup_exam.select_one(selector.get("next_page_link"))
                current_url = urljoin(current_url, next_page_tag['href']) if next_page_tag else None
                await asyncio.sleep(1) # Rate limiting

            return all_questions
        except Exception as e:
            logger.error(f"❌ Error while processing {task['exam_code']}: {e}")
            return []