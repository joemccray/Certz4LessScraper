# utils.py

import os
import sys
import json
import logging
import hashlib
import time
from typing import List, Dict, Optional, Any
from logging.handlers import RotatingFileHandler
import re
import httpx
from pydantic import BaseModel, ValidationError
from tenacity import retry, stop_after_attempt, wait_exponential
import fitz  # PyMuPDF

try:
    from PIL import Image
    import pytesseract

    OCR_ENABLED = True
except ImportError:
    OCR_ENABLED = False


# --- Pydantic Models ---
class Answer(BaseModel):
    option: str
    text: str
    is_correct: bool = False


class Question(BaseModel):
    question_id: str
    vendor: str
    exam_code: str
    exam_name: str
    question_text: str
    answers: List[Answer]
    explanation: str = ""
    tags: List[str]
    source_url: str
    source_type: str = "native"
    version: str = "latest"


# --- Core Utility Functions ---


def setup_logging(logs_dir: str):
    """Configures logging to console and separate files."""
    base_formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] [%(name)s] - %(message)s"
    )

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Clear existing handlers to avoid duplication
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(base_formatter)
    root_logger.addHandler(console_handler)

    log_files = {
        "main": os.path.join(logs_dir, "scraper_main.log"),
        "pdf_worker": os.path.join(logs_dir, "pdf.log"),
        "html_worker": os.path.join(logs_dir, "html.log"),
    }

    for name, path in log_files.items():
        file_handler = RotatingFileHandler(
            path, maxBytes=5 * 1024 * 1024, backupCount=3
        )
        file_handler.setFormatter(base_formatter)
        logger = logging.getLogger(name)
        logger.addHandler(file_handler)
        logger.propagate = True


def smart_hash(
    text: str, answers: List[Dict], explanation: str = "", version: str = "latest"
) -> str:
    """Generate a robust hash for a question to prevent duplicates."""
    sorted_answers = sorted([a["text"].strip() for a in answers])
    canonical_string = (
        f"{version}|{text.strip()}|{'|'.join(sorted_answers)}|{explanation.strip()}"
    )
    return hashlib.md5(canonical_string.encode()).hexdigest()


def send_notification(message: str, config: Dict, level: str = "INFO"):
    """Sends a notification to Slack or Discord."""
    logger = logging.getLogger("main")
    webhook_url = config.get("slack_webhook_url") or config.get("discord_webhook_url")
    if webhook_url:
        try:
            payload_key = "content" if "discord" in webhook_url else "text"
            payload = {payload_key: f"📢 Exam Scraper V9.1 ({level}): {message}"}
            httpx.post(webhook_url, json=payload, timeout=10)
        except httpx.RequestError as e:
            logger.error(f"Failed to send webhook notification: {e}")
    else:
        logger.info(f"NOTIFICATION: {message}")


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=2, min=10, max=50),
    retry_error_callback=lambda retry_state: None,
)
async def fetch_with_retry(
    client: httpx.AsyncClient, url: str, logger, config: Dict, is_pdf: bool = False
) -> Optional[httpx.Response]:
    """Asynchronously fetches a URL with retries using httpx."""
    try:
        logger.info(f"Fetching URL ====== : {url}")
        user_agents = config.get("user_agents", ["Mozilla/5.0"])
        headers = {"User-Agent": user_agents[0]}  # Basic rotation, can be improved

        params = {
            "api_key": os.getenv("SCRAPINGBEE_API_KEY"),
            "url": url,
            "render_js": str(not is_pdf).lower(),
            "forward_headers": "true",
        }
        res = await client.get(
            "https://app.scrapingbee.com/api/v1/",
            params=params,
            headers=headers,
            timeout=config["request_timeout"],
        )
        res.raise_for_status()
        return res
    except httpx.RequestError as e:
        logger.warning(f"Fetch failed for {url}: {e}. Retrying...")
        raise


def parse_pdf_task(pdf_content: bytes, metadata: Dict, config: Dict) -> List[Dict]:
    """Standalone CPU-bound function to parse a PDF, with OCR fallback."""
    logger = logging.getLogger("pdf_worker")
    text = ""
    source_type = "native"

    try:
        with fitz.open(stream=pdf_content, filetype="pdf") as doc:
            native_text = "".join(page.get_text() for page in doc).strip()

            if (
                len(native_text) < config["min_text_length_for_native_pdf"]
                or "NEW QUESTION" not in native_text.upper()
            ):
                if not OCR_ENABLED:
                    return []

                source_type = "ocr"
                ocr_texts = []
                for page in doc:
                    pix = page.get_pixmap(dpi=300)
                    img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                    ocr_data = pytesseract.image_to_data(
                        img, output_type=pytesseract.Output.DICT
                    )

                    confidences = [int(c) for c in ocr_data["conf"] if int(c) > -1]
                    avg_conf = sum(confidences) / len(confidences) if confidences else 0

                    if avg_conf >= config["min_ocr_confidence"]:
                        ocr_texts.append(pytesseract.image_to_string(img))
                text = "\n".join(ocr_texts)
            else:
                text = native_text
    except Exception as e:
        logger.error(
            f"Failed to read PDF content for {metadata.get('source_url')}: {e}"
        )
        return []

    if not text:
        return []

    questions = []
    question_blocks = re.findall(
        r"(NEW QUESTION \d+[\s\S]+?)(?=NEW QUESTION \d+|$)", text, re.IGNORECASE
    )

    for block in question_blocks:
        q_match = re.search(r"NEW QUESTION \d+\s*(.*?)(?=A\.\s)", block, re.DOTALL)
        question_text = q_match.group(1).strip() if q_match else ""
        if not question_text:
            continue

        choices = re.findall(r"([A-E])\.\s([^\n]+)", block)
        ans_match = re.search(r"Answer:\s*([A-Z](?:,\s*[A-Z])*)", block, re.IGNORECASE)
        correct_options = (
            {opt.strip().upper() for opt in ans_match.group(1).split(",")}
            if ans_match
            else set()
        )

        expl_match = re.search(r"Explanation:\s*([\s\S]+)", block, re.IGNORECASE)
        explanation = expl_match.group(1).strip() if expl_match else ""

        answers = [
            {"option": opt, "text": txt.strip(), "is_correct": (opt in correct_options)}
            for opt, txt in choices
        ]
        if not answers:
            continue

        qid = smart_hash(
            question_text, answers, explanation, metadata.get("version", "latest")
        )

        questions.append(
            Question(
                question_id=qid,
                question_text=question_text,
                answers=answers,
                explanation=explanation,
                vendor=metadata.get("vendor"),
                exam_code=metadata.get("exam_code"),
                exam_name=metadata.get("exam_name"),
                tags=[metadata.get("vendor", "Unknown"), metadata.get("exam_code")],
                source_url=metadata.get("source_url"),
                source_type=source_type,
            ).dict()
        )
    return questions


def get_task_lists():
    """Reads whitelist/blacklist files."""
    try:
        with open("blacklist.txt", "r") as f:
            blacklist = {line.strip().upper() for line in f if line.strip()}
    except FileNotFoundError:
        blacklist = set()
    try:
        with open("whitelist.txt", "r") as f:
            whitelist = {line.strip().upper() for line in f if line.strip()}
    except FileNotFoundError:
        whitelist = set()
    return whitelist, blacklist


def save_state(checkpoint: Dict, metrics: Dict, metadata: Dict, metadata_changed: bool):
    """Saves the checkpoint file, metrics, and any updated metadata."""
    logger = logging.getLogger("main")
    try:
        with open("checkpoint.json", "w") as f:
            json.dump(checkpoint, f, indent=4)

        with open("run_metrics.json", "w") as f:
            json.dump(metrics, f, indent=4)

        if metadata_changed:
            with open("metadata_mapping.json", "w") as f:
                json.dump(metadata, f, indent=4, sort_keys=True)
            logger.info("Saved updated metadata mapping.")
    except IOError as e:
        logger.error(f"Failed to save state files: {e}")
