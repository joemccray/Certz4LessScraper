# config_schema.py
from pydantic import BaseModel, Field
from typing import Dict


class Selectors(BaseModel):
    question_container: str
    question_body: str
    correct_answer: str
    answer_option: str
    next_page_link: str


class SiteSelectors(BaseModel):
    itexams: Selectors
    allfreedumps: Dict[str, str]


class ConfigSchema(BaseModel):
    max_concurrent_requests: int = 10
    max_cpu_workers: int = 2
    request_timeout: int = 60
    max_retries: int = 3
    task_timeout_seconds: int = 300
    min_ocr_confidence: int = 65
    min_text_length_for_native_pdf: int = 500
    selectors: SiteSelectors
    slack_webhook_url: str = ""
    discord_webhook_url: str = ""
