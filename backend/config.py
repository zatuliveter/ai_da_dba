import logging
import os
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

API_KEY = os.getenv("API_KEY", "")
API_URL = (os.getenv("API_URL") or "").strip()
SQL_SERVER = os.getenv("SQL_SERVER", "localhost")
LLM_MODEL = os.getenv("LLM_MODEL")

if API_KEY and API_URL:
    llm_client = OpenAI(api_key=API_KEY, base_url=API_URL)
else:
    llm_client = None

_log = logging.getLogger("config")


def validate_config() -> bool:
    """Check required env vars for LLM and DB. Log errors and return True if LLM is usable."""
    ok = True
    if not API_KEY or not API_URL:
        _log.error("API_KEY and API_URL must be set for LLM; current API_URL is set: %s", bool(API_URL))
        ok = False
    if not SQL_SERVER:
        _log.error("SQL_SERVER must be set for database connectivity")
        ok = False
    return ok
