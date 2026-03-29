import logging
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY", "")
API_URL = (os.getenv("API_URL") or "").strip()
SQL_SERVER = os.getenv("SQL_SERVER", "localhost")
LLM_MODEL = os.getenv("LLM_MODEL")

ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True) # Ensure data directory exists


_log = logging.getLogger(__name__)


def validate_config():
    """Check required env vars for LLM and DB. Log errors and return True if LLM is usable."""
    ok = True
    if not API_KEY or not API_URL:
        _log.error("API_KEY and API_URL must be set for LLM; current API_URL is set: %s", bool(API_URL))
        ok = False
    if not SQL_SERVER:
        _log.error("SQL_SERVER must be set for database connectivity")
        ok = False
    if not ok:
        raise ValueError("Invalid configuration; see logs for details")
    _log.info("Configuration validated successfully")
