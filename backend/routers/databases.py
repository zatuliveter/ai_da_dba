import logging

from fastapi import APIRouter, Body

from backend.db import list_databases
from backend.ai.store import get_db_description, set_db_description

log = logging.getLogger("agent")

router = APIRouter(prefix="/api/databases", tags=["databases"])


@router.get("")
def api_databases():
    try:
        names = list_databases()
        databases = [
            {"name": n, "description": get_db_description(n) or ""}
            for n in names
        ]
        return {"databases": databases}
    except Exception as e:
        log.error("Failed to list databases: %s", e)
        return {"databases": [], "error": str(e)}


@router.put("/{name}/description")
def api_set_database_description(name: str, body: dict = Body(default={})):
    """Body: {"description": "..."}. Saves to SQLite store."""
    try:
        description = (body or {}).get("description", "") or ""
        set_db_description(name, description)
        return {"ok": True}
    except Exception as e:
        log.error("Failed to set description for %s: %s", name, e)
        return {"ok": False, "error": str(e)}
