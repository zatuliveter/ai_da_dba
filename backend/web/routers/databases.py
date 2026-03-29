import logging

from fastapi import APIRouter, Body, Query

from backend.ai.store import get_connection_string, get_db_description, set_db_description
from backend.mssql_db import list_databases

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/databases", tags=["databases"])


@router.get("")
def api_databases(connection_id: int = Query(..., description="MSSQL connection id")):
    cs = get_connection_string(connection_id)
    if cs is None:
        return {"databases": [], "error": "Unknown connection_id"}
    try:
        names = list_databases(cs)
        databases = [
            {"name": n, "description": get_db_description(connection_id, n) or ""}
            for n in names
        ]
        return {"databases": databases}
    except Exception as e:
        log.error("Failed to list databases: %s", e)
        return {"databases": [], "error": str(e)}


@router.put("/{name}/description")
def api_set_database_description(
    name: str,
    connection_id: int = Query(..., description="MSSQL connection id"),
    body: dict = Body(default={}),
):
    """Body: {"description": "..."}. Saves to SQLite store."""
    try:
        description = (body or {}).get("description", "") or ""
        set_db_description(connection_id, name, description)
        return {"ok": True}
    except Exception as e:
        log.error("Failed to set description for %s: %s", name, e)
        return {"ok": False, "error": str(e)}
