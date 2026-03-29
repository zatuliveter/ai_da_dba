import logging

from fastapi import APIRouter, Body, HTTPException

from backend.ai.store import add_mssql_connection, delete_mssql_connection, list_mssql_connections

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/connections", tags=["connections"])


@router.get("")
def api_list_connections():
    return {"connections": list_mssql_connections()}


@router.post("")
def api_add_connection(body: dict = Body(...)):
    label = (body.get("label") or "").strip() or "Connection"
    connection_string = (body.get("connection_string") or "").strip()
    if not connection_string:
        raise HTTPException(status_code=400, detail="connection_string is required")
    try:
        cid = add_mssql_connection(label, connection_string)
        return {"id": cid, "label": label}
    except Exception as e:
        log.error("Failed to add connection: %s", e)
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.delete("/{connection_id}")
def api_delete_connection(connection_id: int):
    if not delete_mssql_connection(connection_id):
        raise HTTPException(status_code=404, detail="Connection not found")
    return {"ok": True}
