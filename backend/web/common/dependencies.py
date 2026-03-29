from fastapi import HTTPException, Query

from backend.ai.store import get_chat_scope


def require_chat_belongs_to_db(
    name: str,
    chat_id: int,
    connection_id: int = Query(..., description="MSSQL connection id"),
) -> str:
    """Ensure chat exists and belongs to the given connection and database name."""
    scope = get_chat_scope(chat_id)
    if scope is None:
        raise HTTPException(status_code=404, detail="Chat not found")
    cid, db_name = scope
    if cid != connection_id or db_name != name:
        raise HTTPException(status_code=404, detail="Chat not found")
    return name
