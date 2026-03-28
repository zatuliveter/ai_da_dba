from fastapi import HTTPException

from backend.ai.store import get_chat_database_name


def require_chat_belongs_to_db(name: str, chat_id: int) -> str:
    """Ensure chat exists and belongs to the given database. Return database_name or raise HTTPException(404)."""
    db_name = get_chat_database_name(chat_id)
    if db_name is None or db_name != name:
        raise HTTPException(status_code=404, detail="Chat not found")
    return db_name
