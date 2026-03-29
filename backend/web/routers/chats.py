import logging

from fastapi import APIRouter, Body, Depends, HTTPException

from backend.web.common.dependencies import require_chat_belongs_to_db
from backend.ai.store import (
    create_chat,
    delete_chat,
    list_chats,
    set_chat_starred,
    update_chat_title,
)

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/databases", tags=["chats"])


@router.get("/{name}/chats")
def api_list_chats(name: str):
    """List chats for the given database."""
    try:
        chats = list_chats(name)
        return {"chats": chats}
    except Exception as e:
        log.error("Failed to list chats for %s: %s", name, e)
        return {"chats": [], "error": str(e)}


@router.post("/{name}/chats")
def api_create_chat(name: str, body: dict = Body(default=None)):
    """Create a new chat for the database. Body optional: {"title": "..."}. Returns {id, title, created_at, starred}."""
    try:
        title = (body or {}).get("title", "Новый чат") or "Новый чат"
        chat = create_chat(name, title)
        return chat
    except Exception as e:
        log.error("Failed to create chat for %s: %s", name, e)
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/{name}/chats/{chat_id}/title")
def api_set_chat_title(
    name: str,
    chat_id: int,
    body: dict = Body(default=None),
    _: str = Depends(require_chat_belongs_to_db),
):
    """Set chat title. Body: {"title": "..."}. Chat must belong to this database."""
    try:
        title = (body or {}).get("title", "Новый чат") or "Новый чат"
        update_chat_title(chat_id, title)
        return {"ok": True, "title": title}
    except Exception as e:
        if hasattr(e, "status_code"):
            raise
        log.error("Failed to set title for chat %s: %s", chat_id, e)
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/{name}/chats/{chat_id}/star")
def api_set_chat_starred(
    name: str,
    chat_id: int,
    body: dict = Body(default=None),
    _: str = Depends(require_chat_belongs_to_db),
):
    """Set starred flag. Body: {"starred": true|false}. Chat must belong to this database."""
    try:
        starred = (body or {}).get("starred", False)
        set_chat_starred(chat_id, bool(starred))
        return {"ok": True, "starred": bool(starred)}
    except Exception as e:
        if hasattr(e, "status_code"):
            raise
        log.error("Failed to set starred for chat %s: %s", chat_id, e)
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{name}/chats/{chat_id}")
def api_delete_chat(
    name: str,
    chat_id: int,
    _: str = Depends(require_chat_belongs_to_db),
):
    """Delete a chat. Chat must belong to this database."""
    try:
        delete_chat(chat_id)
        return {"ok": True}
    except HTTPException:
        raise
    except Exception as e:
        log.error("Failed to delete chat %s: %s", chat_id, e)
        raise HTTPException(status_code=500, detail=str(e))
