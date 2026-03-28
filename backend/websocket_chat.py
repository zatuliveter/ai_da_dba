import json
import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from backend.agent_loop import EMPTY_TOKEN_STATS, run_agent_loop
from backend.ai.prompts import DEFAULT_ROLE
from backend.routers.chat_files import FILES_DIR, sanitize_filename
from backend.ai.store import (
    ChatMessage,
    append_chat_messages,
    create_chat,
    get_chat_messages,
    get_chat_token_stats,
)

log = logging.getLogger("agent")

router = APIRouter()


@router.websocket("/ws")
async def ws_chat(ws: WebSocket):
    await ws.accept()
    database: str | None = None
    chat_id: int | None = None
    agent_role: str = DEFAULT_ROLE

    try:
        while True:
            raw = await ws.receive_text()
            payload = json.loads(raw)

            if payload.get("type") == "set_chat":
                cid = payload.get("chat_id")
                if cid is None:
                    await ws.send_text(json.dumps({"type": "error", "content": "chat_id required"}))
                    continue
                chat_id = int(cid)
                history = get_chat_messages(chat_id)
                await ws.send_text(json.dumps({
                    "type": "history_loaded",
                    "messages": [
                        {
                            "role": m.role,
                            "content": m.content,
                            **({"tool_result": m.tool_result} if m.role == "tool_call" and m.tool_result else {}),
                        }
                        for m in history
                    ],
                    "token_stats": get_chat_token_stats(chat_id),
                }))
                continue

            if payload.get("type") == "set_database":
                db_name = payload.get("database")
                if not db_name:
                    await ws.send_text(json.dumps({"type": "error", "content": "database required"}))
                    continue
                database = str(db_name)
                chat_id = None
                await ws.send_text(json.dumps({
                    "type": "history_loaded",
                    "messages": [],
                    "token_stats": EMPTY_TOKEN_STATS,
                }))
                continue

            if payload.get("type") == "create_chat":
                if not database:
                    await ws.send_text(json.dumps({"type": "error", "content": "Select a database first."}))
                    continue
                title = payload.get("title", "Новый чат") or "Новый чат"
                chat = create_chat(database, title)
                chat_id = chat["id"]
                await ws.send_text(json.dumps({
                    "type": "chat_created",
                    "chat": chat,
                }))
                await ws.send_text(json.dumps({
                    "type": "history_loaded",
                    "messages": [],
                    "token_stats": get_chat_token_stats(chat_id),
                }))
                continue

            if payload.get("type") == "message":
                if not database:
                    await ws.send_text(json.dumps({
                        "type": "error",
                        "content": "Please select a database first.",
                    }))
                    continue
                if chat_id is None:
                    await ws.send_text(json.dumps({
                        "type": "error",
                        "content": "Please select or create a chat first.",
                    }))
                    continue

                user_text = (payload.get("content") or "").strip()
                attachments = payload.get("attachments") or []
                if isinstance(attachments, list):
                    attachments = [str(a) for a in attachments]
                else:
                    attachments = []

                combined_parts = []
                chat_dir = FILES_DIR / str(chat_id)
                log.info("Message attachments: %s (chat_id=%s, dir=%s)", attachments, chat_id, chat_dir)
                for att_name in attachments:
                    safe = sanitize_filename(att_name)
                    if not safe:
                        continue
                    path = chat_dir / safe
                    if not path.is_file():
                        log.warning("Attachment not found: %s (chat_id=%s) path=%s", safe, chat_id, path)
                        continue
                    try:
                        content = path.read_text(encoding="utf-8")
                        combined_parts.append(f"Attached file: {safe}\n\n{content}\n\n---\n\n")
                        log.info("Read attachment %s: %d chars", safe, len(content))
                    except Exception as e:
                        log.warning("Failed to read attachment %s: %s", safe, e)

                if combined_parts:
                    full_content = "".join(combined_parts) + (user_text or "")
                else:
                    full_content = user_text or ""

                user_msg = ChatMessage(role="user", content=full_content)
                append_chat_messages(chat_id, [user_msg])

                await run_agent_loop(ws, database, agent_role, chat_id)
                continue

    except WebSocketDisconnect:
        log.info("Client disconnected")
    except Exception as e:
        log.exception("WebSocket error")
        try:
            await ws.send_text(json.dumps({"type": "error", "content": str(e)}))
        except Exception:
            pass
