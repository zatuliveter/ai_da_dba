import json
import logging
import os
import re
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Body, UploadFile, File, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from config import llm_client, LLM_MODEL, validate_config
from db import list_databases
from prompts import DEFAULT_ROLE, get_system_prompt
from store import (
    ChatMessage,
    get_db_description,
    set_db_description,
    list_chats,
    create_chat,
    get_chat_messages,
    append_chat_messages,
    set_chat_starred,
    update_chat_title,
    delete_chat,
    get_chat_database_name,
    init_db,
)
from tools import TOOL_DEFINITIONS, dispatch_tool

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("agent")


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    validate_config()
    yield


app = FastAPI(title="AI da DBA", lifespan=lifespan)

# ---------------------------------------------------------------------------
# File attachments: storage root and sanitization
# ---------------------------------------------------------------------------

FILES_DIR = Path(__file__).resolve().parent / "data" / "files"
MAX_FILE_SIZE_BYTES = 2 * 1024 * 1024  # 2 MB
MAX_FILES_PER_MESSAGE = 10
ALLOWED_EXTENSIONS = {".txt", ".sql", ".xml", ".json", ".md", ".csv", ".xdl", ".sqlplan"}
ALLOWED_CONTENT_TYPE_PREFIX = "text/"


def sanitize_filename(name: str) -> str:
    """Return a safe filename: basename only, no .., only word chars, hyphens, dots."""
    base = os.path.basename(name)
    if not base:
        base = "unnamed"
    safe = re.sub(r"[^\w\-.]", "_", base)
    return safe or "unnamed"

# ---------------------------------------------------------------------------
# REST: list available databases (with descriptions from store)
# ---------------------------------------------------------------------------

@app.get("/api/databases")
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


@app.put("/api/databases/{name}/description")
def api_set_database_description(name: str, body: dict = Body(default={})):
    """Body: {"description": "..."}. Saves to SQLite store."""
    try:
        description = (body or {}).get("description", "") or ""
        set_db_description(name, description)
        return {"ok": True}
    except Exception as e:
        log.error("Failed to set description for %s: %s", name, e)
        return {"ok": False, "error": str(e)}


@app.get("/api/databases/{name}/chats")
def api_list_chats(name: str):
    """List chats for the given database."""
    try:
        chats = list_chats(name)
        return {"chats": chats}
    except Exception as e:
        log.error("Failed to list chats for %s: %s", name, e)
        return {"chats": [], "error": str(e)}


@app.post("/api/databases/{name}/chats")
def api_create_chat(name: str, body: dict = Body(default=None)):
    """Create a new chat for the database. Body optional: {"title": "..."}. Returns {id, title, created_at, starred}."""
    try:
        title = (body or {}).get("title", "Новый чат") or "Новый чат"
        chat = create_chat(name, title)
        return chat
    except Exception as e:
        log.error("Failed to create chat for %s: %s", name, e)
        from fastapi import HTTPException
        raise HTTPException(status_code=500, detail=str(e))


@app.patch("/api/databases/{name}/chats/{chat_id}/title")
def api_set_chat_title(name: str, chat_id: int, body: dict = Body(default=None)):
    """Set chat title. Body: {"title": "..."}. Chat must belong to this database."""
    try:
        _require_chat_belongs_to_db(chat_id, name)
        title = (body or {}).get("title", "Новый чат") or "Новый чат"
        update_chat_title(chat_id, title)
        return {"ok": True, "title": title}
    except Exception as e:
        if hasattr(e, "status_code"):
            raise
        log.error("Failed to set title for chat %s: %s", chat_id, e)
        from fastapi import HTTPException
        raise HTTPException(status_code=500, detail=str(e))


@app.patch("/api/databases/{name}/chats/{chat_id}/star")
def api_set_chat_starred(name: str, chat_id: int, body: dict = Body(default=None)):
    """Set starred flag. Body: {"starred": true|false}. Chat must belong to this database."""
    try:
        _require_chat_belongs_to_db(chat_id, name)
        starred = (body or {}).get("starred", False)
        set_chat_starred(chat_id, bool(starred))
        return {"ok": True, "starred": bool(starred)}
    except Exception as e:
        if hasattr(e, "status_code"):
            raise
        log.error("Failed to set starred for chat %s: %s", chat_id, e)
        from fastapi import HTTPException
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/databases/{name}/chats/{chat_id}")
def api_delete_chat(name: str, chat_id: int):
    """Delete a chat. Chat must belong to this database."""
    try:
        _require_chat_belongs_to_db(chat_id, name)
        delete_chat(chat_id)
        return {"ok": True}
    except HTTPException:
        raise
    except Exception as e:
        log.error("Failed to delete chat %s: %s", chat_id, e)
        raise HTTPException(status_code=500, detail=str(e))


def _is_allowed_file(filename: str, content_type: str | None) -> bool:
    """Allow by extension or text/* content-type."""
    ext = os.path.splitext(filename)[1].lower()
    if ext in ALLOWED_EXTENSIONS:
        return True
    if content_type and content_type.lower().startswith(ALLOWED_CONTENT_TYPE_PREFIX):
        return True
    return False


def _require_chat_belongs_to_db(chat_id: int, name: str) -> str:
    """Ensure chat exists and belongs to the given database. Return database_name or raise HTTPException(404)."""
    db_name = get_chat_database_name(chat_id)
    if db_name is None or db_name != name:
        raise HTTPException(status_code=404, detail="Chat not found")
    return db_name


@app.post("/api/databases/{name}/chats/{chat_id}/files")
async def api_upload_chat_files(
    name: str, chat_id: int, files: list[UploadFile] = File(default=[])
):
    """Upload one or more text files for this chat. Returns uploaded list and optional errors."""
    _require_chat_belongs_to_db(chat_id, name)
    if not files:
        return {"uploaded": [], "errors": ["No files provided"]}

    chat_dir = FILES_DIR / str(chat_id)
    chat_dir.mkdir(parents=True, exist_ok=True)
    uploaded = []
    errors = []
    for f in files[:MAX_FILES_PER_MESSAGE]:
        original = f.filename or "unnamed"
        safe = sanitize_filename(original)
        if not _is_allowed_file(original, f.content_type):
            errors.append(f"{original}: unsupported type (use .txt, .sql, .xml, .json, .md, .csv or text/*)")
            continue
        try:
            body = await f.read()
        except Exception as e:
            errors.append(f"{original}: read failed — {e}")
            continue
        if len(body) > MAX_FILE_SIZE_BYTES:
            errors.append(f"{original}: file too large (max {MAX_FILE_SIZE_BYTES // (1024*1024)} MB)")
            continue
        try:
            decoded = body.decode("utf-8", errors="replace")
        except Exception:
            errors.append(f"{original}: not valid text (UTF-8)")
            continue
        path = chat_dir / safe
        try:
            path.write_text(decoded, encoding="utf-8")
        except Exception as e:
            errors.append(f"{original}: write failed — {e}")
            continue
        uploaded.append({"filename": original, "saved_as": safe})
    return {"uploaded": uploaded, "errors": errors if errors else None}


@app.get("/api/databases/{name}/chats/{chat_id}/files/{filename}")
def api_get_chat_file(name: str, chat_id: int, filename: str):
    """Download a previously uploaded chat file. Filename is sanitized."""
    _require_chat_belongs_to_db(chat_id, name)
    safe = sanitize_filename(filename)
    if not safe:
        raise HTTPException(status_code=400, detail="Invalid filename")
    path = FILES_DIR / str(chat_id) / safe
    if not path.is_file():
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(path, filename=safe)

# ---------------------------------------------------------------------------
# WebSocket: chat with agent
# ---------------------------------------------------------------------------

MAX_TOOL_ROUNDS = 10
# Maximum length of the tool result going into the LLM context (protection against bloating from large samples/JSON)
MAX_TOOL_RESULT_LENGTH = 80_000


def _parse_tool_args(tc: dict) -> dict:
    """Parse tool call arguments JSON. Returns {} on decode error."""
    try:
        return json.loads(tc["function"]["arguments"])
    except (json.JSONDecodeError, KeyError, TypeError):
        return {}


def _format_tool_call_content(name: str, args: dict) -> str:
    """Format tool name and args as a single string for chat display, e.g. get_indexes(table_name=Companies, schema=cab)."""
    parts = [f"{k}={v}" for k, v in (args or {}).items()]
    return f"{name}({', '.join(parts)})"


@app.websocket("/ws")
async def ws_chat(ws: WebSocket):
    await ws.accept()
    messages: list[ChatMessage] = []
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
                messages.clear()
                history = get_chat_messages(chat_id)
                messages.extend(history)
                await ws.send_text(json.dumps({
                    "type": "history_loaded",
                    "messages": [{"role": m.role, "content": m.content} for m in history],
                }))
                continue
            
            if payload.get("type") == "set_database":
                db_name = payload.get("database")
                if not db_name:
                    await ws.send_text(json.dumps({"type": "error", "content": "database required"}))
                    continue
                database = str(db_name)
                chat_id = None
                messages.clear()
                await ws.send_text(json.dumps({"type": "history_loaded", "messages": []}))
                continue
            
            if payload.get("type") == "create_chat":
                if not database:
                    await ws.send_text(json.dumps({"type": "error", "content": "Select a database first."}))
                    continue
                title = payload.get("title", "Новый чат") or "Новый чат"
                chat = create_chat(database, title)
                chat_id = chat["id"]
                messages.clear()
                await ws.send_text(json.dumps({
                    "type": "chat_created",
                    "chat": chat,
                }))
                await ws.send_text(json.dumps({"type": "history_loaded", "messages": []}))
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
                for name in attachments:
                    safe = sanitize_filename(name)
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

                messages.append(ChatMessage(role="user", content=full_content))
                append_chat_messages(chat_id, [messages[-1]])

                await _agent_loop(ws, messages, database, agent_role, chat_id)
                continue

    except WebSocketDisconnect:
        log.info("Client disconnected")
    except Exception as e:
        log.exception("WebSocket error")
        try:
            await ws.send_text(json.dumps({"type": "error", "content": str(e)}))
        except Exception:
            pass


async def _agent_loop(
    ws: WebSocket,
    messages: list[ChatMessage],
    database: str,
    agent_role: str,
    chat_id: int | None,
):
    # System prompt with database context for AI
    description = get_db_description(database) or ""
    db_context = f"\n\nYou are working with database: {database}."
    if description:
        db_context += f" User-provided context: {description}"
    db_context += "\n"
    system_content = get_system_prompt(agent_role) + db_context

    # Build API messages: Gemini accepts only "user", "assistant", "system", "tool".
    # History stores "tool_call" for display; convert to "tool" with placeholder result.
    api_messages: list[dict] = []
    tool_call_idx = 0
    for m in messages:
        if m.role == "tool_call":
            continue
        else:
            api_messages.append({"role": m.role, "content": m.content})
    full_messages: list[dict] = [{"role": "system", "content": system_content}] + api_messages

    if llm_client is None:
        await ws.send_text(json.dumps({"type": "error", "content": "LLM not configured: set API_KEY and API_URL."}))
        return

    for round_num in range(MAX_TOOL_ROUNDS):
        log.info("Agent round %d, messages: %d", round_num + 1, len(full_messages))

        try:
            # enable streaming in llm client
            response = llm_client.chat.completions.create(
                model=LLM_MODEL,
                messages=full_messages,
                tools=TOOL_DEFINITIONS,
                temperature=0.2,
                stream=True, 
            )
        except Exception as e:
            log.error("LLM call failed: %s", e)
            await ws.send_text(json.dumps({"type": "error", "content": f"LLM error: {e}"}))
            return

        collected_msg = ""
        tools_acc = {}
        next_auto_index = 0  # when API omits index in chunk, assign 0, 1, 2...

        # process chunks as they arrive
        for chunk in response:
            delta = chunk.choices[0].delta

            # stream normal text directly to frontend
            if delta.content:
                collected_msg += delta.content
                await ws.send_text(json.dumps({
                    "type": "stream",
                    "content": delta.content
                }))

            # accumulate tool call chunks
            if delta.tool_calls:
                for tc in delta.tool_calls:
                    # Gemini streaming sometimes omits index; use next sequential index
                    idx = tc.index if isinstance(tc.index, int) and tc.index >= 0 else next_auto_index
                    if idx >= next_auto_index:
                        next_auto_index = idx + 1
                    if idx not in tools_acc:
                        tools_acc[idx] = {
                            "id": tc.id or "",
                            "type": "function",
                            "function": {"name": "", "arguments": ""}
                        }
                    if tc.function.name:
                        tools_acc[idx]["function"]["name"] += tc.function.name
                    if tc.function.arguments:
                        tools_acc[idx]["function"]["arguments"] += tc.function.arguments
                    if tc.id:
                        tools_acc[idx]["id"] += tc.id

        # if tools were called, execute them and continue the loop
        if tools_acc:
            # Ensure tool_calls are in index order (0, 1, 2...) for Gemini API
            sorted_calls = [v for _, v in sorted(tools_acc.items())]
            assistant_msg = {
                "role": "assistant",
                "content": collected_msg if collected_msg else "",  # Gemini rejects null content
                "tool_calls": sorted_calls,
            }
            full_messages.append(assistant_msg)

            for tc in sorted_calls:
                t_name = tc["function"]["name"]
                t_args = _parse_tool_args(tc)
                log.info("Tool call: %s(%s)", t_name, t_args)
                await ws.send_text(json.dumps({
                    "type": "tool_call",
                    "tool": t_name,
                    "args": t_args,
                }))

                result = dispatch_tool(t_name, t_args, database)
                if len(result) > MAX_TOOL_RESULT_LENGTH:
                    result = result[:MAX_TOOL_RESULT_LENGTH] + "\n\n[... result truncated due to size ...]"
                full_messages.append({
                    "role": "tool",
                    "tool_call_id": tc["id"],
                    "content": result,
                })

            # Persist assistant + tool calls to chat (no tool results)
            if chat_id is not None:
                to_append: list[ChatMessage] = [
                    ChatMessage(role="assistant", content=collected_msg or ""),
                ]
                for tc in sorted_calls:
                    t_name = tc["function"]["name"]
                    t_args = _parse_tool_args(tc)
                    to_append.append(
                        ChatMessage(
                            role="tool_call",
                            content=_format_tool_call_content(t_name, t_args),
                        )
                    )
                append_chat_messages(chat_id, to_append)

            # go to the next react loop iteration
            continue

        # no tools called, meaning this is the final answer
        messages.append(ChatMessage(role=agent_role, content=collected_msg))
        if chat_id is not None:
            append_chat_messages(chat_id, [ChatMessage(role=agent_role, content=collected_msg)])
        await ws.send_text(json.dumps({"type": "stream_end"}))
        return

    # safety limit reached
    err_msg = "Agent reached maximum tool call rounds."
    await ws.send_text(json.dumps({"type": "error", "content": err_msg}))


# ---------------------------------------------------------------------------
# Serve frontend static files
# ---------------------------------------------------------------------------

_frontend_dir = Path(__file__).resolve().parent.parent / "frontend"
app.mount("/", StaticFiles(directory=str(_frontend_dir), html=True), name="frontend")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8888, reload=True)
