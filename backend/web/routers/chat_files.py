import os
import re

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from fastapi.responses import FileResponse

from backend.config import DATA_DIR
from backend.web.common.dependencies import require_chat_belongs_to_db

router = APIRouter(prefix="/api/databases", tags=["chat_files"])

FILES_DIR = DATA_DIR / "files"
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


def _is_allowed_file(filename: str, content_type: str | None) -> bool:
    """Allow by extension or text/* content-type."""
    ext = os.path.splitext(filename)[1].lower()
    if ext in ALLOWED_EXTENSIONS:
        return True
    if content_type and content_type.lower().startswith(ALLOWED_CONTENT_TYPE_PREFIX):
        return True
    return False


@router.post("/{name}/chats/{chat_id}/files")
async def api_upload_chat_files(
    name: str,
    chat_id: int,
    files: list[UploadFile] = File(default=[]),
    _: str = Depends(require_chat_belongs_to_db),
):
    """Upload one or more text files for this chat. Returns uploaded list and optional errors."""
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


@router.get("/{name}/chats/{chat_id}/files/{filename}")
def api_get_chat_file(
    name: str,
    chat_id: int,
    filename: str,
    _: str = Depends(require_chat_belongs_to_db),
):
    """Download a previously uploaded chat file. Filename is sanitized."""
    safe = sanitize_filename(filename)
    if not safe:
        raise HTTPException(status_code=400, detail="Invalid filename")
    path = FILES_DIR / str(chat_id) / safe
    if not path.is_file():
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(path, filename=safe)
