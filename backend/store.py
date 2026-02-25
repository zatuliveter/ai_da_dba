"""
SQLite store: database descriptions, chats, and chat messages.
DB file: backend/data/app.db (created on first use).
"""
import os
import sqlite3
from datetime import datetime
from pathlib import Path

DB_DIR = Path(__file__).resolve().parent / "data"
DB_PATH = DB_DIR / "app.db"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS database_descriptions (
    name TEXT PRIMARY KEY,
    description TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS chats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    database_name TEXT NOT NULL,
    title TEXT NOT NULL DEFAULT 'Новый чат',
    created_at TEXT NOT NULL,
    starred INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS chat_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id INTEGER NOT NULL REFERENCES chats(id) ON DELETE CASCADE,
    role TEXT NOT NULL,
    content TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_chats_database_name ON chats(database_name);
CREATE INDEX IF NOT EXISTS idx_chat_messages_chat_id ON chat_messages(chat_id);
"""

# Max length for one message content to avoid DB bloat from runaway model output
MAX_MESSAGE_CONTENT_LENGTH = 200_000


def _ensure_dir():
    DB_DIR.mkdir(parents=True, exist_ok=True)


def _get_conn() -> sqlite3.Connection:
    _ensure_dir()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    """Create DB file and tables if they do not exist."""
    conn = _get_conn()
    try:
        conn.executescript(_SCHEMA)
        conn.commit()
        # Migration: add starred column if missing (existing DBs)
        cur = conn.execute("PRAGMA table_info(chats)")
        columns = [row[1] for row in cur.fetchall()]
        if "starred" not in columns:
            conn.execute("ALTER TABLE chats ADD COLUMN starred INTEGER NOT NULL DEFAULT 0")
            conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Database descriptions
# ---------------------------------------------------------------------------

def get_db_description(name: str) -> str:
    """Return description for a database by name. Empty string if not set."""
    init_db()
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT description FROM database_descriptions WHERE name = ?",
            (name,),
        ).fetchone()
        return row[0] if row else ""


def set_db_description(name: str, description: str) -> None:
    """Insert or replace description for a database."""
    init_db()
    with _get_conn() as conn:
        conn.execute(
            "INSERT INTO database_descriptions (name, description) VALUES (?, ?) "
            "ON CONFLICT(name) DO UPDATE SET description = excluded.description",
            (name, description or ""),
        )
        conn.commit()


# ---------------------------------------------------------------------------
# Chats
# ---------------------------------------------------------------------------

def list_chats(database_name: str) -> list[dict]:
    """Return list of chats for the given database, starred first then newest first."""
    init_db()
    with _get_conn() as conn:
        rows = conn.execute(
            "SELECT id, title, created_at, starred FROM chats WHERE database_name = ? "
            "ORDER BY created_at DESC",
            (database_name,),
        ).fetchall()
        return [
            {
                "id": r["id"],
                "title": r["title"],
                "created_at": r["created_at"],
                "starred": bool(r["starred"]),
            }
            for r in rows
        ]


def create_chat(database_name: str, title: str = "Новый чат") -> dict:
    """Create a new chat for the database. Returns {id, title, created_at, starred}."""
    init_db()
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    with _get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO chats (database_name, title, created_at, starred) VALUES (?, ?, ?, 0)",
            (database_name, title or "Новый чат", now),
        )
        conn.commit()
        chat_id = cur.lastrowid
    return {"id": chat_id, "title": title or "Новый чат", "created_at": now, "starred": False}


def get_chat_messages(chat_id: int) -> list[dict]:
    """Load all messages for a chat as list of {role, content}."""
    init_db()
    with _get_conn() as conn:
        rows = conn.execute(
            "SELECT role, content FROM chat_messages WHERE chat_id = ? ORDER BY id ASC",
            (chat_id,),
        ).fetchall()
        return [{"role": r["role"], "content": r["content"]} for r in rows]


def append_chat_messages(chat_id: int, messages: list[dict]) -> None:
    """Append messages to a chat. Each msg: {role, content}. Content is truncated to MAX_MESSAGE_CONTENT_LENGTH."""
    if not messages:
        return
    init_db()
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    with _get_conn() as conn:
        for msg in messages:
            content = msg.get("content") or ""
            content = content.strip()
            if len(content) > MAX_MESSAGE_CONTENT_LENGTH:
                content = content[:MAX_MESSAGE_CONTENT_LENGTH] + "\n\n[... message truncated due to size ...]"
            conn.execute(
                "INSERT INTO chat_messages (chat_id, role, content, created_at) VALUES (?, ?, ?, ?)",
                (chat_id, msg["role"], content, now),
            )
        conn.commit()


def update_chat_title(chat_id: int, title: str) -> None:
    """Update chat title (e.g. after first user message)."""
    init_db()
    with _get_conn() as conn:
        conn.execute("UPDATE chats SET title = ? WHERE id = ?", (title, chat_id))
        conn.commit()


def get_chat_database_name(chat_id: int) -> str | None:
    """Return database_name for the chat, or None if not found."""
    init_db()
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT database_name FROM chats WHERE id = ?", (chat_id,)
        ).fetchone()
        return row["database_name"] if row else None


def set_chat_starred(chat_id: int, starred: bool) -> None:
    """Set or unset the starred flag for a chat."""
    init_db()
    with _get_conn() as conn:
        conn.execute(
            "UPDATE chats SET starred = ? WHERE id = ?",
            (1 if starred else 0, chat_id),
        )
        conn.commit()


def delete_chat(chat_id: int) -> None:
    """Delete a chat and its messages (CASCADE)."""
    init_db()
    with _get_conn() as conn:
        conn.execute("DELETE FROM chats WHERE id = ?", (chat_id,))
        conn.commit()


def fix_oversized_message_contents(max_length: int = MAX_MESSAGE_CONTENT_LENGTH) -> int:
    """Replace oversized message content with truncated version. Returns number of rows updated."""
    init_db()
    with _get_conn() as conn:
        rows = conn.execute(
            "SELECT id, content FROM chat_messages WHERE length(content) > ?",
            (max_length,),
        ).fetchall()
        for row in rows:
            new_content = row["content"][:max_length] + "\n\n[... сообщение обрезано из-за большого объёма ...]"
            conn.execute("UPDATE chat_messages SET content = ? WHERE id = ?", (new_content, row["id"]))
        conn.commit()
        return len(rows)
