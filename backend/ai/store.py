"""
SQLite store: database descriptions, chats, and chat messages.
DB file: data/app.db (created on first use).
"""
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime
import logging

from backend.config import DATA_DIR, SQL_SERVER

log = logging.getLogger(__name__)

DB_PATH = DATA_DIR / "app.db"


@dataclass(frozen=True)
class ChatMessage:
    """A single chat message with strict role and content. Used for history and persistence."""
    role: str
    content: str
    tool_result: str | None = None
    tool_call_id: str | None = None
    tool_calls: list | None = None
    prompt_tokens: int | None = None
    cached_tokens: int | None = None
    completion_tokens: int | None = None


_SCHEMA = """
CREATE TABLE IF NOT EXISTS database_descriptions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    description TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS chats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    database_id INTEGER REFERENCES database_descriptions(id) ON DELETE CASCADE,
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



def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db() -> None:
    """Create DB file and tables if they do not exist. Run migrations for existing DBs."""
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

        # Migration: database_descriptions.id (identity) + chats.database_id
        cur = conn.execute("PRAGMA table_info(database_descriptions)")
        dd_columns = [row[1] for row in cur.fetchall()]
        if "id" not in dd_columns:
            conn.execute(
                "CREATE TABLE database_descriptions_new ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                "name TEXT NOT NULL UNIQUE, "
                "description TEXT NOT NULL DEFAULT ''"
                ")"
            )
            conn.execute(
                "INSERT INTO database_descriptions_new (name, description) "
                "SELECT name, description FROM database_descriptions"
            )
            conn.execute("DROP TABLE database_descriptions")
            conn.execute("ALTER TABLE database_descriptions_new RENAME TO database_descriptions")
            conn.commit()

        cur = conn.execute("PRAGMA table_info(chats)")
        columns = [row[1] for row in cur.fetchall()]
        if "database_id" not in columns:
            conn.execute(
                "ALTER TABLE chats ADD COLUMN database_id INTEGER REFERENCES database_descriptions(id)"
            )
            conn.execute(
                "INSERT OR IGNORE INTO database_descriptions (name, description) "
                "SELECT DISTINCT database_name, '' FROM chats"
            )
            conn.execute(
                "UPDATE chats SET database_id = ("
                "SELECT id FROM database_descriptions WHERE database_descriptions.name = chats.database_name"
                ")"
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_chats_database_id ON chats(database_id)")
            conn.commit()

        conn.execute("CREATE INDEX IF NOT EXISTS idx_chats_database_id ON chats(database_id)")
        conn.commit()

        # Migration: add tool_result, tool_call_id, tool_calls_json to chat_messages
        cur = conn.execute("PRAGMA table_info(chat_messages)")
        cm_columns = [row[1] for row in cur.fetchall()]
        if "tool_result" not in cm_columns:
            conn.execute("ALTER TABLE chat_messages ADD COLUMN tool_result TEXT")
            conn.commit()
        if "tool_call_id" not in cm_columns:
            conn.execute("ALTER TABLE chat_messages ADD COLUMN tool_call_id TEXT")
            conn.commit()
        if "tool_calls_json" not in cm_columns:
            conn.execute("ALTER TABLE chat_messages ADD COLUMN tool_calls_json TEXT")
            conn.commit()

        # Migration: LLM usage per assistant turn
        cur = conn.execute("PRAGMA table_info(chat_messages)")
        cm_columns = [row[1] for row in cur.fetchall()]
        if "prompt_tokens" not in cm_columns:
            conn.execute("ALTER TABLE chat_messages ADD COLUMN prompt_tokens INTEGER")
            conn.commit()
        if "cached_tokens" not in cm_columns:
            conn.execute("ALTER TABLE chat_messages ADD COLUMN cached_tokens INTEGER")
            conn.commit()
        if "completion_tokens" not in cm_columns:
            conn.execute("ALTER TABLE chat_messages ADD COLUMN completion_tokens INTEGER")
            conn.commit()

        # Migration: MSSQL connections + database_descriptions scoped by connection_id
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS mssql_connections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                label TEXT NOT NULL,
                connection_string TEXT NOT NULL
            )
            """
        )
        conn.commit()
        cnt = conn.execute("SELECT COUNT(*) AS c FROM mssql_connections").fetchone()["c"]
        if cnt == 0:
            default_cs = f"SERVER={SQL_SERVER};Trusted_Connection=yes;"
            conn.execute(
                "INSERT INTO mssql_connections (label, connection_string) VALUES (?, ?)",
                ("Default", default_cs),
            )
            conn.commit()

        cur = conn.execute("PRAGMA table_info(database_descriptions)")
        dd_cols = [row[1] for row in cur.fetchall()]
        if "connection_id" not in dd_cols:
            default_cid = conn.execute(
                "SELECT id FROM mssql_connections ORDER BY id LIMIT 1"
            ).fetchone()["id"]
            conn.execute(
                """
                CREATE TABLE database_descriptions_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    connection_id INTEGER NOT NULL REFERENCES mssql_connections(id) ON DELETE CASCADE,
                    name TEXT NOT NULL,
                    description TEXT NOT NULL DEFAULT '',
                    UNIQUE(connection_id, name)
                )
                """
            )
            conn.execute(
                """
                INSERT INTO database_descriptions_new (id, connection_id, name, description)
                SELECT id, ?, name, description FROM database_descriptions
                """,
                (default_cid,),
            )
            conn.execute("DROP TABLE database_descriptions")
            conn.execute("ALTER TABLE database_descriptions_new RENAME TO database_descriptions")
            conn.commit()

    finally:
        conn.close()


# ---------------------------------------------------------------------------
# MSSQL connections (full connection strings; secrets not exposed via list API)
# ---------------------------------------------------------------------------


def list_mssql_connections() -> list[dict]:
    """Return {id, label} for each saved connection."""
    with _get_conn() as conn:
        rows = conn.execute(
            "SELECT id, label FROM mssql_connections ORDER BY id ASC"
        ).fetchall()
        return [{"id": r["id"], "label": r["label"]} for r in rows]


def add_mssql_connection(label: str, connection_string: str) -> int:
    """Insert a connection. Returns new id."""
    with _get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO mssql_connections (label, connection_string) VALUES (?, ?)",
            (label.strip() or "Connection", (connection_string or "").strip()),
        )
        conn.commit()
        return int(cur.lastrowid)


def get_connection_string(connection_id: int) -> str | None:
    """Return stored connection string or None if id missing."""
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT connection_string FROM mssql_connections WHERE id = ?",
            (connection_id,),
        ).fetchone()
        return row["connection_string"] if row else None


def delete_mssql_connection(connection_id: int) -> bool:
    """Delete connection and CASCADE database_descriptions/chats for that connection. Returns False if id missing."""
    with _get_conn() as conn:
        cur = conn.execute("DELETE FROM mssql_connections WHERE id = ?", (connection_id,))
        conn.commit()
        return cur.rowcount > 0


# ---------------------------------------------------------------------------
# Database descriptions (scoped by connection_id)
# ---------------------------------------------------------------------------


def get_db_description(connection_id: int, name: str) -> str:
    """Return description for a database on a connection. Empty string if not set."""
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT description FROM database_descriptions WHERE connection_id = ? AND name = ?",
            (connection_id, name),
        ).fetchone()
        return row[0] if row else ""


def set_db_description(connection_id: int, name: str, description: str) -> None:
    """Insert or update description for (connection_id, name)."""
    with _get_conn() as conn:
        conn.execute(
            """
            INSERT INTO database_descriptions (connection_id, name, description)
            VALUES (?, ?, ?)
            ON CONFLICT(connection_id, name) DO UPDATE SET description = excluded.description
            """,
            (connection_id, name, description or ""),
        )
        conn.commit()


def get_or_create_database_id(connection_id: int, name: str) -> int:
    """Return database_descriptions.id; create row with empty description if missing."""
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT id FROM database_descriptions WHERE connection_id = ? AND name = ?",
            (connection_id, name),
        ).fetchone()
        if row:
            return int(row["id"])
        cur = conn.execute(
            "INSERT INTO database_descriptions (connection_id, name, description) VALUES (?, ?, '')",
            (connection_id, name),
        )
        conn.commit()
        return int(cur.lastrowid)


# ---------------------------------------------------------------------------
# Chats
# ---------------------------------------------------------------------------

def list_chats(connection_id: int, database_name: str) -> list[dict]:
    """Return chats for the given database on the given connection."""
    with _get_conn() as conn:
        rows = conn.execute(
            """
            SELECT c.id, c.title, c.created_at, c.starred FROM chats c
            INNER JOIN database_descriptions dd ON c.database_id = dd.id
            WHERE dd.connection_id = ? AND dd.name = ?
            ORDER BY c.created_at DESC
            """,
            (connection_id, database_name),
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


def create_chat(connection_id: int, database_name: str, title: str = "Новый чат") -> dict:
    """Create a new chat for the database on this connection. Returns {id, title, created_at, starred}."""
    database_id = get_or_create_database_id(connection_id, database_name)
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    with _get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO chats (database_id, database_name, title, created_at, starred) VALUES (?, ?, ?, ?, 0)",
            (database_id, database_name, title or "Новый чат", now),
        )
        conn.commit()
        chat_id = cur.lastrowid
    return {"id": chat_id, "title": title or "Новый чат", "created_at": now, "starred": False}


def get_chat_messages(chat_id: int) -> list[ChatMessage]:
    """Load all messages for a chat as list of ChatMessage."""
    with _get_conn() as conn:
        rows = conn.execute("""
            SELECT role, content, tool_result, tool_call_id, tool_calls_json, 
                   prompt_tokens, cached_tokens, completion_tokens 
            FROM chat_messages 
            WHERE chat_id = ? 
            ORDER BY id ASC
            """,
            (chat_id,),
        ).fetchall()
        out = []
        for r in rows:
            tool_calls = None
            tool_calls_json = r["tool_calls_json"]
            if tool_calls_json:
                try:
                    tool_calls = json.loads(tool_calls_json)
                except (json.JSONDecodeError, TypeError) as e:
                    log.error("Error loading tool_calls_json: %s, content: %s", e, tool_calls_json)
                    pass
            out.append(
                ChatMessage(
                    role=r["role"],
                    content=r["content"] or "",
                    tool_result=r["tool_result"] if r["tool_result"] else None,
                    tool_call_id=r["tool_call_id"] if r["tool_call_id"] else None,
                    tool_calls=tool_calls,
                    prompt_tokens=r["prompt_tokens"],
                    cached_tokens=r["cached_tokens"],
                    completion_tokens=r["completion_tokens"],
                )
            )
        return out


def append_chat_messages(chat_id: int, messages: list[ChatMessage]) -> None:
    """Append messages to a chat. Content and tool_result are truncated to MAX_MESSAGE_CONTENT_LENGTH."""
    if not messages:
        return
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    with _get_conn() as conn:
        for msg in messages:
            content = (msg.content or "").strip()
            if len(content) > MAX_MESSAGE_CONTENT_LENGTH:
                content = content[:MAX_MESSAGE_CONTENT_LENGTH] + "\n\n[... message truncated due to size ...]"
            tool_result = getattr(msg, "tool_result", None) or None
            if tool_result and len(tool_result) > MAX_MESSAGE_CONTENT_LENGTH:
                tool_result = tool_result[:MAX_MESSAGE_CONTENT_LENGTH] + "\n\n[... result truncated due to size ...]"
            tool_call_id = getattr(msg, "tool_call_id", None) or None
            tool_calls_json = None
            if getattr(msg, "tool_calls", None):
                try:
                    tool_calls_json = json.dumps(msg.tool_calls)
                except (TypeError, ValueError):
                    pass
            pt = getattr(msg, "prompt_tokens", None)
            ct_cached = getattr(msg, "cached_tokens", None)
            ct_comp = getattr(msg, "completion_tokens", None)
            conn.execute(
                "INSERT INTO chat_messages (chat_id, role, content, created_at, tool_result, tool_call_id, tool_calls_json, "
                "prompt_tokens, cached_tokens, completion_tokens) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    chat_id,
                    msg.role,
                    content,
                    now,
                    tool_result,
                    tool_call_id,
                    tool_calls_json,
                    pt,
                    ct_cached,
                    ct_comp,
                ),
            )
        conn.commit()


def get_chat_token_stats(chat_id: int) -> dict:
    """Aggregates for footer UI: last prompt for latest LLM turn and sums over the chat."""
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT "
            "(SELECT prompt_tokens FROM chat_messages WHERE chat_id = ? AND prompt_tokens IS NOT NULL "
            "ORDER BY id DESC LIMIT 1) AS last_prompt_tokens, "
            "COALESCE((SELECT SUM(prompt_tokens) FROM chat_messages WHERE chat_id = ?), 0) AS total_prompt_tokens, "
            "COALESCE((SELECT SUM(cached_tokens) FROM chat_messages WHERE chat_id = ?), 0) AS total_cached_tokens, "
            "COALESCE((SELECT SUM(completion_tokens) FROM chat_messages WHERE chat_id = ?), 0) AS total_completion_tokens",
            (chat_id, chat_id, chat_id, chat_id),
        ).fetchone()
    last_p = row["last_prompt_tokens"]
    return {
        "last_prompt_tokens": int(last_p) if last_p is not None else 0,
        "total_prompt_tokens": int(row["total_prompt_tokens"] or 0),
        "total_cached_tokens": int(row["total_cached_tokens"] or 0),
        "total_completion_tokens": int(row["total_completion_tokens"] or 0),
    }


def update_chat_title(chat_id: int, title: str) -> None:
    """Update chat title (e.g. after first user message)."""
    with _get_conn() as conn:
        conn.execute("UPDATE chats SET title = ? WHERE id = ?", (title, chat_id))
        conn.commit()


def get_chat_database_name(chat_id: int) -> str | None:
    """Return database_name for the chat, or None if not found."""
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT database_name FROM chats WHERE id = ?", (chat_id,)
        ).fetchone()
        return row["database_name"] if row else None


def get_chat_scope(chat_id: int) -> tuple[int, str] | None:
    """Return (connection_id, database name) for the chat, or None."""
    with _get_conn() as conn:
        row = conn.execute(
            """
            SELECT dd.connection_id, dd.name AS db_name
            FROM chats c
            INNER JOIN database_descriptions dd ON c.database_id = dd.id
            WHERE c.id = ?
            """,
            (chat_id,),
        ).fetchone()
        if not row:
            return None
        return (int(row["connection_id"]), row["db_name"])


def set_chat_starred(chat_id: int, starred: bool) -> None:
    """Set or unset the starred flag for a chat."""
    with _get_conn() as conn:
        conn.execute(
            "UPDATE chats SET starred = ? WHERE id = ?",
            (1 if starred else 0, chat_id),
        )
        conn.commit()


def delete_chat(chat_id: int) -> None:
    """Delete a chat and its messages (CASCADE)."""
    with _get_conn() as conn:
        conn.execute("DELETE FROM chats WHERE id = ?", (chat_id,))
        conn.commit()


def fix_oversized_message_contents(max_length: int = MAX_MESSAGE_CONTENT_LENGTH) -> int:
    """Replace oversized message content with truncated version. Returns number of rows updated."""
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
