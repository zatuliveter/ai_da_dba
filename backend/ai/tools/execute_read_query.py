import re

import yaml

from backend.db import MAX_ROWS, execute_query


def execute_read_query(database: str, query: str) -> str:
    normalized = re.sub(r"--[^\n]*", "", query)
    normalized = re.sub(r"/\*.*?\*/", "", normalized, flags=re.DOTALL)
    normalized = normalized.strip().upper()

    if not normalized.startswith("SELECT") and not normalized.startswith("WITH"):
        return yaml.dump({"error": "Only SELECT queries are allowed"}, allow_unicode=True)

    forbidden = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE",
                 "TRUNCATE", "EXEC", "EXECUTE", "MERGE", "GRANT", "REVOKE"]
    tokens = re.findall(r'\b[A-Z]+\b', normalized)
    for token in tokens:
        if token in forbidden:
            return yaml.dump({"error": f"Forbidden keyword: {token}"}, allow_unicode=True)

    return execute_query(database, query)


definition = {
    "type": "function",
    "function": {
        "name": "execute_read_query",
        "description": f"Execute a read-only SELECT query against the database. Returns up to {MAX_ROWS} rows. Only SELECT/WITH statements are allowed.",
        "parameters": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "The SELECT query to execute"},
            },
            "required": ["query"],
        },
    },
}
