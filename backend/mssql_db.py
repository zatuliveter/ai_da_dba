import yaml
from mssql_python import Cursor, SQL_ATTR_LOGIN_TIMEOUT, connect

from backend.ai.store import get_connection_string

MAX_ROWS = 1000
QUERY_TIMEOUT = 30

# Keys that pin the default database on the instance (strip for listing / override for queries)
_DB_KEYS = frozenset({"DATABASE", "INITIAL CATALOG"})


def _parse_connection_segments(raw: str) -> list[tuple[str, str]]:
    """Parse 'KEY=value;...' pairs preserving original key spelling for the first occurrence."""
    pairs: list[tuple[str, str]] = []
    seen_upper: set[str] = set()
    for segment in raw.split(";"):
        seg = segment.strip()
        if not seg:
            continue
        if "=" not in seg:
            continue
        key, val = seg.split("=", 1)
        ku = key.strip().upper()
        if ku in _DB_KEYS:
            continue
        k0 = key.strip()
        if ku in seen_upper:
            continue
        seen_upper.add(ku)
        pairs.append((k0, val.strip()))
    return pairs


def connection_string_for_instance_scope(raw: str) -> str:
    """Force connection to master for listing databases / server-level queries."""
    pairs = _parse_connection_segments(raw)
    pairs.append(("DATABASE", "master"))
    return ";".join(f"{k}={v}" for k, v in pairs)


def connection_string_with_database(raw: str, database: str) -> str:
    """Build a string that uses the given database name."""
    pairs = _parse_connection_segments(raw)
    pairs.append(("DATABASE", database))
    return ";".join(f"{k}={v}" for k, v in pairs)


def list_databases(connection_string: str) -> list[str]:
    cs = connection_string_for_instance_scope(connection_string)
    with connect(cs, attrs_before={SQL_ATTR_LOGIN_TIMEOUT: QUERY_TIMEOUT}) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sys.databases "
            "WHERE state_desc = 'ONLINE' "
            "AND name NOT IN ('master', 'tempdb', 'model', 'msdb') "
            "ORDER BY name"
        )
        return [row[0] for row in cursor.fetchall()]


def rows_to_yaml(cursor: Cursor, max_rows: int = MAX_ROWS) -> str:
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchmany(max_rows + 1)

    truncated = len(rows) > max_rows
    if truncated:
        rows = rows[:max_rows]

    result = []
    for row in rows:
        record = {}
        for col, val in zip(columns, row):
            if isinstance(val, (bytes, bytearray)):
                record[col] = val.hex()
            elif isinstance(val, (str, int, float, bool, type(None))):
                record[col] = val
            else:
                record[col] = str(val)
        result.append(record)

    if truncated:
        data = {
            "rows": result,
            "truncated": True,
            "note": f"Showing first {max_rows} rows",
        }
        return yaml.dump(data, allow_unicode=True)
    return yaml.dump(result, allow_unicode=True)


def execute_query(connection_id: int, database: str, sql: str, params: tuple = ()) -> str:
    raw = get_connection_string(connection_id)
    if not raw:
        return yaml.dump({"error": "Unknown connection_id"}, allow_unicode=True)
    cs = connection_string_with_database(raw, database)
    with connect(cs, attrs_before={SQL_ATTR_LOGIN_TIMEOUT: QUERY_TIMEOUT}) as conn:
        cursor = conn.cursor()
        cursor.execute(sql, params)
        if cursor.description:
            return rows_to_yaml(cursor)
        return yaml.dump({"affected_rows": cursor.rowcount}, allow_unicode=True)


def execute_scalar(connection_id: int, database: str, sql: str, params: tuple = ()) -> str | None:
    raw = get_connection_string(connection_id)
    if not raw:
        return None
    cs = connection_string_with_database(raw, database)
    with connect(cs, attrs_before={SQL_ATTR_LOGIN_TIMEOUT: QUERY_TIMEOUT}) as conn:
        cursor = conn.cursor()
        cursor.execute(sql, params)

        if not cursor.description:
            return None

        row = cursor.fetchone()

        if row is None:
            return None

        value = row[0]

        if value is None:
            return None

        return str(value)
