import pyodbc
import yaml
from config import SQL_SERVER

MAX_ROWS = 1000
QUERY_TIMEOUT = 30

_connection_string = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={SQL_SERVER};"
    f"Trusted_Connection=yes;"
)


def get_connection(database: str | None = None) -> pyodbc.Connection:
    cs = _connection_string
    if database:
        cs += f"DATABASE={database};"
    return pyodbc.connect(cs, timeout=QUERY_TIMEOUT)


def list_databases() -> list[str]:
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sys.databases "
            "WHERE state_desc = 'ONLINE' "
            "AND name NOT IN ('master', 'tempdb', 'model', 'msdb') "
            "ORDER BY name"
        )
        return [row[0] for row in cursor.fetchall()]


def rows_to_yaml(cursor: pyodbc.Cursor, max_rows: int = MAX_ROWS) -> str:
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


def execute_query(database: str, sql: str, params: tuple = ()) -> str:
    with get_connection(database) as conn:
        cursor = conn.cursor()
        cursor.execute(sql, params)
        if cursor.description:
            return rows_to_yaml(cursor)
        return yaml.dump({"affected_rows": cursor.rowcount}, allow_unicode=True)
