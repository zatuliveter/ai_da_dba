from backend.mssql_db import execute_query


def get_missing_indexes(database: str, table_name: str | None = None, schema: str = "dbo") -> str:
    sql = """
        SELECT
            s.name AS schema_name,
            OBJECT_NAME(mid.object_id) AS table_name,
            mid.equality_columns,
            mid.inequality_columns,
            mid.included_columns,
            migs.avg_user_impact,
            migs.user_seeks,
            migs.user_scans,
            migs.last_user_seek
        FROM sys.dm_db_missing_index_details mid
        JOIN sys.dm_db_missing_index_groups mig ON mid.index_handle = mig.index_handle
        JOIN sys.dm_db_missing_index_group_stats migs ON mig.index_group_handle = migs.group_handle
        JOIN sys.schemas s ON mid.object_id = OBJECT_ID(QUOTENAME(s.name) + '.' + QUOTENAME(OBJECT_NAME(mid.object_id)))
        WHERE mid.database_id = DB_ID()
    """
    params: list[str] = []
    if table_name:
        sql += " AND OBJECT_NAME(mid.object_id) = ? AND s.name = ?"
        params.extend([table_name, schema])

    sql += " ORDER BY migs.avg_user_impact * (migs.user_seeks + migs.user_scans) DESC"

    return execute_query(database, sql, tuple(params))


definition = {
    "type": "function",
    "function": {
        "name": "get_missing_indexes",
        "description": "Get missing index recommendations from SQL Server DMVs. Can filter by a specific table or return all missing indexes for the database.",
        "parameters": {
            "type": "object",
            "properties": {
                "table_name": {"type": "string", "description": "Optional: filter by table name"},
                "schema": {"type": "string", "description": "Schema name (default: dbo)", "default": "dbo"},
            },
            "required": [],
        },
    },
}
