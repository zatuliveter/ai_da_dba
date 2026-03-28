from backend.mssql_db import execute_query


def get_foreign_keys(database: str, table_name: str, schema: str = "dbo") -> str:
    sql = """
        SELECT
            fk.name AS fk_name,
            tp.name AS parent_table,
            sp.name AS parent_schema,
            cp.name AS parent_column,
            tr.name AS referenced_table,
            sr.name AS referenced_schema,
            cr.name AS referenced_column,
            fk.delete_referential_action_desc AS on_delete,
            fk.update_referential_action_desc AS on_update
        FROM sys.foreign_keys fk
        JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
        JOIN sys.tables tp ON fkc.parent_object_id = tp.object_id
        JOIN sys.schemas sp ON tp.schema_id = sp.schema_id
        JOIN sys.columns cp ON fkc.parent_object_id = cp.object_id AND fkc.parent_column_id = cp.column_id
        JOIN sys.tables tr ON fkc.referenced_object_id = tr.object_id
        JOIN sys.schemas sr ON tr.schema_id = sr.schema_id
        JOIN sys.columns cr ON fkc.referenced_object_id = cr.object_id AND fkc.referenced_column_id = cr.column_id
        WHERE (sp.name = ? AND tp.name = ?)
           OR (sr.name = ? AND tr.name = ?)
        ORDER BY fk.name
    """
    return execute_query(database, sql, (schema, table_name, schema, table_name))


definition = {
    "type": "function",
    "function": {
        "name": "get_foreign_keys",
        "description": "Get all foreign key relationships for a table (both as parent and referenced table).",
        "parameters": {
            "type": "object",
            "properties": {
                "table_name": {"type": "string", "description": "Table name"},
                "schema": {"type": "string", "description": "Schema name (default: dbo)", "default": "dbo"},
            },
            "required": ["table_name"],
        },
    },
}
