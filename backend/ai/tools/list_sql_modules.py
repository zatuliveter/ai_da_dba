import yaml

from backend.mssql_db import execute_query


def list_sql_modules(connection_id: int, database: str, object_type: str) -> str:
    sql = """
        select concat(s.name, '.', o.name) as name
        from sys.sql_modules sm
            join sys.objects o on sm.object_id = o.object_id
            join sys.schemas s on o.schema_id = s.schema_id
        where o.type = UPPER(?)
           or o.type_desc = UPPER(?)
        order by o.type, s.name        
    """
    object_type_normalized = object_type.strip()
    module_names = execute_query(
        connection_id, database, sql, (object_type_normalized, object_type_normalized)
    )
    names = yaml.safe_load(module_names) or []
    names_cleared = [item["name"] for item in names]
    return yaml.dump(names_cleared, allow_unicode=True)


definition = {
    "type": "function",
    "function": {
        "name": "list_sql_modules",
        "description": "List SQL modules from sys.sql_modules filtered by object type (for example: P, V, FN, IF, TF, TR or SQL_STORED_PROCEDURE).",
        "parameters": {
            "type": "object",
            "properties": {
                "object_type": {"type": "string", "description": "Object type code (P, V, FN, IF, TF, TR) or type_desc value"},
            },
            "required": ["object_type"],
        },
    },
}
