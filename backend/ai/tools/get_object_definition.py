from backend.mssql_db import execute_scalar


def get_object_definition(database: str, object_name: str, schema: str = "dbo") -> str:
    sql = """
        select sm.definition
        from sys.sql_modules sm
        where sm.object_id = object_id(QUOTENAME(?) + '.' + QUOTENAME(?))
    """
    return execute_scalar(database, sql, (schema, object_name))


definition = {
    "type": "function",
    "function": {
        "name": "get_object_definition",
        "description": "Get T-SQL definition text for an object (procedure, function, view, trigger, etc.) from sys.sql_modules.",
        "parameters": {
            "type": "object",
            "properties": {
                "object_name": {"type": "string", "description": "Object name"},
                "schema": {"type": "string", "description": "Schema name (default: dbo)", "default": "dbo"},
            },
            "required": ["object_name"],
        },
    },
}
