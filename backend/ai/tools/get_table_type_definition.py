import yaml

from backend.db import execute_query, execute_scalar

from ._columns_sql import COLUMNS_SQL


def get_table_type_definition(database: str, table_type_name: str, schema: str = "dbo") -> str:
    params = (schema, table_type_name)
    object_id = execute_scalar(
        database,
        """
        select type_table_object_id
        from sys.table_types tt
            join sys.schemas s on s.schema_id = tt.schema_id
        where s.name = ?
          and tt.name = ?
    """,
        params,
    )
    columns_yaml = execute_query(database, COLUMNS_SQL, (object_id,))
    columns = yaml.safe_load(columns_yaml) or []
    columns_cleared = [item["col"] for item in columns]
    return yaml.dump(columns_cleared, allow_unicode=True)


definition = {
    "type": "function",
    "function": {
        "name": "get_table_type_definition",
        "description": "Get the T-SQL definition of a specific table type (TVP).",
        "parameters": {
            "type": "object",
            "properties": {
                "table_type_name": {"type": "string", "description": "Table type name"},
                "schema": {"type": "string", "description": "Schema name (default: dbo)", "default": "dbo"},
            },
            "required": ["table_type_name"],
        },
    },
}
