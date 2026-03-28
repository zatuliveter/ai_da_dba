import yaml

from .execute_read_query import definition as execute_read_query_def, execute_read_query
from .get_current_utc_time import definition as get_current_utc_time_def, get_current_utc_time
from .get_database_info import definition as get_database_info_def, get_database_info
from .get_execution_plan import definition as get_execution_plan_def, get_execution_plan
from .get_foreign_keys import definition as get_foreign_keys_def, get_foreign_keys
from .get_indexes import definition as get_indexes_def, get_indexes
from .get_missing_indexes import definition as get_missing_indexes_def, get_missing_indexes
from .get_object_definition import definition as get_object_definition_def, get_object_definition
from .get_table_structure import definition as get_table_structure_def, get_table_structure
from .get_table_type_definition import definition as get_table_type_definition_def, get_table_type_definition
from .list_sql_modules import definition as list_sql_modules_def, list_sql_modules
from .list_tables import definition as list_tables_def, list_tables

TOOL_DEFINITIONS = [
    get_current_utc_time_def,
    get_database_info_def,
    list_tables_def,
    get_table_structure_def,
    get_table_type_definition_def,
    get_indexes_def,
    get_execution_plan_def,
    get_missing_indexes_def,
    get_foreign_keys_def,
    get_object_definition_def,
    list_sql_modules_def,
    execute_read_query_def,
]


def dispatch_tool(name: str, args: dict, database: str) -> str:
    """Route a tool call to the appropriate function."""
    handlers = {
        "get_current_utc_time": lambda a: get_current_utc_time(),
        "get_database_info": lambda a: get_database_info(database),
        "list_tables": lambda a: list_tables(database),
        "get_table_structure": lambda a: get_table_structure(database, a["table_name"], a.get("schema", "dbo")),
        "get_table_type_definition": lambda a: get_table_type_definition(database, a["table_type_name"], a.get("schema", "dbo")),
        "get_indexes": lambda a: get_indexes(database, a["table_name"], a.get("schema", "dbo")),
        "get_execution_plan": lambda a: get_execution_plan(database, a["query"]),
        "get_missing_indexes": lambda a: get_missing_indexes(database, a.get("table_name"), a.get("schema", "dbo")),
        "get_foreign_keys": lambda a: get_foreign_keys(database, a["table_name"], a.get("schema", "dbo")),
        "get_object_definition": lambda a: get_object_definition(database, a["object_name"], a.get("schema", "dbo")),
        "list_sql_modules": lambda a: list_sql_modules(database, a["object_type"]),
        "execute_read_query": lambda a: execute_read_query(database, a["query"]),
    }

    handler = handlers.get(name)
    if not handler:
        return yaml.dump({"error": f"Unknown tool: {name}"}, allow_unicode=True)

    try:
        return handler(args)
    except Exception as e:
        return yaml.dump({"error": str(e)}, allow_unicode=True)
