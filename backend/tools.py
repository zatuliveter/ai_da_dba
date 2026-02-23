import json
import re
import xml.etree.ElementTree as ET

from db import execute_query, get_connection, rows_to_json

# ---------------------------------------------------------------------------
# Tool implementations
# ---------------------------------------------------------------------------

def list_tables(database: str) -> str:
    sql = """
        SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
        FROM INFORMATION_SCHEMA.TABLES
        ORDER BY TABLE_TYPE, TABLE_NAME
    """
    return execute_query(database, sql)


def get_table_structure(database: str, table_name: str, schema: str = "dbo") -> str:
    sql = """
        SELECT
            c.COLUMN_NAME,
            c.DATA_TYPE,
            c.CHARACTER_MAXIMUM_LENGTH,
            c.NUMERIC_PRECISION,
            c.NUMERIC_SCALE,
            c.IS_NULLABLE,
            c.COLUMN_DEFAULT,
            CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 'YES' ELSE 'NO' END AS IS_PRIMARY_KEY
        FROM INFORMATION_SCHEMA.COLUMNS c
        LEFT JOIN (
            SELECT ku.TABLE_SCHEMA, ku.TABLE_NAME, ku.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
                ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
                AND tc.TABLE_SCHEMA = ku.TABLE_SCHEMA
            WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
        ) pk ON c.TABLE_SCHEMA = pk.TABLE_SCHEMA
            AND c.TABLE_NAME = pk.TABLE_NAME
            AND c.COLUMN_NAME = pk.COLUMN_NAME
        WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
        ORDER BY c.ORDINAL_POSITION
    """
    return execute_query(database, sql, (schema, table_name))


def get_indexes(database: str, table_name: str, schema: str = "dbo") -> str:
    sql = """
        SELECT
            i.name AS index_name,
            i.type_desc AS index_type,
            i.is_unique,
            i.is_primary_key,
            STRING_AGG(
                CASE WHEN ic.is_included_column = 0 THEN c.name END, ', '
            ) WITHIN GROUP (ORDER BY ic.key_ordinal) AS key_columns,
            STRING_AGG(
                CASE WHEN ic.is_included_column = 1 THEN c.name END, ', '
            ) WITHIN GROUP (ORDER BY ic.key_ordinal) AS included_columns,
			i.filter_definition
        FROM sys.indexes i
        JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        JOIN sys.tables t ON i.object_id = t.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = ? AND t.name = ?
          AND i.name IS NOT NULL
        GROUP BY i.name, i.type_desc, i.is_unique, i.is_primary_key, i.filter_definition
        ORDER BY i.is_primary_key DESC, i.name
    """
    return execute_query(database, sql, (schema, table_name))


def get_table_stats(database: str, table_name: str, schema: str = "dbo") -> str:
    sql = """
        SELECT
            s.name AS schema_name,
            t.name AS table_name,
            SUM(ps.row_count) AS row_count,
            SUM(ps.reserved_page_count) * 8 / 1024 AS reserved_mb,
            SUM(ps.used_page_count) * 8 / 1024 AS used_mb
        FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        JOIN sys.dm_db_partition_stats ps ON t.object_id = ps.object_id
        WHERE s.name = ? AND t.name = ? AND ps.index_id IN (0, 1)
        GROUP BY s.name, t.name
    """
    return execute_query(database, sql, (schema, table_name))


def get_execution_plan(database: str, query: str) -> str:
    """Get estimated execution plan and return a text summary."""
    with get_connection(database) as conn:
        cursor = conn.cursor()
        cursor.execute("SET SHOWPLAN_XML ON")
        cursor.execute(query)
        row = cursor.fetchone()
        cursor.execute("SET SHOWPLAN_XML OFF")

    if not row:
        return json.dumps({"error": "No execution plan returned"})

    xml_plan = row[0]
    return _parse_execution_plan(xml_plan)


def _parse_execution_plan(xml_plan: str) -> str:
    """Parse SHOWPLAN_XML into a readable summary."""
    ns = {"sp": "http://schemas.microsoft.com/sqlserver/2004/07/showplan"}
    try:
        root = ET.fromstring(xml_plan)
    except ET.ParseError:
        return json.dumps({"raw_plan": xml_plan[:4000]})

    statements = []
    for stmt in root.findall(".//sp:StmtSimple", ns):
        stmt_text = stmt.get("StatementText", "")
        est_rows = stmt.get("StatementEstRows", "")
        est_cost = stmt.get("StatementSubTreeCost", "")

        operators = []
        for rel_op in stmt.findall(".//sp:RelOp", ns):
            op_info = {
                "operation": rel_op.get("PhysicalOp", ""),
                "logical_op": rel_op.get("LogicalOp", ""),
                "est_rows": rel_op.get("EstimateRows", ""),
                "est_cost": rel_op.get("EstimatedTotalSubtreeCost", ""),
                "est_cpu": rel_op.get("EstimateCPU", ""),
                "est_io": rel_op.get("EstimateIO", ""),
            }
            # Capture object references (table/index scans)
            for obj in rel_op.findall(".//sp:Object", ns):
                op_info["table"] = obj.get("Table", "").strip("[]")
                op_info["index"] = obj.get("Index", "").strip("[]")
                op_info["schema"] = obj.get("Schema", "").strip("[]")

            # Capture warnings
            for warn in rel_op.findall(".//sp:Warnings", ns):
                warnings = []
                for child in warn:
                    tag = child.tag.replace(f"{{{ns['sp']}}}", "")
                    warnings.append(tag)
                if warnings:
                    op_info["warnings"] = warnings

            operators.append(op_info)

        statements.append({
            "statement": stmt_text.strip()[:200],
            "estimated_rows": est_rows,
            "estimated_cost": est_cost,
            "operators": operators,
        })

    # Capture missing index hints
    missing_indexes = []
    for mg in root.findall(".//sp:MissingIndexGroup", ns):
        impact = mg.get("Impact", "")
        for mi in mg.findall(".//sp:MissingIndex", ns):
            table = mi.get("Table", "").strip("[]")
            schema = mi.get("Schema", "").strip("[]")
            eq_cols = [
                c.get("Name", "").strip("[]")
                for cg in mi.findall("sp:ColumnGroup[@Usage='EQUALITY']", ns)
                for c in cg.findall("sp:Column", ns)
            ]
            ineq_cols = [
                c.get("Name", "").strip("[]")
                for cg in mi.findall("sp:ColumnGroup[@Usage='INEQUALITY']", ns)
                for c in cg.findall("sp:Column", ns)
            ]
            incl_cols = [
                c.get("Name", "").strip("[]")
                for cg in mi.findall("sp:ColumnGroup[@Usage='INCLUDE']", ns)
                for c in cg.findall("sp:Column", ns)
            ]
            missing_indexes.append({
                "table": f"{schema}.{table}",
                "impact": impact,
                "equality_columns": eq_cols or None,
                "inequality_columns": ineq_cols or None,
                "include_columns": incl_cols or None,
            })

    result = {"statements": statements}
    if missing_indexes:
        result["missing_indexes"] = missing_indexes

    return json.dumps(result, default=str, ensure_ascii=False)


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


def execute_read_query(database: str, query: str) -> str:
    normalized = re.sub(r"--[^\n]*", "", query)
    normalized = re.sub(r"/\*.*?\*/", "", normalized, flags=re.DOTALL)
    normalized = normalized.strip().upper()

    if not normalized.startswith("SELECT") and not normalized.startswith("WITH"):
        return json.dumps({"error": "Only SELECT queries are allowed"})

    forbidden = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE",
                 "TRUNCATE", "EXEC", "EXECUTE", "MERGE", "GRANT", "REVOKE"]
    tokens = re.findall(r'\b[A-Z]+\b', normalized)
    for token in tokens:
        if token in forbidden:
            return json.dumps({"error": f"Forbidden keyword: {token}"})

    return execute_query(database, query)


# ---------------------------------------------------------------------------
# Tool definitions for OpenAI function calling
# ---------------------------------------------------------------------------

TOOL_DEFINITIONS = [
    {
        "type": "function",
        "function": {
            "name": "list_tables",
            "description": "List all tables and views in the selected database for a given schema.",
            "parameters": {
                "type": "object",
                "properties": {},
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_table_structure",
            "description": "Get the column definitions of a table: column names, data types, nullability, defaults, and primary key info.",
            "parameters": {
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Table name"},
                    "schema": {"type": "string", "description": "Schema name (default: dbo)", "default": "dbo"},
                },
                "required": ["table_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_indexes",
            "description": "Get all indexes on a table: index name, type (clustered/nonclustered), uniqueness, key columns, and included columns.",
            "parameters": {
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Table name"},
                    "schema": {"type": "string", "description": "Schema name (default: dbo)", "default": "dbo"},
                },
                "required": ["table_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_table_stats",
            "description": "Get table statistics: row count, reserved space (MB), used space (MB).",
            "parameters": {
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Table name"},
                    "schema": {"type": "string", "description": "Schema name (default: dbo)", "default": "dbo"},
                },
                "required": ["table_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_execution_plan",
            "description": "Get the estimated execution plan for a SQL query. Returns operators, costs, row estimates, and missing index hints. Use this to analyze query performance.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "The SQL query to analyze"},
                },
                "required": ["query"],
            },
        },
    },
    {
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
    },
    {
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
    },
    {
        "type": "function",
        "function": {
            "name": "execute_read_query",
            "description": "Execute a read-only SELECT query against the database. Returns up to 50 rows. Only SELECT/WITH statements are allowed.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "The SELECT query to execute"},
                },
                "required": ["query"],
            },
        },
    },
]


def dispatch_tool(name: str, args: dict, database: str) -> str:
    """Route a tool call to the appropriate function."""
    handlers = {
        "list_tables": lambda a: list_tables(database),
        "get_table_structure": lambda a: get_table_structure(database, a["table_name"], a.get("schema", "dbo")),
        "get_indexes": lambda a: get_indexes(database, a["table_name"], a.get("schema", "dbo")),
        "get_table_stats": lambda a: get_table_stats(database, a["table_name"], a.get("schema", "dbo")),
        "get_execution_plan": lambda a: get_execution_plan(database, a["query"]),
        "get_missing_indexes": lambda a: get_missing_indexes(database, a.get("table_name"), a.get("schema", "dbo")),
        "get_foreign_keys": lambda a: get_foreign_keys(database, a["table_name"], a.get("schema", "dbo")),
        "execute_read_query": lambda a: execute_read_query(database, a["query"]),
    }

    handler = handlers.get(name)
    if not handler:
        return json.dumps({"error": f"Unknown tool: {name}"})

    try:
        return handler(args)
    except Exception as e:
        return json.dumps({"error": str(e)})
