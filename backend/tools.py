import json
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from db import MAX_ROWS, execute_query, get_connection, rows_to_json

# ---------------------------------------------------------------------------
# Tool implementations
# ---------------------------------------------------------------------------

def get_current_utc_time() -> str:
    return datetime.now(timezone.utc).isoformat()

def get_database_info(database: str) -> str:
    sql = """
        with db_sizes
        as (
            select
                  sum(case when type = 0 then size * 8 / 1024 else 0 end) as data_allocated_mb
                , sum(case when type = 1 then size * 8 / 1024 else 0 end) as log_allocated_mb
                , sum(case when type = 0 then fileproperty(name, 'SpaceUsed') * 8 / 1024 else 0 end) as data_used_mb
                , sum(case when type = 1 then fileproperty(name, 'SpaceUsed') * 8 / 1024 else 0 end) as log_used_mb
            from sys.database_files
        )
        select
              d.name as db_name
            , d.state_desc as db_state
            , s.data_allocated_mb
            , s.log_allocated_mb
            , s.data_used_mb
            , s.log_used_mb
            , d.recovery_model_desc as recovery_model
            , d.is_query_store_on as query_store_enabled
            , d.is_read_committed_snapshot_on as rcsi_enabled
            , d.snapshot_isolation_state_desc as snapshot_isolation
            , d.compatibility_level as compat_level
            , d.page_verify_option_desc as page_verify
            , d.is_auto_close_on as auto_close
            , d.is_auto_shrink_on as auto_shrink
            , d.user_access_desc as user_access
            , d.target_recovery_time_in_seconds as indirect_checkpoint_sec

            -- basic options and stats
            , is_read_only
            , is_auto_create_stats_on
            , is_auto_update_stats_on
            , is_auto_update_stats_async_on
            -- security and encryption
            , is_encrypted
            , is_trustworthy_on
            , is_db_chaining_on
            -- broker and cdc
            , is_broker_enabled
            , is_cdc_enabled
            -- replication
            , is_published
            , is_subscribed
            , is_merge_published
            -- ansi and behavior options
            , is_ansi_null_default_on
            , is_ansi_nulls_on
            , is_ansi_padding_on
            , is_ansi_warnings_on
            , is_arithabort_on
            , is_concat_null_yields_null_on
            , is_quoted_identifier_on
            , is_numeric_roundabort_on
            -- triggers, cursors, and advanced
            , is_recursive_triggers_on
            , is_cursor_close_on_commit_on
            , is_local_cursor_default
            , is_fulltext_enabled
            , delayed_durability_desc
        from sys.databases d
            cross join db_sizes s
        where d.database_id = db_id()
    """
    return execute_query(database, sql)

def list_tables(database: str) -> str:
    sql = """
        select
            tt.table_schema
          , tt.table_name
          , tt.table_type
          , stat.row_count
          , stat.data_size_mb
          , stat.indexes_size_mb
        from INFORMATION_SCHEMA.TABLES tt
            outer apply (
                select
                    sum(iif(ps.index_id in (0, 1), ps.row_count, 0)) as row_count
                , sum(iif(ps.index_id in (0, 1), ps.used_page_count, 0) * 8 / 1024) as data_size_mb
                , sum(iif(ps.index_id > 1, ps.used_page_count, 0) * 8 / 1024) as indexes_size_mb
                from sys.tables t
                    join sys.schemas s on t.schema_id = s.schema_id
                    join sys.dm_db_partition_stats ps on t.object_id = ps.object_id
                where ps.index_id in (0, 1)
                and t.name = tt.table_name
                and s.name = tt.table_schema
            ) stat
        order by table_type, table_schema, table_name
    """
    return execute_query(database, sql)


def get_table_structure(database: str, table_name: str, schema: str = "dbo") -> str:
    columns_sql = """
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
    stats_sql = """
        with o ( object_id )
        as (
            select object_id(quotename(?) + '.' + quotename(?))
        )
        select
            p.row_count
          , p.data_size_mb
          , idx.index_count
          , p.indexes_size_mb
		  , ds.data_space
		  , ds.data_space_type
        from o
            outer apply ( 
				select
					  sum(case when ps.index_id in (0, 1) then ps.row_count else 0 end) as row_count
					, sum(case when ps.index_id in (0, 1) then ps.used_page_count * 8 / 1024.0
							   else 0
						  end) as data_size_mb
					, sum(case when ps.index_id > 1 then ps.used_page_count * 8 / 1024.0 else 0 end) as indexes_size_mb
				from sys.dm_db_partition_stats ps
				where ps.object_id = o.object_id 
			) p
            outer apply ( 
				select count(*) as index_count
                from sys.indexes i
                where i.object_id = o.object_id
                    and i.name is not null 
			) idx
			outer apply (
				select
                    case when ds.type_desc = 'ROWS_FILEGROUP' then ds.name
                         else concat(ds.name, '(', c.name, ')')
                    end as data_space
				  , ds.type_desc as data_space_type
				from sys.tables t
					inner join sys.indexes i on t.object_id = i.object_id
					inner join sys.data_spaces ds on i.data_space_id = ds.data_space_id
					left join sys.index_columns ic on i.object_id = ic.object_id
						and i.index_id = ic.index_id
						and ic.partition_ordinal > 0
					left join sys.columns c on t.object_id = c.object_id
						and ic.column_id = c.column_id
				where t.object_id = o.object_id
				  and i.index_id in (0, 1)
			) ds
    """
    params = (schema, table_name)
    columns_json = execute_query(database, columns_sql, params)
    stats_json = execute_query(database, stats_sql, params)
    columns = json.loads(columns_json)
    stats_list = json.loads(stats_json)
    stats_row = stats_list[0] if stats_list else {}
    combined = {
        "columns": columns,
        "row_count": stats_row.get("row_count"),
        "data_size_mb": stats_row.get("data_size_mb"),
        "index_count": stats_row.get("index_count"),
        "indexes_size_mb": stats_row.get("indexes_size_mb"),
        "data_space": stats_row.get("data_space"),
        "data_space_type": stats_row.get("data_space_type")
    }
    return json.dumps(combined, default=str, ensure_ascii=False)


def get_indexes(database: str, table_name: str, schema: str = "dbo") -> str:
    sql = """
        select
          i.name as index_name
        , i.type_desc as index_type
        , i.is_unique
        , i.is_primary_key
        , cols.key_columns
        , cols.included_columns
        , i.filter_definition
        , stat.row_count
        , stat.size_mb
		, frag.avg_fragmentation_percent
		, stu.last_stats_update_days_ago
		, ds.data_space
		, ds.data_space_type
        from sys.indexes i
            join sys.tables t on i.object_id = t.object_id
            join sys.schemas s on t.schema_id = s.schema_id
            outer apply ( select
                            string_agg(case when ic.is_included_column = 0 then c.name end, ', ')within group(order by ic.key_ordinal) as key_columns
                            , string_agg(case when ic.is_included_column = 1 then c.name end, ', ')within group(order by ic.key_ordinal) as included_columns
                        from sys.index_columns ic
                            join sys.columns c on ic.object_id = c.object_id
                                                and ic.column_id = c.column_id
                        where i.object_id = ic.object_id
                            and i.index_id = ic.index_id ) cols
            outer apply ( select
                            sum(ps.row_count) as row_count
                            , cast(sum(ps.used_page_count * 8 / 1024.0) as decimal(32, 3)) as size_mb
                        from sys.dm_db_partition_stats ps
                        where ps.object_id = t.object_id
                            and ps.index_id = i.index_id ) stat
			outer apply ( select cast(avg(ips.avg_fragmentation_in_percent) as decimal(18, 1)) as avg_fragmentation_percent
						  from sys.dm_db_index_physical_stats(db_id(), t.object_id, null, null, 'LIMITED') ips
						  where ips.index_id > 0 ) frag
			outer apply ( select convert(decimal(18, 1), datediff(hour, max(sp.last_updated), sysutcdatetime()) / 24.0) as last_stats_update_days_ago
						  from sys.stats st
							  cross apply sys.dm_db_stats_properties(st.object_id, st.stats_id) sp
						  where st.object_id = t.object_id 
						    and st.stats_id = i.index_id ) stu
            			outer apply (
				select
                    case when ds.type_desc = 'ROWS_FILEGROUP' then ds.name
                         else concat(ds.name, '(', c.name, ')')
                    end as data_space
				  , ds.type_desc as data_space_type
				from sys.data_spaces ds 
					left join sys.index_columns ic on i.object_id = ic.object_id
						and i.index_id = ic.index_id
						and ic.partition_ordinal > 0
					left join sys.columns c on t.object_id = c.object_id
						and ic.column_id = c.column_id
				where i.data_space_id = ds.data_space_id
			) ds
        where i.name is not null
          and s.name = ? 
          and t.name = ?
        order by i.is_primary_key desc
               , i.name;
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
            "name": "get_current_utc_time",
            "description": "Get the current UTC time.",
            "parameters": {},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_database_info",
            "description": "Get information about the current database.",
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
            "description": "Get the column definitions of a table (column names, data types, nullability, defaults, primary key) and summary stats for optimization: row count, data size (MB), index count, total index size (MB), data space name, data space type.",
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
            "description": "Get all indexes on a table: index name, type (clustered/nonclustered), uniqueness, key columns, and included columns, rows count, size (MB), fragmentation, last stats update days ago, data space name, data space type.",
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
            "description": f"Execute a read-only SELECT query against the database. Returns up to {MAX_ROWS} rows. Only SELECT/WITH statements are allowed.",
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
        "get_current_utc_time": lambda a: get_current_utc_time(),
        "get_database_info": lambda a: get_database_info(database),
        "list_tables": lambda a: list_tables(database),
        "get_table_structure": lambda a: get_table_structure(database, a["table_name"], a.get("schema", "dbo")),
        "get_indexes": lambda a: get_indexes(database, a["table_name"], a.get("schema", "dbo")),
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
