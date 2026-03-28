from backend.mssql_db import execute_query


def list_tables(database: str) -> str:
    sql = """
        select
            concat(tt.table_schema, '.', tt.table_name) as table_name
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
        order by table_type, table_name
    """
    return execute_query(database, sql)


definition = {
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
}
