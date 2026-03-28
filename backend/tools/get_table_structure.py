import yaml

from db import execute_query, execute_scalar

from ._columns_sql import COLUMNS_SQL


def get_table_structure(database: str, table_name: str, schema: str = "dbo") -> str:

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
    object_id = execute_scalar(database, "select object_id(quotename(?) + '.' + quotename(?))", params)
    columns_yaml = execute_query(database, COLUMNS_SQL, (object_id,))
    stats_yaml = execute_query(database, stats_sql, params)
    columns = yaml.safe_load(columns_yaml) or []
    columns_cleared = [item["col"] for item in columns]
    stats_list = yaml.safe_load(stats_yaml) or []
    stats_row = stats_list[0] if stats_list else {}
    combined = {
        "columns": columns_cleared,
        "row_count": stats_row.get("row_count"),
        "data_size_mb": stats_row.get("data_size_mb"),
        "index_count": stats_row.get("index_count"),
        "indexes_size_mb": stats_row.get("indexes_size_mb"),
        "data_space": stats_row.get("data_space"),
        "data_space_type": stats_row.get("data_space_type"),
    }
    return yaml.dump(combined, allow_unicode=True)


definition = {
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
}
