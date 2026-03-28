from backend.db import execute_query


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
		, case when i.fill_factor = 0 then 100 else i.fill_factor end as fill_factor
        , stat.row_count
        , stat.size_mb
		, frag.avg_fragmentation_percent
		, stu.last_stats_update_days_ago
		, ds.data_space
		, ds.data_space_type
		, compr.compressions as compressions_per_partition
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
		cross apply ( 
			select case when max(partitions_count) = 1 then lower(min(data_compression_desc))
						else string_agg(
							   case 
								   when min_p = max_p 
									   then concat(lower(data_compression_desc), ' ', min_p)
								   else concat(lower(data_compression_desc), ' ', min_p, '-', max_p)
							   end
						   , ', ')
					end as compressions
			from (
				select 
					  data_compression_desc
					, min(partition_number) as min_p
					, max(partition_number) as max_p
					, max(partitions_count) as partitions_count
				from (
					select 
						  partition_number
						, data_compression_desc
						, sum(change_flag) over (order by partition_number) as grp
						, sum(1) over() as partitions_count
					from (
						select 
							  p.partition_number
							, p.data_compression_desc
							, case 
								  when p.data_compression_desc 
									   <> lag(p.data_compression_desc) over (order by p.partition_number)
								  then 1 
								  else 0 
							  end as change_flag
						from sys.partitions p
						where p.index_id = i.index_id
						  and p.object_id = i.object_id
					) t1
				) t2
				group by grp, data_compression_desc
			) t3
		) compr
        where i.name is not null
          and s.name = ? 
          and t.name = ?
        order by i.is_primary_key desc
               , i.name;
    """
    return execute_query(database, sql, (schema, table_name))


definition = {
    "type": "function",
    "function": {
        "name": "get_indexes",
        "description": "Get all indexes on a table: index name, type (clustered/nonclustered), uniqueness, key columns, and included columns, fill factor,rows count, size (MB), fragmentation, last stats update days ago, data space name, data space type, compression per partition.",
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
