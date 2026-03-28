COLUMNS_SQL = """
    select
        c.name 
        + ' '
        + case when ct.SchemaName = 'sys' 
            then ct.Name + ct.Suffix 
            else quotename(ct.SchemaName) + '.' + quotename(ct.Name) + ct.Suffix 
        end 
        + ct.Collation
        + isnull(' identity(' + cast(ic.seed_value as varchar(64)) + ',' + cast(ic.increment_value as varchar(64)) + ')', '') as [col]
    from sys.columns c
        left join sys.identity_columns ic on ic.object_id = c.object_id and ic.column_id = c.column_id
        join sys.databases db on db.name = db_name()
        outer apply (
            select schema_name(schema_id) as SchemaName
                , case when name='timestamp' then 'rowversion' else name end as Name
                , case when max_length = -1 then ''
                        when c.max_length = -1 then '(max)'
                        when name like 'n%char' then '(' + cast(c.max_length / 2 as nvarchar) + ')'
                        when name like '%char' or name like '%binary' then '(' + cast(c.max_length as nvarchar) + ')'
                        when name in ('datetime2', 'time', 'datetimeoffset') then '(' + cast(c.scale as nvarchar) + ')'
                        when name in ('decimal', 'numeric') then '(' + cast(c.precision as nvarchar) + ',' + cast(c.scale as nvarchar) + ')'
                        else ''
                end Suffix
                , case when c.collation_name is null or c.collation_name = db.collation_name
                        then '' 
                        else ' collate ' + c.collation_name 
                end Collation
                , case 
                    when name = 'image' then convert(sql_variant, 0x00)
                    when name = 'text' then convert(sql_variant, '')
                    when name = 'uniqueidentifier' then convert(sql_variant, '00000000-0000-0000-0000-000000000000')
                    when name = 'date' then convert(sql_variant, '0001-01-01')
                    when name = 'time' then convert(sql_variant, '00:00:00')
                    when name = 'datetime2' then convert(sql_variant, '0001-01-01 00:00:00')
                    when name = 'datetimeoffset' then convert(sql_variant, '0001-01-01 00:00:00 +00:00')
                    when name = 'tinyint' then convert(sql_variant, 0)
                    when name = 'smallint' then convert(sql_variant, -32768)
                    when name = 'int' then convert(sql_variant, -2147483648)
                    when name = 'smalldatetime' then convert(sql_variant, '1900-01-01 00:00:00')
                    when name = 'real' then convert(sql_variant, -3.4028235E+38)
                    when name = 'money' then convert(sql_variant, -922337203685477.5808)
                    when name = 'datetime' then convert(sql_variant, '1753-01-01 00:00:00')
                    when name = 'float' then convert(sql_variant, -1.79E+308)
                    when name = 'sql_variant' then convert(sql_variant, 0)
                    when name = 'ntext' then convert(sql_variant, N'')
                    when name = 'bit' then convert(sql_variant, 0)
                    when name = 'decimal' then convert(sql_variant, convert(decimal, -999999999999999999))
                    when name = 'numeric' then convert(sql_variant, convert(numeric, -999999999999999999))
                    when name = 'smallmoney' then convert(sql_variant, -214748.3648)
                    when name = 'bigint' then convert(sql_variant, -9223372036854775808)
                    when name = 'hierarchyid' then convert(sql_variant, 0x)
                    --when name = 'geometry' then convert(sql_variant, geometry::STGeomFromText('POINT EMPTY', 0))
                    --when name = 'geography' then convert(sql_variant, geography::STGeomFromText('POINT EMPTY', 4326))
                    when name = 'varbinary' then convert(sql_variant, 0x00)
                    when name = 'varchar' then convert(sql_variant, '')
                    when name = 'binary' then convert(sql_variant, 0x00)
                    when name = 'char' then convert(sql_variant, ' ')
                    when name = 'timestamp' then convert(sql_variant, 0x00)
                    when name = 'nvarchar' then convert(sql_variant, N'')
                    when name = 'nchar' then convert(sql_variant, N' ')
                    --when name = 'xml' then convert(sql_variant, convert(xml, ''))
                    when name = 'sysname' then convert(sql_variant, N'')
                    else convert(bit, concat('Unsupported data type ', name, '.'))
                end as MinValue
            from sys.types t
            where user_type_id = c.user_type_id
        ) ct
    where c.object_id = ?
    order by c.column_id
"""
