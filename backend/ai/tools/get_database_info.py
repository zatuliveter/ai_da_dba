from backend.db import execute_query


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


definition = {
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
}
