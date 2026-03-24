-- create inventory tables
CREATE TABLE IF NOT EXISTS temp_table_inventory
(
    uuid             uuid,
    start_time       timestamptz, -- (start of the time range) time range for which it contains data 
    end_time         timestamptz, -- (end of the time range) time range for which it contains data 
    status           table_status,
    table_type       table_type,
    table_name       text UNIQUE, -- full table name to use
    -- tech information
    created_time     timestamptz,
    rows_count       integer,     --                                             -- updated by collector
    table_size       bigint,      -- select pg_relation_size('tablename')        -- updated by collector
    table_total_size bigint,      -- select pg_total_relation_size('tablename')  -- updated by collector
    PRIMARY KEY (uuid)
);
CREATE INDEX IF NOT EXISTS temps_types_idx ON temp_table_inventory (table_type, status, start_time); -- for collector
CREATE INDEX IF NOT EXISTS temps_time_idx ON temp_table_inventory (status, start_time); -- for cleanup

CREATE TABLE IF NOT EXISTS s3_files
(
    uuid                uuid,
    start_time          timestamptz, -- (start of the time range) time range for which it contains data 
    end_time            timestamptz, -- (end of the time range) time range for which it contains data 
    file_type           file_type,
    dump_type           text,        -- only for dumps (java: top, td, ... | go: cpu, allocs, ...)
    namespace           text,
    duration_range      integer,
    -- state
    file_name           text,
    status              file_status,
    services            jsonb,       -- (additional info for future filtering) list of services
    -- tech information
    created_time        timestamptz,
    api_version         integer,
    rows_count          integer,
    file_size           bigint,
    remote_storage_path text UNIQUE,
    local_file_path     text,
    PRIMARY KEY (uuid)
);
CREATE INDEX IF NOT EXISTS s3_types_idx ON s3_files (file_type, dump_type, status, start_time);
CREATE INDEX IF NOT EXISTS s3_deadline_idx ON s3_files (status, namespace, duration_range);