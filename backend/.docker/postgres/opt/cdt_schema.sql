-- create enums
DO $$ BEGIN
    CREATE TYPE table_type AS ENUM (
        'calls',
        'traces',
        'dumps'
        );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;
DO $$ BEGIN
CREATE TYPE table_status AS ENUM (
    'creating',
    'ready',
    'persisting',
    'persisted',
    'to_delete'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;
DO $$ BEGIN
CREATE TYPE file_type AS ENUM (
    'calls',
    'traces',
    'dumps',
    'heap'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;
DO $$ BEGIN
CREATE TYPE file_status AS ENUM (
    'creating', -- created by collector
    'created', -- collector finished creating Parquet file on the local PV (it is ready to transfer to S3)
    'transferring', -- collector started sending file to S3
    'completed', -- collector finished creating Parquet file on the local PV (it is ready to transfer to S3)
    'to_delete' -- marked by k8 job before deleting permanently
    -- NOTE: after TTL k8 job will delete file (first step) and delete the row from inventory table (last step)
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- create short-term calls/traces -- see tables_template.sql
-- CREATE TABLE IF NOT EXISTS calls
-- CREATE TABLE IF NOT EXISTS traces
-- CREATE TABLE IF NOT EXISTS dumps

----------------------------------------------------------------------------------------------
-- create inventory tables

CREATE TABLE IF NOT EXISTS temp_table_inventory
(
    uuid             text,
    start_time       timestamptz, -- (start of the time range) time range for which it contains data 
    end_time         timestamptz, -- (end of the time range) time range for which it contains data 
    status           table_status,
    table_type       table_type,
    table_name       text,        -- full table name to use
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
    uuid                text,
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
    remote_storage_path text,
    local_file_path     text,
    PRIMARY KEY (uuid)
);
CREATE INDEX IF NOT EXISTS s3_types_idx ON s3_files (file_type, dump_type, status, start_time);
CREATE INDEX IF NOT EXISTS s3_deadline_idx ON s3_files (status, namespace, duration_range);

----------------------------------------------------------------------------------------------
-- create metadata tables

CREATE TABLE IF NOT EXISTS pods
(
    namespace    text,
    service_name text,
    pod_name     text,
    active_since timestamptz,
    last_restart timestamptz,
    last_active  timestamptz,
    tags         jsonb,
    pod_id       text, -- id (ns.service.pod) as FK for other tables
    PRIMARY KEY (namespace, service_name, pod_name)
);
CREATE INDEX IF NOT EXISTS pods_ns_idx ON pods (namespace);
CREATE INDEX IF NOT EXISTS pods_id_idx ON pods (pod_id);

CREATE TABLE IF NOT EXISTS pod_restarts
(
    pod_id       text,
    namespace    text,
    service_name text,
    pod_name     text,
    restart_time timestamptz,
    active_since timestamptz,
    last_active  timestamptz,
    PRIMARY KEY (namespace, service_name, pod_name, restart_time)
    -- PRIMARY KEY (pod_id)
);
CREATE INDEX IF NOT EXISTS pod_restarts_id_idx ON pod_restarts (pod_id); -- TODO create unique index, read about it 
CREATE INDEX IF NOT EXISTS pod_restarts_ns_idx ON pod_restarts (namespace, service_name);

-- TODO сделать index по pod_name + restart_time
CREATE TABLE IF NOT EXISTS dictionary
(
    pod_id       text,
    pod_name     text,
    restart_time timestamptz,
    position     integer,
    tag          text,
    PRIMARY KEY (pod_name, restart_time, position)
);

-- TODO сделать index по pod_name + restart_time
CREATE TABLE IF NOT EXISTS params
(
    pod_id       text,
    pod_name     text,
    restart_time timestamptz,
    param_name   text,
    param_index  boolean,
    param_list   boolean,
    param_order  integer,
    signature    text,
    PRIMARY KEY (pod_name, restart_time, param_name)
);

-- TODO проанализировать, надо ли здесь сделать партиционирование по дням
-- TODO сделать index по date и по pod_name + restart_time
CREATE TABLE IF NOT EXISTS pod_statistics
(
    date                 timestamptz,
    pod_id               text,
    pod_name             text,
    restart_time         timestamptz,
    cur_time             timestamptz, -- by minute
    data_accumulated     jsonb,
    original_accumulated jsonb,
    PRIMARY KEY (date, pod_name, restart_time, cur_time)
);

-- TODO сделать index по date и по pod_name + restart_time
CREATE TABLE IF NOT EXISTS suspend
(
    date         timestamptz,
    pod_id       text,
    pod_name     text,
    restart_time timestamptz,
    cur_time     timestamptz, -- by minute
    suspend_time jsonb,    -- ts (second+ms) -> count
    PRIMARY KEY (date, pod_name, restart_time, cur_time)
);