
-- create calls/traces and inventory tables
-- partitions created for each 5min

CREATE TABLE IF NOT EXISTS calls_%TIMESTAMP% (
    time timestamptz, -- time when func/method was called
    cpu_time bigint,
    wait_time bigint,
    memory_used bigint,
    duration bigint,
    non_blocking bigint,
    queue_wait_duration integer,
    suspend_duration integer,
    calls integer,
    transactions bigint,
    logs_generated integer,
    logs_written integer,
    file_read bigint,
    file_written bigint,
    net_read bigint,
    net_written bigint,
    namespace text,
    service_name text,
    pod_name text,
    restart_time timestamptz,
    method integer,
    params jsonb,
    trace_file_index integer,
    buffer_offset integer,
    record_index integer
);
CREATE INDEX IF NOT EXISTS calls_%TIMESTAMP%_idx ON calls_%TIMESTAMP% (namespace, service_name, pod_name, restart_time);
CREATE INDEX IF NOT EXISTS calls_%TIMESTAMP%_time_idx ON calls_%TIMESTAMP% (namespace, service_name, pod_name, restart_time, time);

CREATE TABLE IF NOT EXISTS traces_%TIMESTAMP% (
    pod_name text,
    restart_time timestamptz,
    trace_file_index integer,
    buffer_offset integer,
    record_index integer,
    trace bytea,
    PRIMARY KEY (pod_name, restart_time, trace_file_index, buffer_offset, record_index)
);
-- CREATE INDEX traces_%TIMESTAMP%_idx ON traces_%TIMESTAMP% (pod, restart_time, trace_file_index, buffer_offset, record_index);

CREATE TABLE IF NOT EXISTS dumps_%TIMESTAMP% (
    uuid text,
    created_time timestamptz,
    namespace text,
    service_name text,
    pod_name text,
    pod_type text,
    restart_time timestamptz,
    dump_type text,
    bytes_size bigint,
    info jsonb,
    binary_data bytea,
    PRIMARY KEY (uuid)
);

-- for testing inverted index
-- CREATE TABLE IF NOT EXISTS profiler_plugin_exception_1710201600 (
--     value text,
--     file_id text,
--     PRIMARY KEY (value, file_id)
-- );
