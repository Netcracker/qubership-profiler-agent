-- ====================================================================================
-- Schema Initialization for Dump Management System
-- This script defines custom types, tables, and functions used for managing
-- heap, td, and top dumps associated with Kubernetes pods. It includes logic
-- for transactional inserts.
-- ====================================================================================


-- ====================================================================================
-- ENUM Type Definitions
-- ====================================================================================
DO $$ BEGIN
    CREATE TYPE timeline_status AS ENUM (
        'raw',
        'zipping',
        'zipped',
        'removing'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE dump_object_type AS ENUM (
        'td',
        'top',
        'heap'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- ====================================================================================
-- Composite Types for dump/pod data transport
-- ====================================================================================
DROP TYPE IF EXISTS pod_details CASCADE;

CREATE TYPE pod_details AS (
    id uuid,
    namespace text,
    service_name text,
    pod_name text,
    restart_time timestamp,
    last_active timestamp
);

DROP TYPE IF EXISTS dump_info CASCADE;

CREATE TYPE dump_info AS (
    pod_d pod_details,
    creation_time timestamp,
    file_size integer,
    dump_type dump_object_type
);

-- ====================================================================================
-- Table Definitions
-- ====================================================================================
CREATE TABLE IF NOT EXISTS dump_pods (
    id uuid NOT NULL,
    namespace text NOT NULL,
    service_name text NOT NULL,
    pod_name text NOT NULL,
    restart_time timestamp NOT NULL,
    last_active timestamp,
    dump_type dump_object_type[] DEFAULT '{}',
    PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS pod_idx ON dump_pods (namespace, service_name, pod_name);

CREATE TABLE IF NOT EXISTS heap_dumps (
    handle text NOT NULL,
    pod_id uuid NOT NULL,
    creation_time timestamp NOT NULL,
    file_size integer NOT NULL,
    PRIMARY KEY (handle)
);

CREATE TABLE IF NOT EXISTS timeline (
    ts_hour timestamp NOT NULL,
    status timeline_status NOT NULL,
    PRIMARY KEY (ts_hour)
);

-- Partitioned table for td/top dumps
CREATE TABLE dump_objects (
    id uuid not null, 
    pod_id uuid not null,
    creation_time timestamp not null,
    file_size integer not null,
    dump_type dump_object_type not null,
    PRIMARY KEY (id, creation_time)
) PARTITION BY RANGE (creation_time);

CREATE INDEX IF NOT EXISTS dump_objects_idx ON dump_objects (pod_id, creation_time, dump_type);

-- ====================================================================================
-- Functions for Insertion and Management
-- ====================================================================================

-- Create a timeline if not exists for a given hour
CREATE OR REPLACE FUNCTION create_timeline_if_not_exists(p_restart_time timestamp)
RETURNS TABLE(timeline_hour timestamp, is_created boolean) AS
$$
DECLARE
    created_ts timestamp;
BEGIN
    SELECT t.ts_hour INTO created_ts
    FROM timeline t
    WHERE t.ts_hour = date_trunc('hour', p_restart_time)
    LIMIT 1;

    IF NOT FOUND THEN
        INSERT INTO timeline (ts_hour, status)
        VALUES (date_trunc('hour', p_restart_time), 'raw'::timeline_status)
        RETURNING ts_hour INTO created_ts;
        RETURN QUERY SELECT created_ts, true;
    ELSE
        RETURN QUERY SELECT created_ts, false;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Create or update pod entry
CREATE OR REPLACE FUNCTION create_or_update_pod(
    p_namespace text,
    p_service_name text,
    p_pod_name text,
    p_restart_time timestamp
) RETURNS TABLE(id uuid, is_created boolean) AS
$$
DECLARE
    v_id uuid;
BEGIN
    SELECT dump_pods.id INTO v_id
    FROM dump_pods
    WHERE namespace = p_namespace
      AND service_name = p_service_name
      AND pod_name = p_pod_name
      AND restart_time = p_restart_time
    LIMIT 1;

    IF FOUND THEN
        RETURN QUERY SELECT v_id, false;
    ELSE
        INSERT INTO dump_pods (id, namespace, service_name, pod_name, restart_time)
        VALUES (gen_random_uuid(), p_namespace, p_service_name, p_pod_name, p_restart_time)
        RETURNING dump_pods.id INTO v_id;
        RETURN QUERY SELECT v_id, true;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Update pod's last active and dump_type
CREATE OR REPLACE FUNCTION update_pod_last_active(
    p_pod_id uuid,
    p_creation_time timestamp,
    p_dump_type dump_object_type
) RETURNS void AS
$$
BEGIN
    UPDATE dump_pods
    SET last_active = p_creation_time,
        dump_type = (
            SELECT ARRAY(
                SELECT DISTINCT unnest(array_append(dump_type, p_dump_type))
            )
        )
    WHERE id = p_pod_id;
END;
$$ LANGUAGE plpgsql;

-- Insert heap dump
CREATE OR REPLACE FUNCTION insert_heap_dumps(
    p_pod_id uuid,
    p_creation_time timestamp,
    p_file_size integer,
    p_pod_name text
) RETURNS void AS
$$
DECLARE
    table_name text := 'heap_dumps';
    handle text;
BEGIN
    handle := p_pod_name || '-heap-' || (extract(epoch FROM p_creation_time) * 1000)::bigint;
    EXECUTE format('INSERT INTO %I (handle, pod_id, creation_time, file_size) VALUES ($1, $2, $3, $4)', table_name)
    USING handle, p_pod_id, p_creation_time, p_file_size;
END;
$$ LANGUAGE plpgsql;

-- Insert td/top dump into dynamic partition
CREATE OR REPLACE FUNCTION insert_td_top_dumps(
    p_pod_id uuid,
    p_creation_time timestamp,
    p_file_size integer,
    p_dump_type dump_object_type
) RETURNS void AS
$$
DECLARE
    partition_name text;
    new_id UUID;
BEGIN
    new_id := gen_random_uuid();
    partition_name := 'dump_objects_' || extract(epoch FROM date_trunc('hour', p_creation_time))::bigint;

    EXECUTE format('INSERT INTO %I (id, pod_id, creation_time, file_size, dump_type) VALUES ($1, $2, $3, $4, $5)', partition_name)
    USING new_id, p_pod_id, p_creation_time, p_file_size, p_dump_type;
END;
$$ LANGUAGE plpgsql;

-- Create partition if missing for dump_objects
CREATE OR REPLACE FUNCTION create_partition_for_dump_objects(p_creation_time timestamp) RETURNS void AS
$$
DECLARE
    partition_name text;
    from_ts timestamp;
    to_ts timestamp;
BEGIN
    from_ts := date_trunc('hour', p_creation_time);
    to_ts := from_ts + interval '1 hour';
    partition_name := 'dump_objects_' || extract(epoch FROM from_ts)::bigint;

    IF NOT EXISTS (
        SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = partition_name
    ) THEN
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF dump_objects FOR VALUES FROM (''%s'') TO (''%s'')',
            partition_name, from_ts, to_ts
        );
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Function to insert dumps transactionally
CREATE OR REPLACE FUNCTION upsert_dumps_transactionally(
    p_time TIMESTAMP,
    p_heap_dumps dump_info[],
    p_td_top_dumps dump_info[]
)
RETURNS TABLE(
    timelines_created INT,
    pods_created INT,
    heap_dumps_inserted INT,
    td_top_dumps_inserted INT
) AS
$$
DECLARE
    timeline RECORD;
    dump_pod RECORD;
    dump dump_info;
    created_timeline INT := 0;
    created_pods INT := 0;
    inserted_heap_dumps INT := 0;
    inserted_td_top_dumps INT := 0;
BEGIN
    BEGIN
        SELECT * INTO timeline FROM create_timeline_if_not_exists(p_time);

        IF timeline.is_created THEN
            created_timeline := created_timeline + 1;
        END IF;

        PERFORM create_partition_for_dump_objects(p_time);

        FOREACH dump IN ARRAY p_heap_dumps
        LOOP
            SELECT * INTO dump_pod
            FROM create_or_update_pod(
                (dump).pod_d.namespace,
                (dump).pod_d.service_name,
                (dump).pod_d.pod_name,
                (dump).pod_d.restart_time
            );

            IF dump_pod.is_created THEN
                created_pods := created_pods + 1;
            END IF;

            PERFORM update_pod_last_active(dump_pod.id, (dump).creation_time, (dump).dump_type);
            PERFORM insert_heap_dumps(dump_pod.id, (dump).creation_time, (dump).file_size, (dump).pod_d.pod_name);
            inserted_heap_dumps := inserted_heap_dumps + 1;
        END LOOP;

        FOREACH dump IN ARRAY p_td_top_dumps
        LOOP
            SELECT * INTO dump_pod
            FROM create_or_update_pod(
                (dump).pod_d.namespace,
                (dump).pod_d.service_name,
                (dump).pod_d.pod_name,
                (dump).pod_d.restart_time
            );

            IF dump_pod.is_created THEN
                created_pods := created_pods + 1;
            END IF;

            PERFORM update_pod_last_active(dump_pod.id, (dump).creation_time, (dump).dump_type);
            PERFORM insert_td_top_dumps(dump_pod.id, (dump).creation_time, (dump).file_size, (dump).dump_type);
            inserted_td_top_dumps := inserted_td_top_dumps + 1;
        END LOOP;

        RETURN QUERY SELECT created_timeline, created_pods, inserted_heap_dumps, inserted_td_top_dumps;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE EXCEPTION 'Transaction failed: %', SQLERRM;
    END;
END;
$$ LANGUAGE plpgsql;
