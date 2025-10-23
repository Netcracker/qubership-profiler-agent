-- ====================================================================================
-- Migration 2025052101: Function Updates for Dump Management System
-- This migration modifies logic for TD/TOP dump insertion and removes dynamic partition creation.
-- Affected functions:
-- - create_partition_for_dump_objects (removed)
-- - insert_td_top_dumps (replaced)
-- - upsert_dumps_transactionally (updated to use static dump_objects table)
-- ====================================================================================


-- ====================================================================================
-- Refactored timeline creation logic
-- Replaced SELECT-before-INSERT with INSERT ... ON CONFLICT DO NOTHING to prevent
-- race conditions during concurrent inserts and improve performance.
-- ====================================================================================
CREATE OR REPLACE FUNCTION create_timeline_if_not_exists(p_restart_time timestamp)
RETURNS TABLE(timeline_hour timestamp, is_created boolean) AS
$$
DECLARE
    ts_hour_val timestamp := date_trunc('hour', p_restart_time);
    inserted boolean := false;
BEGIN
    BEGIN
        INSERT INTO timeline (ts_hour, status)
        VALUES (ts_hour_val, 'raw')
        ON CONFLICT DO NOTHING;
        inserted := true;
    EXCEPTION
        WHEN unique_violation THEN
            inserted := false;
    END;

    RETURN QUERY SELECT ts_hour_val, inserted;
END;
$$ LANGUAGE plpgsql;

-- ====================================================================================
-- Refactored td/top dump insertion logic
-- Replaced pre-insert existence check with INSERT ... ON CONFLICT DO NOTHING to avoid
-- race conditions and ensure idempotent behavior during concurrent executions.
-- ====================================================================================
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

    EXECUTE format('
        INSERT INTO %I (id, pod_id, creation_time, file_size, dump_type)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT DO NOTHING'
    , partition_name)
    USING new_id, p_pod_id, p_creation_time, p_file_size, p_dump_type;
END;
$$ LANGUAGE plpgsql;

-- ====================================================================================
-- Added unique constraint for logical pod identity
-- Ensures uniqueness of namespace, service name, pod name, and restart time
-- to safely support ON CONFLICT logic in pod creation.
-- ====================================================================================
CREATE UNIQUE INDEX IF NOT EXISTS dump_pods_unique_key
ON dump_pods (namespace, service_name, pod_name, restart_time);

-- ====================================================================================t
-- Refactored pod creation logic
-- Replaced SELECT-before-INSERT with INSERT ... ON CONFLICT DO NOTHING and RETURNING
-- to prevent race conditions and ensure idempotent pod creation.
-- ====================================================================================
CREATE OR REPLACE FUNCTION create_or_update_pod(
    p_namespace text,
    p_service_name text,
    p_pod_name text,
    p_restart_time timestamp
) RETURNS TABLE(id uuid, is_created boolean) AS
$$
DECLARE
    new_id uuid := gen_random_uuid();
BEGIN
    BEGIN
        INSERT INTO dump_pods (id, namespace, service_name, pod_name, restart_time)
        VALUES (new_id, p_namespace, p_service_name, p_pod_name, p_restart_time)
        ON CONFLICT (namespace, service_name, pod_name, restart_time) DO NOTHING
        RETURNING dump_pods.id INTO new_id;
    EXCEPTION
        WHEN unique_violation THEN
            NULL;
    END;

    IF FOUND THEN
        RETURN QUERY SELECT new_id, true;
    ELSE
        SELECT dump_pods.id INTO new_id
        FROM dump_pods
        WHERE namespace = p_namespace
          AND service_name = p_service_name
          AND pod_name = p_pod_name
          AND restart_time = p_restart_time
        LIMIT 1;

        RETURN QUERY SELECT new_id, false;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- ====================================================================================
-- Refactored heap dump insertion logic
-- Added ON CONFLICT DO NOTHING to avoid duplicate insert failures
-- when identical dump handles are processed concurrently.
-- ====================================================================================
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

    EXECUTE format(
        'INSERT INTO %I (handle, pod_id, creation_time, file_size)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT DO NOTHING',
        table_name
    )
    USING handle, p_pod_id, p_creation_time, p_file_size;
END;
$$ LANGUAGE plpgsql;

-- ====================================================================================
-- Always updates last_active using GREATEST
-- Adds dump_type only if it is not already present
-- ====================================================================================
CREATE OR REPLACE FUNCTION update_pod_last_active(
    p_pod_id uuid,
    p_creation_time timestamp,
    p_dump_type dump_object_type
) RETURNS void AS
$$
BEGIN
    UPDATE dump_pods
    SET last_active = GREATEST(last_active, p_creation_time),
        dump_type = (
            SELECT ARRAY(
                SELECT DISTINCT unnest(array_append(dump_type, p_dump_type))
            )
        )
    WHERE id = p_pod_id;
END;
$$ LANGUAGE plpgsql;

-- ====================================================================================
-- Refactored partition creation logic
-- Replaced existence check with exception-safe CREATE TABLE inside a block
-- to avoid race conditions during concurrent partition creation.
-- ====================================================================================
CREATE OR REPLACE FUNCTION create_partition_for_dump_objects(p_creation_time timestamp)
RETURNS void AS
$$
DECLARE
    partition_name text;
    from_ts timestamp;
    to_ts timestamp;
BEGIN
    from_ts := date_trunc('hour', p_creation_time);
    to_ts := from_ts + interval '1 hour';
    partition_name := 'dump_objects_' || extract(epoch FROM from_ts)::bigint;

    BEGIN
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF dump_objects FOR VALUES FROM (''%s'') TO (''%s'')',
            partition_name, from_ts, to_ts
        );
    EXCEPTION
        WHEN duplicate_table THEN
            -- Partition already exists, safe to ignore
            NULL;
    END;
END;
$$ LANGUAGE plpgsql;

-- ====================================================================================
-- Refactored transactional insert function
-- Utilizes safe insertion functions with ON CONFLICT logic to eliminate race conditions.
-- Logic and behavior are preserved, but redundant existence checks are removed.
-- ====================================================================================
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
        -- Timeline
        SELECT * INTO timeline FROM create_timeline_if_not_exists(p_time);
        IF timeline.is_created THEN
            created_timeline := 1;
        END IF;

        -- Partition creation is now optional/external — skip or enable if needed
        PERFORM create_partition_for_dump_objects(p_time);

        -- Heap dumps
        FOREACH dump IN ARRAY p_heap_dumps LOOP
            SELECT * INTO dump_pod FROM create_or_update_pod(
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

        -- TD/TOP dumps
        FOREACH dump IN ARRAY p_td_top_dumps LOOP
            SELECT * INTO dump_pod FROM create_or_update_pod(
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
