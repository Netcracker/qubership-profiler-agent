-- =======================
-- DROP ENUM TYPES
-- =======================
DROP TYPE IF EXISTS timeline_status CASCADE;
DROP TYPE IF EXISTS dump_object_type CASCADE;

-- =======================
-- DROP INDEXES
-- =======================
DROP INDEX IF EXISTS pod_idx;
DROP INDEX IF EXISTS dump_objects_idx;

-- =======================
-- DROP TABLES
-- =======================
DROP TABLE IF EXISTS dump_pods CASCADE;
DROP TABLE IF EXISTS heap_dumps CASCADE;
DROP TABLE IF EXISTS timeline CASCADE;
DROP TABLE IF EXISTS dump_objects CASCADE;

-- =======================
-- DROP FUNCTIONS
-- =======================
DROP FUNCTION IF EXISTS upsert_dumps_transactionally(
    TIMESTAMP,
    heap_dumps[],
    td_top_dumps[]
);

DROP FUNCTION IF EXISTS create_timeline_if_not_exists(TIMESTAMP);

DROP FUNCTION IF EXISTS create_or_update_pod(
    TEXT,
    TEXT,
    TEXT,
    TIMESTAMP
);

DROP FUNCTION IF EXISTS update_pod_last_active(
    UUID,
    TIMESTAMP,
    dump_object_type
);

DROP FUNCTION IF EXISTS insert_heap_dumps(
    UUID,
    TIMESTAMP,
    INTEGER,
    dump_object_type
);

DROP FUNCTION IF EXISTS insert_td_top_dumps(
    UUID,
    TIMESTAMP,
    INTEGER,
    dump_object_type
);

DROP FUNCTION IF EXISTS create_partition_for_dump_objects(TIMESTAMP);
