-- ====================================================================================
-- Migration 2025061001: Function Updates for Dump Management System
-- This migration adds a new feature to clean up heap dumps that have exceeded the limit.
-- Affected functions:
-- - trim_heap_dumps (created)
-- ====================================================================================


-- ====================================================================================
-- Delete heap dumps that exceed the per-pod limit (limit_per_pod).
-- For each pod (identified by the prefix before the first underscore in the handle),
-- the newest N dumps (based on creation_time) are kept.
-- Older dumps are deleted and returned as a result of the function.
-- ====================================================================================
CREATE OR REPLACE FUNCTION trim_heap_dumps(limit_per_pod INTEGER)
RETURNS TABLE (
    handle TEXT,
    pod_id UUID,
    creation_time TIMESTAMP,
    file_size INTEGER
)
LANGUAGE SQL
AS $$
    DELETE FROM heap_dumps hd
    USING (
        SELECT handle FROM (
            SELECT handle,
                   ROW_NUMBER() OVER (
                       PARTITION BY split_part(handle, '_', 1)
                       ORDER BY creation_time DESC
                   ) AS row_num
            FROM heap_dumps
        ) ranked
        WHERE row_num > limit_per_pod
    ) to_delete
    WHERE hd.handle = to_delete.handle
    RETURNING hd.handle, hd.pod_id, hd.creation_time, hd.file_size;
$$;
