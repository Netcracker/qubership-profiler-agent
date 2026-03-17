-- ====================================================================================
-- SQLite Schema for Dump Management System
-- This schema is compatible with SQLite and emulates PostgreSQL features
-- ====================================================================================

-- Enable foreign keys
PRAGMA foreign_keys = ON;

-- ====================================================================================
-- Table: dump_pods
-- ====================================================================================
CREATE TABLE IF NOT EXISTS dump_pods (
    id TEXT NOT NULL PRIMARY KEY,
    namespace TEXT NOT NULL,
    service_name TEXT NOT NULL,
    pod_name TEXT NOT NULL,
    restart_time TIMESTAMP NOT NULL,
    last_active TIMESTAMP,
    dump_type TEXT DEFAULT '[]' -- JSON array of dump types: ["td", "top", "heap"]
);

CREATE INDEX IF NOT EXISTS idx_dump_pods_composite ON dump_pods (namespace, service_name, pod_name);
CREATE INDEX IF NOT EXISTS idx_dump_pods_last_active ON dump_pods (last_active);

-- ====================================================================================
-- Table: timeline
-- ====================================================================================
CREATE TABLE IF NOT EXISTS timeline (
    ts_hour TIMESTAMP NOT NULL PRIMARY KEY,
    status TEXT NOT NULL CHECK (status IN ('raw', 'zipping', 'zipped', 'removing'))
);

CREATE INDEX IF NOT EXISTS idx_timeline_status ON timeline (status);
CREATE INDEX IF NOT EXISTS idx_timeline_ts_hour ON timeline (ts_hour);

-- ====================================================================================
-- Table: heap_dumps
-- ====================================================================================
CREATE TABLE IF NOT EXISTS heap_dumps (
    handle TEXT NOT NULL PRIMARY KEY,
    pod_id TEXT NOT NULL,
    creation_time TIMESTAMP NOT NULL,
    file_size INTEGER NOT NULL,
    FOREIGN KEY (pod_id) REFERENCES dump_pods (id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_heap_dumps_pod_id ON heap_dumps (pod_id);
CREATE INDEX IF NOT EXISTS idx_heap_dumps_creation_time ON heap_dumps (creation_time);
CREATE INDEX IF NOT EXISTS idx_heap_dumps_composite ON heap_dumps (pod_id, creation_time);

-- ====================================================================================
-- Partitioned tables emulation for dump_objects
-- Tables are created dynamically with naming: dump_objects_<epoch_timestamp>
-- Example template for a partition table:
-- ====================================================================================

-- Note: Actual partition tables are created dynamically at runtime.
-- Template for reference:
--
-- CREATE TABLE IF NOT EXISTS dump_objects_<epoch> (
--     id TEXT NOT NULL PRIMARY KEY,
--     pod_id TEXT NOT NULL,
--     creation_time TIMESTAMP NOT NULL,
--     file_size INTEGER NOT NULL,
--     dump_type TEXT NOT NULL CHECK (dump_type IN ('td', 'top', 'heap')),
--     FOREIGN KEY (pod_id) REFERENCES dump_pods (id) ON DELETE CASCADE
-- );
--
-- CREATE INDEX IF NOT EXISTS idx_dump_objects_<epoch>_composite
--     ON dump_objects_<epoch>(pod_id, creation_time, dump_type);
-- CREATE INDEX IF NOT EXISTS idx_dump_objects_<epoch>_creation_time
--     ON dump_objects_<epoch>(creation_time);

-- ====================================================================================
-- Metadata table to track partition tables
-- ====================================================================================
CREATE TABLE IF NOT EXISTS dump_objects_partitions (
    partition_name TEXT NOT NULL PRIMARY KEY,
    hour_epoch INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dump_objects_partitions_epoch ON dump_objects_partitions (hour_epoch);
