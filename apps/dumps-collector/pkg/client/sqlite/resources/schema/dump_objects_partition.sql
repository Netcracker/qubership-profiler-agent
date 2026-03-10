-- Template for creating a partition table for dump_objects
-- This template is used to create hourly partition tables dynamically

CREATE TABLE IF NOT EXISTS dump_objects_{{.TimeStamp}} (
    id TEXT NOT NULL PRIMARY KEY,
    pod_id TEXT NOT NULL,
    creation_time TIMESTAMP NOT NULL,
    file_size INTEGER NOT NULL,
    dump_type TEXT NOT NULL CHECK(dump_type IN ('td', 'top', 'heap')),
    FOREIGN KEY (pod_id) REFERENCES dump_pods(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_dump_objects_{{.TimeStamp}}_composite 
    ON dump_objects_{{.TimeStamp}}(pod_id, creation_time, dump_type);

CREATE INDEX IF NOT EXISTS idx_dump_objects_{{.TimeStamp}}_creation_time 
    ON dump_objects_{{.TimeStamp}}(creation_time);

CREATE INDEX IF NOT EXISTS idx_dump_objects_{{.TimeStamp}}_pod_id 
    ON dump_objects_{{.TimeStamp}}(pod_id);
