CREATE TABLE IF NOT EXISTS dump_objects_{{.TimeStamp}}
PARTITION OF dump_objects
FOR VALUES FROM {{.From}} TO {{.To}};

CREATE INDEX IF NOT EXISTS dump_objects_idx_{{.TimeStamp}} ON dump_objects_{{.TimeStamp}} (pod_id, creation_time, dump_type);
