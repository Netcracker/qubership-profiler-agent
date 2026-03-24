-- drop inventory tables
DROP TABLE IF EXISTS suspend;
DROP TABLE IF EXISTS pod_statistics;
DROP TABLE IF EXISTS params;
DROP TABLE IF EXISTS dictionary;

DROP INDEX IF EXISTS pod_restarts_ns_idx CASCADE;
DROP INDEX IF EXISTS pod_restarts_id_idx CASCADE;
DROP TABLE IF EXISTS pod_restarts;

DROP INDEX IF EXISTS pods_id_idx CASCADE;
DROP INDEX IF EXISTS pods_ns_idx CASCADE;
DROP TABLE IF EXISTS pods;
