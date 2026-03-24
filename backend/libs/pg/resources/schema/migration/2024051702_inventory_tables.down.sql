-- drop inventory tables
DROP INDEX IF EXISTS s3_types_idx CASCADE;
DROP INDEX IF EXISTS s3_deadline_idx CASCADE;
DROP TABLE IF EXISTS s3_files;

DROP INDEX IF EXISTS temps_types_idx CASCADE;
DROP INDEX IF EXISTS temps_time_idx CASCADE;
DROP TABLE IF EXISTS temp_table_inventory;

