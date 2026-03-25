-- create enums
DO $$ BEGIN
    CREATE TYPE table_type AS ENUM (
        'calls',
        'traces',
        'dumps',
        'suspend'
        );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;
DO $$ BEGIN
CREATE TYPE table_status AS ENUM (
    'creating',
    'ready',
    'persisting',
    'persisted',
    'to_delete'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;
DO $$ BEGIN
CREATE TYPE file_type AS ENUM (
    'calls',
    'traces',
    'dumps',
    'heap'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;
DO $$ BEGIN
CREATE TYPE file_status AS ENUM (
    'creating', -- created by collector
    'created', -- collector finished creating Parquet file on the local PV (it is ready to transfer to S3)
    'transferring', -- collector started sending file to S3
    'completed', -- collector finished creating Parquet file on the local PV (it is ready to transfer to S3)
    'to_delete' -- marked by k8 job before deleting permanently
    -- NOTE: after TTL k8 job will delete file (first step) and delete the row from inventory table (last step)
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;
