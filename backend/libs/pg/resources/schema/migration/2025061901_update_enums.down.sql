-- ====================================================================================
-- Rollback for Migration: 2025061901_update_enums
-- This rollback removes 'inverted_index' from the table_type ENUM.
-- All dependent rows in temp_table_inventory are deleted before type recreation.
-- ====================================================================================

-- ====================================================================================
-- Step 1: Delete dependent rows from temp_table_inventory
-- ====================================================================================
DELETE FROM temp_table_inventory WHERE table_type = 'inverted_index';

-- ====================================================================================
-- Step 2: Recreate ENUM without 'inverted_index'
-- ====================================================================================

-- Rename the existing type
ALTER TYPE table_type RENAME TO table_type_old;

-- Create the new type without 'inverted_index'
CREATE TYPE table_type AS ENUM (
    'calls',
    'traces',
    'dumps',
    'suspend'
);

-- Alter the column to use the new type
ALTER TABLE temp_table_inventory
ALTER COLUMN table_type TYPE table_type
    USING table_type::text::table_type;

-- Drop the old type
DROP TYPE table_type_old;
