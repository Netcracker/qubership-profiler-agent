-- ====================================================================================
-- Migration 2025061901: Add 'inverted_index' to table_type ENUM
-- This migration extends the table_type ENUM to support the 'inverted_index' value,
-- enabling proper classification of inverted index tables in metadata.
-- Affected objects:
-- - table_type (modified)
-- ====================================================================================


-- ====================================================================================
-- Extend the table_type ENUM to support 'inverted_index' as a valid type.
-- ====================================================================================
ALTER TYPE table_type ADD VALUE IF NOT EXISTS 'inverted_index';
