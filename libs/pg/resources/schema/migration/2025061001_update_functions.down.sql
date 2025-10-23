-- ====================================================================================
-- Rollback for Migration: 2025061001_update_functions
-- This script restores 2025052101_update_functions state of functions.
-- ====================================================================================


-- =======================
-- DROP FUNCTIONS
-- =======================
DROP FUNCTION IF EXISTS trim_heap_dumps(INTEGER);
