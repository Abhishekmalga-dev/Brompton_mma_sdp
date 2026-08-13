--------------------------------------------------------------------------
-- Name        : asd_wrapup_summary_load_raw_table.sql
-- Description : Truncate and load JSON records into RAW table
--               via COPY INTO from Azure Blob external stage.
--               Blob path is passed as a substitution variable
--               so any data volume is handled (no SQL string limit).
--
-- Change History:
-- Mod            Date         Name           Reason
-- --------------------------------------------------------------------
-- PBI_1830398    08/12/2026   Abhi M         Initial
-- --------------------------------------------------------------------

USE SCHEMA ADMIN;

--==========================================================================
--Step 1: Truncate ASD_WRAPUP_SUMMARY_RAW table
--==========================================================================

SELECT 'Step 1 - Truncate ASD_WRAPUP_SUMMARY_RAW table started at ' || current_timestamp as "-";

TRUNCATE TABLE ASD_WRAPUP_SUMMARY_RAW;

--==========================================================================
--Step 2: Load JSON records into ASD_WRAPUP_SUMMARY_RAW from blob stage
--==========================================================================

SELECT 'Step 2 - Load JSON records into ASD_WRAPUP_SUMMARY_RAW started at ' || current_timestamp as "-";

COPY INTO ASD_WRAPUP_SUMMARY_RAW
(RECORD_CONTENT, LOAD_TS)
FROM (
    SELECT
        $1                      AS RECORD_CONTENT
        , CURRENT_TIMESTAMP(0)  AS LOAD_TS
    FROM &{blob_stage_path})
    FILE_FORMAT = (TYPE = 'JSON', STRIP_OUTER_ARRAY = TRUE)
PURGE = FALSE;