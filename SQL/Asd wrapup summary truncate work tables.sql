--------------------------------------------------------------------------
-- Name        : asd_wrapup_summary_truncate_work_tables.sql
-- Description : Truncate the 5 wrapup_summary work tables ahead of each
--               populate run. Child-to-parent order (intent/key_point/
--               topic first, then inquiry, then conversation) so we
--               never leave orphaned-looking state mid-run.
--
-- Change History:
-- Mod            Date         Name           Reason
-- --------------------------------------------------------------------
-- PBI_1830398    08/12/2026   Abhi M         Initial
-- --------------------------------------------------------------------

USE SCHEMA ADMIN;

--==========================================================================
--Step 1: Truncate ASDW_WRAPUP_SUMMARY_INQUIRY_INTENT_LOAD
--==========================================================================

SELECT 'Step 1 - Truncate ASDW_WRAPUP_SUMMARY_INQUIRY_INTENT_LOAD started at ' || current_timestamp as "-";

TRUNCATE TABLE ASDW_WRAPUP_SUMMARY_INQUIRY_INTENT_LOAD;

--==========================================================================
--Step 2: Truncate ASDW_WRAPUP_SUMMARY_INQUIRY_KEY_POINT_LOAD
--==========================================================================

SELECT 'Step 2 - Truncate ASDW_WRAPUP_SUMMARY_INQUIRY_KEY_POINT_LOAD started at ' || current_timestamp as "-";

TRUNCATE TABLE ASDW_WRAPUP_SUMMARY_INQUIRY_KEY_POINT_LOAD;

--==========================================================================
--Step 3: Truncate ASDW_WRAPUP_SUMMARY_INQUIRY_TOPIC_LOAD
--==========================================================================

SELECT 'Step 3 - Truncate ASDW_WRAPUP_SUMMARY_INQUIRY_TOPIC_LOAD started at ' || current_timestamp as "-";

TRUNCATE TABLE ASDW_WRAPUP_SUMMARY_INQUIRY_TOPIC_LOAD;

--==========================================================================
--Step 4: Truncate ASDW_WRAPUP_SUMMARY_INQUIRY_LOAD
--==========================================================================

SELECT 'Step 4 - Truncate ASDW_WRAPUP_SUMMARY_INQUIRY_LOAD started at ' || current_timestamp as "-";

TRUNCATE TABLE ASDW_WRAPUP_SUMMARY_INQUIRY_LOAD;

--==========================================================================
--Step 5: Truncate ASDW_WRAPUP_SUMMARY_CONVERSATION_LOAD
--==========================================================================

SELECT 'Step 5 - Truncate ASDW_WRAPUP_SUMMARY_CONVERSATION_LOAD started at ' || current_timestamp as "-";

TRUNCATE TABLE ASDW_WRAPUP_SUMMARY_CONVERSATION_LOAD;