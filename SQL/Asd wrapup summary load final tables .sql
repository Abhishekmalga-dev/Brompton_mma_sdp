--------------------------------------------------------------------------
-- Name        : asd_wrapup_summary_load_final_tables.sql
-- Description : Trim trailing spaces on work tables, then insert into
--               final tables only rows whose PK + UPDATED_ON
--               combination does not already exist (anti-join
--               LEFT JOIN ... WHERE PK IS NULL). Existing rows with
--               the same PK and UPDATED_ON are skipped. All timestamp
--               columns are explicitly cast to TIMESTAMP_NTZ on the
--               way into the final tables. Refresh runs parent-to-
--               child (conversation, then inquiry, then intent/
--               key_point/topic) since the child final tables
--               reference INQUIRY_ID from the parent.
--
-- Change History:
-- Mod            Date         Name           Reason
-- --------------------------------------------------------------------
-- PBI_1830398    08/12/2026   Abhi M         Initial
-- --------------------------------------------------------------------

USE SCHEMA ADMIN;

--==========================================================================
--Step 1: Trim trailing spaces from ASDW_WRAPUP_SUMMARY_CONVERSATION_LOAD
--==========================================================================

SELECT 'Step 1 - Trim ASDW_WRAPUP_SUMMARY_CONVERSATION_LOAD started at ' || current_timestamp as "-";
CALL TOOLS.ADMIN.SP_TRIM_CHAR_COLUMN('ASDW_WRAPUP_SUMMARY_CONVERSATION_LOAD', CURRENT_DATABASE());

--==========================================================================
--Step 2: Trim trailing spaces from ASDW_WRAPUP_SUMMARY_INQUIRY_LOAD
--==========================================================================

SELECT 'Step 2 - Trim ASDW_WRAPUP_SUMMARY_INQUIRY_LOAD started at ' || current_timestamp as "-";
CALL TOOLS.ADMIN.SP_TRIM_CHAR_COLUMN('ASDW_WRAPUP_SUMMARY_INQUIRY_LOAD', CURRENT_DATABASE());

--==========================================================================
--Step 3: Trim trailing spaces from ASDW_WRAPUP_SUMMARY_INQUIRY_INTENT_LOAD
--==========================================================================

SELECT 'Step 3 - Trim ASDW_WRAPUP_SUMMARY_INQUIRY_INTENT_LOAD started at ' || current_timestamp as "-";
CALL TOOLS.ADMIN.SP_TRIM_CHAR_COLUMN('ASDW_WRAPUP_SUMMARY_INQUIRY_INTENT_LOAD', CURRENT_DATABASE());

--==========================================================================
--Step 4: Trim trailing spaces from ASDW_WRAPUP_SUMMARY_INQUIRY_KEY_POINT_LOAD
--==========================================================================

SELECT 'Step 4 - Trim ASDW_WRAPUP_SUMMARY_INQUIRY_KEY_POINT_LOAD started at ' || current_timestamp as "-";
CALL TOOLS.ADMIN.SP_TRIM_CHAR_COLUMN('ASDW_WRAPUP_SUMMARY_INQUIRY_KEY_POINT_LOAD', CURRENT_DATABASE());

--==========================================================================
--Step 5: Trim trailing spaces from ASDW_WRAPUP_SUMMARY_INQUIRY_TOPIC_LOAD
--==========================================================================

SELECT 'Step 5 - Trim ASDW_WRAPUP_SUMMARY_INQUIRY_TOPIC_LOAD started at ' || current_timestamp as "-";
CALL TOOLS.ADMIN.SP_TRIM_CHAR_COLUMN('ASDW_WRAPUP_SUMMARY_INQUIRY_TOPIC_LOAD', CURRENT_DATABASE());

--==========================================================================
--Step 6: Refresh ASDT_WRAPUP_SUMMARY_CONVERSATION
--==========================================================================

SELECT 'Step 6 - Refresh ASDT_WRAPUP_SUMMARY_CONVERSATION started at ' || current_timestamp as "-";

INSERT INTO ASDT_WRAPUP_SUMMARY_CONVERSATION
SELECT
    w.CONVERSATION_ID,
    w.MESSAGE_ID,
    w.SEQUENCE_NUMBER,
    w.GENERATED_AT::TIMESTAMP_NTZ,
    w.MODEL_VERSION,
    w.PROMPT_TOKENS,
    w.COMPLETION_TOKENS,
    w.TOTAL_TOKENS,
    w.UPDATED_ON::TIMESTAMP_NTZ,
    w.LOAD_TS::TIMESTAMP_NTZ
FROM ASDW_WRAPUP_SUMMARY_CONVERSATION_LOAD w
LEFT JOIN ASDT_WRAPUP_SUMMARY_CONVERSATION t
       ON t.CONVERSATION_ID = w.CONVERSATION_ID
      AND t.UPDATED_ON = w.UPDATED_ON
WHERE t.CONVERSATION_ID IS NULL;

--==========================================================================
--Step 7: Refresh ASDT_WRAPUP_SUMMARY_INQUIRY
--==========================================================================

SELECT 'Step 7 - Refresh ASDT_WRAPUP_SUMMARY_INQUIRY started at ' || current_timestamp as "-";

INSERT INTO ASDT_WRAPUP_SUMMARY_INQUIRY
SELECT
    w.INQUIRY_ID,
    w.CONVERSATION_ID,
    w.INQUIRY_NUMBER,
    w.INQUIRY_ROLE,
    w.INQUIRY_PARENT_NUMBER,
    w.CLASSIFICATION,
    w.DESCRIPTION,
    w.RESOLUTION,
    w.STATUS,
    w.INQUIRY_SUMMARY,
    w.MEME_CK,
    w.UPDATED_ON::TIMESTAMP_NTZ,
    w.LOAD_TS::TIMESTAMP_NTZ
FROM ASDW_WRAPUP_SUMMARY_INQUIRY_LOAD w
LEFT JOIN ASDT_WRAPUP_SUMMARY_INQUIRY t
       ON t.INQUIRY_ID = w.INQUIRY_ID
      AND t.UPDATED_ON = w.UPDATED_ON
WHERE t.INQUIRY_ID IS NULL;

--==========================================================================
--Step 8: Refresh ASDT_WRAPUP_SUMMARY_INQUIRY_INTENT
--==========================================================================

SELECT 'Step 8 - Refresh ASDT_WRAPUP_SUMMARY_INQUIRY_INTENT started at ' || current_timestamp as "-";

INSERT INTO ASDT_WRAPUP_SUMMARY_INQUIRY_INTENT
SELECT
    w.INQUIRY_ID,
    w.INTENT_ID,
    w.INTENT_LABEL,
    w.UPDATED_ON::TIMESTAMP_NTZ,
    w.LOAD_TS::TIMESTAMP_NTZ
FROM ASDW_WRAPUP_SUMMARY_INQUIRY_INTENT_LOAD w
LEFT JOIN ASDT_WRAPUP_SUMMARY_INQUIRY_INTENT t
       ON t.INQUIRY_ID = w.INQUIRY_ID
      AND t.INTENT_ID = w.INTENT_ID
      AND t.UPDATED_ON = w.UPDATED_ON
WHERE t.INQUIRY_ID IS NULL;

--==========================================================================
--Step 9: Refresh ASDT_WRAPUP_SUMMARY_INQUIRY_KEY_POINT
--==========================================================================

SELECT 'Step 9 - Refresh ASDT_WRAPUP_SUMMARY_INQUIRY_KEY_POINT started at ' || current_timestamp as "-";

INSERT INTO ASDT_WRAPUP_SUMMARY_INQUIRY_KEY_POINT
SELECT
    w.INQUIRY_ID,
    w.KEY_POINT_SEQ,
    w.KEY_POINT_TEXT,
    w.UPDATED_ON::TIMESTAMP_NTZ,
    w.LOAD_TS::TIMESTAMP_NTZ
FROM ASDW_WRAPUP_SUMMARY_INQUIRY_KEY_POINT_LOAD w
LEFT JOIN ASDT_WRAPUP_SUMMARY_INQUIRY_KEY_POINT t
       ON t.INQUIRY_ID = w.INQUIRY_ID
      AND t.KEY_POINT_SEQ = w.KEY_POINT_SEQ
      AND t.UPDATED_ON = w.UPDATED_ON
WHERE t.INQUIRY_ID IS NULL;

--==========================================================================
--Step 10: Refresh ASDT_WRAPUP_SUMMARY_INQUIRY_TOPIC
--==========================================================================

SELECT 'Step 10 - Refresh ASDT_WRAPUP_SUMMARY_INQUIRY_TOPIC started at ' || current_timestamp as "-";

INSERT INTO ASDT_WRAPUP_SUMMARY_INQUIRY_TOPIC
SELECT
    w.INQUIRY_ID,
    w.TOPIC_SEQ,
    w.TOPIC_TEXT,
    w.UPDATED_ON::TIMESTAMP_NTZ,
    w.LOAD_TS::TIMESTAMP_NTZ
FROM ASDW_WRAPUP_SUMMARY_INQUIRY_TOPIC_LOAD w
LEFT JOIN ASDT_WRAPUP_SUMMARY_INQUIRY_TOPIC t
       ON t.INQUIRY_ID = w.INQUIRY_ID
      AND t.TOPIC_SEQ = w.TOPIC_SEQ
      AND t.UPDATED_ON = w.UPDATED_ON
WHERE t.INQUIRY_ID IS NULL;