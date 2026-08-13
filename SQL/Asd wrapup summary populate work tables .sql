--------------------------------------------------------------------------
-- Name        : asd_wrapup_summary_populate_work_tables.sql
-- Description : Populate 5 wrapup_summary work tables from RAW table.
--               Conversation (header) uses direct JSON extraction.
--               Inquiry uses LATERAL FLATTEN on summary:inquiries.
--               Intent/KeyPoint/Topic use nested LATERAL FLATTEN off
--               the same inquiries array so INQUIRY_ID can be
--               recomputed identically and used as the FK.
--               INQUIRY_ID is a deterministic hash of
--               CONVERSATION_ID + array index (NOT random) so the
--               value is reproducible across runs.
--
-- Change History:
-- Mod            Date         Name           Reason
-- --------------------------------------------------------------------
-- PBI_1830398    08/12/2026   Abhi M         Initial
-- --------------------------------------------------------------------

USE SCHEMA ADMIN;

--==========================================================================
--Step 1: Populate ASDW_WRAPUP_SUMMARY_CONVERSATION_LOAD (header)
--==========================================================================

SELECT 'Step 1 - Populate ASDW_WRAPUP_SUMMARY_CONVERSATION_LOAD started at ' || current_timestamp as "-";

INSERT INTO ASDW_WRAPUP_SUMMARY_CONVERSATION_LOAD (
    CONVERSATION_ID,
    MESSAGE_ID,
    SEQUENCE_NUMBER,
    GENERATED_AT,
    MODEL_VERSION,
    PROMPT_TOKENS,
    COMPLETION_TOKENS,
    TOTAL_TOKENS,
    UPDATED_ON,
    LOAD_TS
)
SELECT
    RECORD_CONTENT:json_body:conversation_id::VARCHAR                       AS CONVERSATION_ID,
    RECORD_CONTENT:message_id::VARCHAR                                      AS MESSAGE_ID,
    TRY_TO_NUMBER(RECORD_CONTENT:sequence_number::VARCHAR)                  AS SEQUENCE_NUMBER,
    TRY_TO_TIMESTAMP_NTZ(RECORD_CONTENT:json_body:generated_at::VARCHAR)    AS GENERATED_AT,
    RECORD_CONTENT:json_body:model_version::VARCHAR                        AS MODEL_VERSION,
    TRY_TO_NUMBER(RECORD_CONTENT:json_body:token_usage:prompt_tokens::VARCHAR)      AS PROMPT_TOKENS,
    TRY_TO_NUMBER(RECORD_CONTENT:json_body:token_usage:completion_tokens::VARCHAR)  AS COMPLETION_TOKENS,
    TRY_TO_NUMBER(RECORD_CONTENT:json_body:token_usage:total_tokens::VARCHAR)       AS TOTAL_TOKENS,
    CURRENT_TIMESTAMP(0)                                                    AS UPDATED_ON,
    CURRENT_TIMESTAMP(0)                                                    AS LOAD_TS
FROM ASD_WRAPUP_SUMMARY_RAW
WHERE RECORD_CONTENT:json_body:conversation_id::VARCHAR IS NOT NULL;

--==========================================================================
--Step 2: Populate ASDW_WRAPUP_SUMMARY_INQUIRY_LOAD (array)
--==========================================================================

SELECT 'Step 2 - Populate ASDW_WRAPUP_SUMMARY_INQUIRY_LOAD started at ' || current_timestamp as "-";

INSERT INTO ASDW_WRAPUP_SUMMARY_INQUIRY_LOAD (
    INQUIRY_ID,
    CONVERSATION_ID,
    INQUIRY_NUMBER,
    INQUIRY_ROLE,
    INQUIRY_PARENT_NUMBER,
    CLASSIFICATION,
    DESCRIPTION,
    RESOLUTION,
    STATUS,
    INQUIRY_SUMMARY,
    MEME_CK,
    UPDATED_ON,
    LOAD_TS
)
SELECT
    MD5(RECORD_CONTENT:json_body:conversation_id::VARCHAR || '-' || inq.INDEX::VARCHAR)  AS INQUIRY_ID,
    RECORD_CONTENT:json_body:conversation_id::VARCHAR                       AS CONVERSATION_ID,
    inq.VALUE:inquiry_number::VARCHAR                                       AS INQUIRY_NUMBER,
    inq.VALUE:inquiry_role::VARCHAR                                         AS INQUIRY_ROLE,
    inq.VALUE:inquiry_parent_number::VARCHAR                                AS INQUIRY_PARENT_NUMBER,
    inq.VALUE:classification::VARCHAR                                       AS CLASSIFICATION,
    inq.VALUE:description::VARCHAR                                          AS DESCRIPTION,
    inq.VALUE:resolution::VARCHAR                                           AS RESOLUTION,
    inq.VALUE:status::VARCHAR                                               AS STATUS,
    inq.VALUE:inquiry_summary::VARCHAR                                      AS INQUIRY_SUMMARY,
    inq.VALUE:meme_ck::VARCHAR                                              AS MEME_CK,
    CURRENT_TIMESTAMP(0)                                                    AS UPDATED_ON,
    CURRENT_TIMESTAMP(0)                                                    AS LOAD_TS
FROM ASD_WRAPUP_SUMMARY_RAW,
LATERAL FLATTEN(input => RECORD_CONTENT:json_body:summary:inquiries) inq
WHERE RECORD_CONTENT:json_body:conversation_id::VARCHAR IS NOT NULL;

--==========================================================================
--Step 3: Populate ASDW_WRAPUP_SUMMARY_INQUIRY_INTENT_LOAD (nested array)
--==========================================================================

SELECT 'Step 3 - Populate ASDW_WRAPUP_SUMMARY_INQUIRY_INTENT_LOAD started at ' || current_timestamp as "-";

INSERT INTO ASDW_WRAPUP_SUMMARY_INQUIRY_INTENT_LOAD (
    INQUIRY_ID,
    INTENT_ID,
    INTENT_LABEL,
    UPDATED_ON,
    LOAD_TS
)
SELECT
    MD5(RECORD_CONTENT:json_body:conversation_id::VARCHAR || '-' || inq.INDEX::VARCHAR)  AS INQUIRY_ID,
    TRY_TO_NUMBER(intent.VALUE:id::VARCHAR)                                 AS INTENT_ID,
    intent.VALUE:label::VARCHAR                                            AS INTENT_LABEL,
    CURRENT_TIMESTAMP(0)                                                    AS UPDATED_ON,
    CURRENT_TIMESTAMP(0)                                                    AS LOAD_TS
FROM ASD_WRAPUP_SUMMARY_RAW,
LATERAL FLATTEN(input => RECORD_CONTENT:json_body:summary:inquiries) inq,
LATERAL FLATTEN(input => inq.VALUE:intents) intent
WHERE RECORD_CONTENT:json_body:conversation_id::VARCHAR IS NOT NULL;

--==========================================================================
--Step 4: Populate ASDW_WRAPUP_SUMMARY_INQUIRY_KEY_POINT_LOAD (nested array)
--==========================================================================

SELECT 'Step 4 - Populate ASDW_WRAPUP_SUMMARY_INQUIRY_KEY_POINT_LOAD started at ' || current_timestamp as "-";

INSERT INTO ASDW_WRAPUP_SUMMARY_INQUIRY_KEY_POINT_LOAD (
    INQUIRY_ID,
    KEY_POINT_SEQ,
    KEY_POINT_TEXT,
    UPDATED_ON,
    LOAD_TS
)
SELECT
    MD5(RECORD_CONTENT:json_body:conversation_id::VARCHAR || '-' || inq.INDEX::VARCHAR)  AS INQUIRY_ID,
    key_point.INDEX + 1                                                     AS KEY_POINT_SEQ,
    key_point.VALUE::VARCHAR                                                AS KEY_POINT_TEXT,
    CURRENT_TIMESTAMP(0)                                                    AS UPDATED_ON,
    CURRENT_TIMESTAMP(0)                                                    AS LOAD_TS
FROM ASD_WRAPUP_SUMMARY_RAW,
LATERAL FLATTEN(input => RECORD_CONTENT:json_body:summary:inquiries) inq,
LATERAL FLATTEN(input => inq.VALUE:key_points) key_point
WHERE RECORD_CONTENT:json_body:conversation_id::VARCHAR IS NOT NULL;

--==========================================================================
--Step 5: Populate ASDW_WRAPUP_SUMMARY_INQUIRY_TOPIC_LOAD (nested array)
--==========================================================================

SELECT 'Step 5 - Populate ASDW_WRAPUP_SUMMARY_INQUIRY_TOPIC_LOAD started at ' || current_timestamp as "-";

INSERT INTO ASDW_WRAPUP_SUMMARY_INQUIRY_TOPIC_LOAD (
    INQUIRY_ID,
    TOPIC_SEQ,
    TOPIC_TEXT,
    UPDATED_ON,
    LOAD_TS
)
SELECT
    MD5(RECORD_CONTENT:json_body:conversation_id::VARCHAR || '-' || inq.INDEX::VARCHAR)  AS INQUIRY_ID,
    topic.INDEX + 1                                                         AS TOPIC_SEQ,
    topic.VALUE::VARCHAR                                                    AS TOPIC_TEXT,
    CURRENT_TIMESTAMP(0)                                                    AS UPDATED_ON,
    CURRENT_TIMESTAMP(0)                                                    AS LOAD_TS
FROM ASD_WRAPUP_SUMMARY_RAW,
LATERAL FLATTEN(input => RECORD_CONTENT:json_body:summary:inquiries) inq,
LATERAL FLATTEN(input => inq.VALUE:topics_covered) topic
WHERE RECORD_CONTENT:json_body:conversation_id::VARCHAR IS NOT NULL;