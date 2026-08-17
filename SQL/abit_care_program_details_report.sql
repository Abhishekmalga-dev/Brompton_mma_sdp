/**********************************************************************
** Name            : ABIT_CARE_PROGRAM_DETAILS_REPORT.sql
**
** Description     : Builds ABIT_CARE_PROGRAM_DETAILS_REPORT from
**                   ABIT_CARE_PROGRAM_DETAILS_REPORT_RAW. Source
**                   columns carry semicolon-delimited multi-values;
**                   this script splits each delimited value onto its
**                   own row via a numbers-table join (nums), casts
**                   date/timestamp columns with format-specific
**                   TRY_TO_TIMESTAMP_NTZ calls, and normalizes the
**                   PROVIDER_OUTREACH_REQUESTED flag.
**
** Developed By    : BI DW DEV
** Date Created    : 08/16/2026
**
** Change History  :
** Mod            Date         Name           Reason
**----------------------------------------------------------------
** PBI_1830398    08/16/2026   Abhi M         Initial
**
**------------------------------------------------------------------
**********************************************************************/
USE SCHEMA ADMIN;

--==========================================================================
--Step 1: Drop existing report table
--==========================================================================

CALL TOOLS.ADMIN.SP_DROP_OBJECT('TABLE', 'ADMIN.ABIT_CARE_PROGRAM_DETAILS_REPORT');

--==========================================================================
--Step 2: Create ABIT_CARE_PROGRAM_DETAILS_REPORT (split/expand/cast from RAW)
--==========================================================================

CREATE TABLE ABIT_CARE_PROGRAM_DETAILS_REPORT AS
WITH src AS (
    SELECT *
    FROM ABIDEV.ADMIN.ABIT_CARE_PROGRAM_DETAILS_REPORT_RAW
),
nums AS (
    SELECT SEQ4() + 1 AS pos
    FROM TABLE(GENERATOR(ROWCOUNT => 50))
)
SELECT
    s.* EXCLUDE (
        "FACILITY_NAME",
        "FACILITY_TYPE",
        "ADMIT_DATE",
        "TARGET_DISCHARGE_DATE",
        "ACTUAL_DISCHARGE_DATE",
        "DISCHARGE_DISPOSITION",
        "DISCHARGE_DELAY_REASON",
        "DATE_PRIMARY_STAFF_ASSIGNED",
        "LAST_PROGRAM_ACTIVITY_DATE",
        "FIRST_ASSESSMENT_SUBMITTED_DATE_ENGAGEMENT_DATE",
        "LAST_ASSESSMENT_SUBMITTED_DATE",
        "ENGAGED_DATE",
        "FIRST_PATIENT_CONTACT_DATE",
        "LAST_PATIENT_CONTACT_DATE",
        "FIRST_CARE_PLAN_DATE",
        "DATE_OF_BIRTH",
        "PROGRAM_CREATE_DATE",
        "PROGRAM_ASSIGNED_DATE",
        "PROGRAM_START_DATE",
        "PROGRAM_CLOSED_DATE",
        "PROVIDER_OUTREACH_REQUESTED",
        "LAST_CMR_SUBMITTED_DATE",
        "WELCOME_PACKET_GENERATED_DATE"
    ),

    -- Semicolon-delimited text splits
    IFF(TRIM(SPLIT_PART(s."FACILITY_NAME", ';', n.pos)) = '', NULL, TRIM(SPLIT_PART(s."FACILITY_NAME", ';', n.pos))) AS "FACILITY_NAME",
    IFF(TRIM(SPLIT_PART(s."FACILITY_TYPE", ';', n.pos)) = '', NULL, TRIM(SPLIT_PART(s."FACILITY_TYPE", ';', n.pos))) AS "FACILITY_TYPE",
    IFF(TRIM(SPLIT_PART(s."DISCHARGE_DISPOSITION", ';', n.pos)) = '', NULL, TRIM(SPLIT_PART(s."DISCHARGE_DISPOSITION", ';', n.pos))) AS "DISCHARGE_DISPOSITION",
    IFF(TRIM(SPLIT_PART(s."DISCHARGE_DELAY_REASON", ';', n.pos)) = '', NULL, TRIM(SPLIT_PART(s."DISCHARGE_DELAY_REASON", ';', n.pos))) AS "DISCHARGE_DELAY_REASON",

    -- Semicolon-delimited date splits
    TRY_TO_TIMESTAMP_NTZ(
        NULLIF(NULLIF(TRIM(SPLIT_PART(s."ADMIT_DATE", ';', n.pos)), ''), 'N/A'),
        'MM/DD/YYYY'
    ) AS "ADMIT_DATE",
    TRY_TO_TIMESTAMP_NTZ(
        NULLIF(NULLIF(TRIM(SPLIT_PART(s."TARGET_DISCHARGE_DATE", ';', n.pos)), ''), 'N/A'),
        'MM/DD/YYYY'
    ) AS "TARGET_DISCHARGE_DATE",
    TRY_TO_TIMESTAMP_NTZ(
        NULLIF(NULLIF(TRIM(SPLIT_PART(s."ACTUAL_DISCHARGE_DATE", ';', n.pos)), ''), 'N/A'),
        'MM/DD/YYYY'
    ) AS "ACTUAL_DISCHARGE_DATE",

    -- Single-valued datetime columns (no splitting)
    TRY_TO_TIMESTAMP_NTZ(
        NULLIF(NULLIF(TRIM(s."DATE_PRIMARY_STAFF_ASSIGNED"), ''), 'N/A'),
        'MM/DD/YYYY HH12:MI:SS AM'
    ) AS "DATE_PRIMARY_STAFF_ASSIGNED",
    TRY_TO_TIMESTAMP_NTZ(
        NULLIF(NULLIF(TRIM(s."LAST_PROGRAM_ACTIVITY_DATE"), ''), 'N/A'),
        'MM/DD/YYYY HH12:MI:SS AM'
    ) AS "LAST_PROGRAM_ACTIVITY_DATE",
    TRY_TO_TIMESTAMP_NTZ(
        NULLIF(NULLIF(TRIM(s."FIRST_ASSESSMENT_SUBMITTED_DATE_ENGAGEMENT_DATE"), ''), 'N/A'),
        'MM/DD/YYYY HH12:MI:SS AM'
    ) AS "FIRST_ASSESSMENT_SUBMITTED_DATE_ENGAGEMENT_DATE",
    TRY_TO_TIMESTAMP_NTZ(
        NULLIF(NULLIF(TRIM(s."LAST_ASSESSMENT_SUBMITTED_DATE"), ''), 'N/A'),
        'MM/DD/YYYY HH12:MI:SS AM'
    ) AS "LAST_ASSESSMENT_SUBMITTED_DATE",
    TRY_TO_TIMESTAMP_NTZ(
        NULLIF(NULLIF(TRIM(s."ENGAGED_DATE"), ''), 'N/A'),
        'MM/DD/YYYY HH12:MI:SS AM'
    ) AS "ENGAGED_DATE",
    TRY_TO_TIMESTAMP_NTZ(
        NULLIF(NULLIF(TRIM(s."FIRST_PATIENT_CONTACT_DATE"), ''), 'N/A'),
        'MM/DD/YYYY HH12:MI:SS AM'
    ) AS "FIRST_PATIENT_CONTACT_DATE",
    TRY_TO_TIMESTAMP_NTZ(
        NULLIF(NULLIF(TRIM(s."LAST_PATIENT_CONTACT_DATE"), ''), 'N/A'),
        'MM/DD/YYYY HH12:MI:SS AM'
    ) AS "LAST_PATIENT_CONTACT_DATE",
    TRY_TO_TIMESTAMP_NTZ(
        NULLIF(NULLIF(TRIM(s."FIRST_CARE_PLAN_DATE"), ''), 'N/A'),
        'MM/DD/YYYY HH12:MI:SS AM'
    ) AS "FIRST_CARE_PLAN_DATE",

    -- Date-only columns (quoted-string cleanup + YYYY-MM-DD format)
    TRY_TO_TIMESTAMP_NTZ(
        NULLIF(NULLIF(REPLACE(TRIM(s."DATE_OF_BIRTH"), '"', ''), ''), 'N/A'),
        'YYYY-MM-DD'
    ) AS "DATE_OF_BIRTH",
    TRY_TO_TIMESTAMP_NTZ(
        NULLIF(NULLIF(REPLACE(TRIM(s."PROGRAM_CREATE_DATE"), '"', ''), ''), 'N/A'),
        'YYYY-MM-DD'
    ) AS "PROGRAM_CREATE_DATE",
    TRY_TO_TIMESTAMP_NTZ(
        NULLIF(NULLIF(TRIM(s."PROGRAM_ASSIGNED_DATE"), ''), 'N/A'),
        'MM/DD/YYYY'
    ) AS "PROGRAM_ASSIGNED_DATE",
    TRY_TO_TIMESTAMP_NTZ(
        NULLIF(NULLIF(REPLACE(TRIM(s."PROGRAM_START_DATE"), '"', ''), ''), 'N/A'),
        'YYYY-MM-DD'
    ) AS "PROGRAM_START_DATE",
    TRY_TO_TIMESTAMP_NTZ(
        NULLIF(NULLIF(REPLACE(TRIM(s."PROGRAM_CLOSED_DATE"), '"', ''), ''), 'N/A'),
        'YYYY-MM-DD'
    ) AS "PROGRAM_CLOSED_DATE",

    -- Boolean-style flag normalization
    IFF(TRIM(s."PROVIDER_OUTREACH_REQUESTED") = 'false', 'No', s."PROVIDER_OUTREACH_REQUESTED"::VARCHAR) AS "PROVIDER_OUTREACH_REQUESTED",

    -- Remaining single-valued date columns
    TRY_TO_TIMESTAMP_NTZ(
        NULLIF(NULLIF(TRIM(s."LAST_CMR_SUBMITTED_DATE"), ''), 'N/A'),
        'MM/DD/YYYY'
    ) AS "LAST_CMR_SUBMITTED_DATE",
    TRY_TO_TIMESTAMP_NTZ(
        NULLIF(NULLIF(TRIM(s."WELCOME_PACKET_GENERATED_DATE"), ''), 'N/A'),
        'MM/DD/YYYY'
    ) AS "WELCOME_PACKET_GENERATED_DATE",

    -- Load timestamp
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS "LOAD_TS"

FROM src s
JOIN nums n
    ON n.pos <= GREATEST(
        1,
        IFF(COALESCE(TRIM(s."FACILITY_NAME"), '') = '', 0, REGEXP_COUNT(s."FACILITY_NAME", ';') + 1),
        IFF(COALESCE(TRIM(s."FACILITY_TYPE"), '') = '', 0, REGEXP_COUNT(s."FACILITY_TYPE", ';') + 1),
        IFF(COALESCE(TRIM(s."ADMIT_DATE"), '') = '', 0, REGEXP_COUNT(s."ADMIT_DATE", ';') + 1),
        IFF(COALESCE(TRIM(s."TARGET_DISCHARGE_DATE"), '') = '', 0, REGEXP_COUNT(s."TARGET_DISCHARGE_DATE", ';') + 1),
        IFF(COALESCE(TRIM(s."ACTUAL_DISCHARGE_DATE"), '') = '', 0, REGEXP_COUNT(s."ACTUAL_DISCHARGE_DATE", ';') + 1),
        IFF(COALESCE(TRIM(s."DISCHARGE_DISPOSITION"), '') = '', 0, REGEXP_COUNT(s."DISCHARGE_DISPOSITION", ';') + 1),
        IFF(COALESCE(TRIM(s."DISCHARGE_DELAY_REASON"), '') = '', 0, REGEXP_COUNT(s."DISCHARGE_DELAY_REASON", ';') + 1)
    );

--==========================================================================
--Step 3: Apply grants
--==========================================================================

CALL TOOLS.ADMIN.SP_APPLY_GRANT('GRANT REFERENCES, SELECT ON ABIT_CARE_PROGRAM_DETAILS_REPORT TO ROLE XXX_RESTRICTED_TABLEREADER');
CALL TOOLS.ADMIN.SP_APPLY_GRANT('GRANT REFERENCES, INSERT, UPDATE, DELETE, TRUNCATE ON ABIT_CARE_PROGRAM_DETAILS_REPORT TO ROLE XXX_TABLEWRITER');
CALL TOOLS.ADMIN.SP_APPLY_GRANT('GRANT OWNERSHIP ON TABLE ABIT_CARE_PROGRAM_DETAILS_REPORT TO ROLE XXX_OWNER COPY CURRENT GRANTS');