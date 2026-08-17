/**********************************************************************
** Name            : ASD_WRAPUP_SUMMARY_RAW.sql
**
** Description     : DDL for ASD_WRAPUP_SUMMARY_RAW table
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

CALL TOOLS.ADMIN.SP_DROP_OBJECT('TABLE', 'ADMIN.ASD_WRAPUP_SUMMARY_RAW');

CREATE TABLE ASD_WRAPUP_SUMMARY_RAW (
    RECORD_CONTENT VARIANT NOT NULL,
    LOAD_TS TIMESTAMP NOT NULL
);

CALL TOOLS.ADMIN.SP_APPLY_GRANT('GRANT REFERENCES, SELECT ON ASD_WRAPUP_SUMMARY_RAW TO ROLE XXX_RESTRICTED_TABLEREADER');
CALL TOOLS.ADMIN.SP_APPLY_GRANT('GRANT REFERENCES, INSERT, UPDATE, DELETE, TRUNCATE ON ASD_WRAPUP_SUMMARY_RAW TO ROLE XXX_TABLEWRITER');
CALL TOOLS.ADMIN.SP_APPLY_GRANT('GRANT OWNERSHIP ON TABLE ASD_WRAPUP_SUMMARY_RAW TO ROLE XXX_OWNER COPY CURRENT GRANTS');