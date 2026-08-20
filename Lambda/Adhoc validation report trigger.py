"""
datalake-ccaas-reconciliation-report-dev (PySpark version)

Reconciles record counts across Raw -> Curated -> Sentiment for all
survey types, explains removed records via quarantine CSV files, and
flags any records that vanished WITHOUT a quarantine entry
(unaccounted_count).

Reads Curated/Sentiment Parquet natively via Spark (spark.read.parquet).
Quarantine (removed_records) is CSV, not Parquet, and is read via
spark.read.csv().

RAW COUNTING (IMPORTANT): Raw is repeatedly re-ingested, and a given
event_date=X/ folder can accumulate MULTIPLE files over time (each
delivery carries a rolling ~7-day window). A record for event_date X
can end up sitting in an OLDER file if a later re-ingestion's window
didn't happen to include it again -- picking only the single
most-recently-modified file can silently MISS records that are only
present in an earlier file (confirmed case: IVR records for a specific
date were present in an older file but absent from the latest
re-ingested one).

To handle this correctly: read EVERY .json file in the event_date
folder, filter each by Invitation Date == event_date, combine all
filtered results together, then DEDUPLICATE by Contact Record ID across
the combined set. This is safe against BOTH failure modes -- missing
records that only live in an older file, and double-counting records
that appear in more than one overlapping re-ingested file.

Output: ONE unified table (data_reconciliation_report), not split
active/raw_only. Raw-only surveys write Python None (-> JSON null) for
sentiment_count/removed_count/unaccounted_count, keeping those columns
a proper nullable int in the crawled schema. reason_breakdown is a
single human-readable STRING (e.g. "DEDUP_CASE_2: 4", or "NONE"), not
an array of structs -- avoids array/struct schema-inference conflicts
and is directly readable without unnesting.

Job modes (set via job parameters):
    --EVENT_DATE             reconcile exactly one date
    --EVENT_DATES_S3_PATH    reconcile every date listed in
                              event_dates.json (same control file the
                              curation/sentiment jobs already use)
    (neither set)             live mode -- each survey resolves its own
                              latest available date independently

Both EVENT_DATE and EVENT_DATES_S3_PATH are OPTIONAL and read manually
from sys.argv (see _get_optional_arg), since getResolvedOptions treats
every name passed to it as REQUIRED.

Starburst never appears in this script. It only ever sees this
pipeline's OUTPUT, via the Glue Crawler triggered at the end.
"""

import sys
import boto3
import json
import logging
from datetime import datetime, timezone
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
import pyspark.sql.functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =============================================================================
# JOB SETUP
# =============================================================================

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'ENV'])
ENV = args.get('ENV', 'dev')


def _get_optional_arg(name):
    """
    Manually checks sys.argv for an optional --NAME value, since
    getResolvedOptions can't express "required if present, fine if
    absent" -- it only knows "must be present."
    """
    flag = f"--{name}"
    if flag in sys.argv:
        idx = sys.argv.index(flag)
        if idx + 1 < len(sys.argv):
            return sys.argv[idx + 1].strip()
    return ""


EVENT_DATE_PARAM = _get_optional_arg('EVENT_DATE')
EVENT_DATES_S3_PATH_PARAM = _get_optional_arg('EVENT_DATES_S3_PATH')

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# =============================================================================
# CONFIG
# =============================================================================

RAW_BUCKET = "psegli-datalakenonprodli-datalake-raw-dev"
RAW_PREFIX = "ccaas/survey_api_json"

CURATED_BUCKET = "psegli-datalakenonprodli-datalake-curated-dev"
CURATED_PREFIX_BASE = "ccaas"

SENTIMENT_PREFIX_BASE = "sentiment_analysis/final"
QUARANTINE_PREFIX_BASE = "sentiment_analysis/removed_records"

EVENT_DATES_BUCKET = "psegli-datalakenonprodli-datalake-temp-dev"
EVENT_DATES_KEY = "sentiment_analysis/event_date/event_dates.json"

RAW_SURVEY_TYPE_FIELD = "Survey Name"
RAW_CONTACT_ID_FIELD = "Contact Record ID"
RAW_INVITATION_DATE_FIELD = "Invitation Date"

CONTACT_RECORD_ID_COLUMN = "contact_record_id"
REMOVAL_REASON_COLUMN = "removal_reason"

SURVEY_CONFIGS = [
    {
        "survey_name": "IVR",
        "raw_survey_name": "customer sat ivr survey",
        "curated_folder": "survey_customer_sat_ivr",
        "sentiment_folder": "ivr",
        "quarantine_folder": "ivr_quarantine",
        "sentiment_pipeline_active": True,
    },
    {
        "survey_name": "API_REL",
        "raw_survey_name": "api - web relational survey",
        "curated_folder": "survey_api_web_relational",
        "sentiment_folder": "api_relational",
        "quarantine_folder": "api_relational_quarantine",
        "sentiment_pipeline_active": True,
    },
    {
        "survey_name": "API_TXN",
        "raw_survey_name": "api - web transactional survey",
        "curated_folder": "survey_api_web_transactional",
        "sentiment_folder": "api_transactional",
        "quarantine_folder": "api_transactional_quarantine",
        "sentiment_pipeline_active": True,
    },
    {
        "survey_name": "SMS_REL",
        "raw_survey_name": "sms - api web relational survey",
        "curated_folder": "survey_sms_web_relational",
        "sentiment_folder": "sms_relational",
        "quarantine_folder": "sms_relational_quarantine",
        "sentiment_pipeline_active": True,
    },
    {
        "survey_name": "SMS_TXN",
        "raw_survey_name": "sms - api web transactional survey",
        "curated_folder": "survey_sms_web_transactional",
        "sentiment_folder": "sms_transactional",
        "quarantine_folder": "sms_transactional_quarantine",
        "sentiment_pipeline_active": True,
    },
    {
        "survey_name": "CUSTOMER_REP_SAT_V2",
        "raw_survey_name": "customer rep sat survey v2",
        "curated_folder": "survey_customer_rep_sat_v2",
        "sentiment_folder": None,
        "quarantine_folder": None,
        "sentiment_pipeline_active": False,
    },
    {
        "survey_name": "CUSTOMER_SAT_EMAIL",
        "raw_survey_name": "customer sat email survey",
        "curated_folder": "survey_customer_sat_email",
        "sentiment_folder": None,
        "quarantine_folder": None,
        "sentiment_pipeline_active": False,
    },
]

# Report storage -- single unified table, named data_reconciliation_report.
S3_REPORT_BUCKET = "psegli-datalakenonprodli-datalake-curated-dev"
S3_REPORT_PREFIX = "ccaas"
REPORT_TABLE_FOLDER = "data_reconciliation_report"

DYNAMODB_TABLE_NAME = "datalake-ccaas-reconciliation-dev"
GLUE_CRAWLER_NAME = "datalake-reconciliation-report-dev"

s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
glue_client = boto3.client("glue")


# =============================================================================
# EVENT DATE DISCOVERY
# =============================================================================

def get_dates_from_control_file(s3_path):
    """Reads event_dates.json (same control file the other Glue jobs use)."""
    path = s3_path.replace("s3://", "")
    bucket, key = path.split("/", 1)
    response = s3_client.get_object(Bucket=bucket, Key=key)
    content = json.loads(response["Body"].read())
    date_lists = list(content.values())
    return sorted(set(date_lists[0])) if date_lists else []


def resolve_dates_to_process():
    """
    1. EVENT_DATE set          -> exactly that one date
    2. EVENT_DATES_S3_PATH set -> every date in the control file
    3. neither set              -> [None] (live mode)
    """
    if EVENT_DATE_PARAM:
        return [EVENT_DATE_PARAM]
    if EVENT_DATES_S3_PATH_PARAM:
        return get_dates_from_control_file(EVENT_DATES_S3_PATH_PARAM)
    return [None]


# =============================================================================
# S3 / PARQUET / CSV / JSON HELPERS
# =============================================================================

def partition_has_data(bucket, prefix):
    """Checks whether an S3 prefix has any objects, before a Spark read."""
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    return response.get("KeyCount", 0) > 0


def get_latest_event_date(bucket, prefix):
    """
    Returns the most recent event_date=YYYY-MM-DD partition folder under
    a prefix, or None. Used only in live mode.
    """
    if not prefix.endswith("/"):
        prefix += "/"
    paginator = s3_client.get_paginator("list_objects_v2")
    dates = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            folder_name = cp["Prefix"].rstrip("/").split("/")[-1]
            if folder_name.startswith("event_date="):
                dates.append(folder_name.replace("event_date=", ""))
    return sorted(dates)[-1] if dates else None


def count_all_raw_survey_records(event_date):
    """
    Reads EVERY Raw JSON file in this event_date's folder (not just the
    most recently modified one), filters each by Invitation Date ==
    event_date, combines all filtered results, then DEDUPLICATES by
    Contact Record ID across all files combined.

    Safe against both failure modes: a record only present in an older
    file (missed by "latest file only"), and a record present in
    multiple overlapping re-ingested files (would be double-counted by
    a naive sum across files).

    Returns a dict: {raw_survey_name_lowercase: distinct_count}
    """
    prefix = f"{RAW_PREFIX}/event_date={event_date}/"
    paginator = s3_client.get_paginator("list_objects_v2")

    json_keys = []
    for page in paginator.paginate(Bucket=RAW_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                json_keys.append(obj["Key"])

    if not json_keys:
        logger.warning(f"No Raw files found at all for event_date={event_date}")
        return {}

    logger.info(f"event_date={event_date}: found {len(json_keys)} Raw file(s) to combine: {json_keys}")

    combined_df = None

    for key in json_keys:
        raw_path = f"s3://{RAW_BUCKET}/{key}"
        try:
            raw_df = spark.read.option("multiLine", "true").json(raw_path)

            filtered_df = (
                raw_df
                .withColumn("_invitation_date_str", F.substring(F.col(RAW_INVITATION_DATE_FIELD), 1, 10))
                .filter(F.col(RAW_INVITATION_DATE_FIELD).isNotNull())
                .filter(F.trim(F.col(RAW_INVITATION_DATE_FIELD)) != "")
                .filter(F.col("_invitation_date_str") == event_date)
                .withColumn("_survey_name_lower", F.lower(F.trim(F.col(RAW_SURVEY_TYPE_FIELD))))
                .select(
                    F.col(RAW_CONTACT_ID_FIELD).alias("_contact_id"),
                    "_survey_name_lower"
                )
            )

            combined_df = filtered_df if combined_df is None else combined_df.unionByName(filtered_df)

        except Exception as e:
            logger.error(f"RAW READ FAILED for event_date={event_date}, path={raw_path}: {e}")
            raise

    if combined_df is None:
        return {}

    distinct_df = combined_df.dropDuplicates(["_contact_id"])

    rows = distinct_df.groupBy("_survey_name_lower").count().collect()
    result = {row["_survey_name_lower"]: row["count"] for row in rows}

    logger.info(f"event_date={event_date}: distinct survey_name keys/counts = {result}")

    return result


def read_parquet_df(bucket, prefix):
    """
    Reads a Curated/Sentiment Parquet partition via Spark, restricted to
    *.parquet files only, so stray non-Parquet files in the same folder
    don't cause CANNOT_READ_FILE_FOOTER.
    Returns None if the partition has no matching Parquet files.
    """
    if not partition_has_data(bucket, prefix):
        return None

    full_prefix = f"s3://{bucket}/{prefix}"
    glob_path = full_prefix.rstrip("/") + "/*.parquet"

    try:
        return spark.read.parquet(glob_path)
    except Exception as e:
        logger.warning(f"No readable Parquet files found at {glob_path}: {e}")
        return None


def read_csv_df(bucket, prefix):
    """
    Reads quarantine (removed_records) data via Spark's CSV reader.
    CONFIRMED: quarantine is written as CSV, not Parquet.
    Returns None if the partition has no data.
    """
    if not partition_has_data(bucket, prefix):
        return None

    full_prefix = f"s3://{bucket}/{prefix}"
    return spark.read.option("header", "true").csv(full_prefix)


def count_df_rows(df):
    return df.count() if df is not None else 0


# =============================================================================
# DYNAMODB HELPERS
# =============================================================================

def already_reconciled(survey_name, event_date):
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)
    response = table.get_item(
        Key={"event_date": event_date, "survey_type": survey_name}
    )
    return "Item" in response


def write_dynamodb_summary(summary):
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)
    table.put_item(
        Item={
            "event_date": summary["event_date"],
            "survey_type": summary["survey_name"],
            "raw_count": summary["raw_count"],
            "curated_count": summary["curated_count"],
            "sentiment_count": summary["sentiment_count"],
            "removed_count": summary["removed_count"],
            "unaccounted_count": summary["unaccounted_count"],
            "raw_curated_mismatch": summary["raw_curated_mismatch"],
            "pipeline_status": summary.get("pipeline_status", "ACTIVE"),
            "s3_report_path": summary["s3_report_path"],
            "generated_at": summary["generated_at"],
        }
    )


# =============================================================================
# CORE RECONCILIATION LOGIC
# =============================================================================

def reconcile_raw_only_survey(survey_config, event_date, raw_count):
    """
    Handles surveys not yet wired into Sentiment. sentiment_count/
    removed_count/unaccounted_count are Python None (-> JSON null), NOT
    the string "N/A" -- keeps those columns a proper nullable int in
    the unified table's crawled schema.
    """
    survey_name = survey_config["survey_name"]
    curated_folder = survey_config["curated_folder"]

    curated_prefix = f"{CURATED_PREFIX_BASE}/{curated_folder}/event_date={event_date}/"
    curated_df = read_parquet_df(CURATED_BUCKET, curated_prefix)
    curated_count = count_df_rows(curated_df)

    raw_curated_mismatch = raw_count != curated_count
    if raw_curated_mismatch:
        logger.warning(
            f"{survey_name} {event_date}: RAW/CURATED MISMATCH "
            f"(raw={raw_count}, curated={curated_count})."
        )

    generated_at = datetime.now(timezone.utc).isoformat()

    summary = {
        "event_date": event_date,
        "survey_name": survey_name,
        "raw_count": raw_count,
        "curated_count": curated_count,
        "raw_curated_mismatch": raw_curated_mismatch,
        "sentiment_count": None,
        "removed_count": None,
        "unaccounted_count": None,
        "reason_breakdown": {},
        "pipeline_status": "RAW_ONLY",
        "generated_at": generated_at,
    }

    s3_path = write_summary_to_s3(summary)
    summary["s3_report_path"] = s3_path
    write_dynamodb_summary(summary)

    return summary


def reconcile_active_survey(survey_config, event_date, raw_count):
    survey_name = survey_config["survey_name"]
    curated_folder = survey_config["curated_folder"]
    sentiment_folder = survey_config["sentiment_folder"]
    quarantine_folder = survey_config["quarantine_folder"]

    curated_prefix = f"{CURATED_PREFIX_BASE}/{curated_folder}/event_date={event_date}/"
    sentiment_prefix = f"{SENTIMENT_PREFIX_BASE}/{sentiment_folder}/event_date={event_date}/"
    quarantine_prefix = f"{QUARANTINE_PREFIX_BASE}/{quarantine_folder}/event_date={event_date}/"

    curated_df = read_parquet_df(CURATED_BUCKET, curated_prefix)
    sentiment_df = read_parquet_df(CURATED_BUCKET, sentiment_prefix)
    quarantine_df = read_csv_df(CURATED_BUCKET, quarantine_prefix)

    curated_count = count_df_rows(curated_df)
    sentiment_count = count_df_rows(sentiment_df)

    raw_curated_mismatch = raw_count != curated_count
    if raw_curated_mismatch:
        logger.warning(
            f"{survey_name} {event_date}: RAW/CURATED MISMATCH "
            f"(raw={raw_count}, curated={curated_count})."
        )

    if curated_df is not None and sentiment_df is not None:
        missing_ids_df = (
            curated_df.select(CONTACT_RECORD_ID_COLUMN).distinct()
            .subtract(sentiment_df.select(CONTACT_RECORD_ID_COLUMN).distinct())
        )
        missing_ids = [row[CONTACT_RECORD_ID_COLUMN] for row in missing_ids_df.collect()]
    elif curated_df is not None:
        missing_ids = [
            row[CONTACT_RECORD_ID_COLUMN]
            for row in curated_df.select(CONTACT_RECORD_ID_COLUMN).distinct().collect()
        ]
    else:
        missing_ids = []

    if quarantine_df is not None and missing_ids:
        quarantine_rows = (
            quarantine_df
            .select(CONTACT_RECORD_ID_COLUMN, REMOVAL_REASON_COLUMN)
            .filter(F.col(CONTACT_RECORD_ID_COLUMN).isin(missing_ids))
            .collect()
        )
        quarantine_reasons = {
            row[CONTACT_RECORD_ID_COLUMN]: row[REMOVAL_REASON_COLUMN] for row in quarantine_rows
        }
    else:
        quarantine_reasons = {}

    removed_count = len(missing_ids)
    unaccounted_ids = [rid for rid in missing_ids if rid not in quarantine_reasons]
    unaccounted_count = len(unaccounted_ids)

    reason_breakdown = {}
    for rid in missing_ids:
        reason = quarantine_reasons.get(rid, "UNACCOUNTED")
        reason_breakdown[reason] = reason_breakdown.get(reason, 0) + 1

    generated_at = datetime.now(timezone.utc).isoformat()

    summary = {
        "event_date": event_date,
        "survey_name": survey_name,
        "raw_count": raw_count,
        "curated_count": curated_count,
        "raw_curated_mismatch": raw_curated_mismatch,
        "sentiment_count": sentiment_count,
        "removed_count": removed_count,
        "unaccounted_count": unaccounted_count,
        "reason_breakdown": reason_breakdown,
        "pipeline_status": "ACTIVE",
        "generated_at": generated_at,
    }

    s3_path = write_summary_to_s3(summary)
    summary["s3_report_path"] = s3_path

    if unaccounted_count > 0:
        write_anomaly_detail_to_s3(survey_name, event_date, unaccounted_ids, generated_at)
        logger.warning(
            f"{survey_name} {event_date}: {unaccounted_count} records unaccounted for."
        )

    write_dynamodb_summary(summary)

    return summary


def reconcile_survey(survey_config, event_date_override, get_raw_counts_for_date):
    survey_name = survey_config["survey_name"]
    curated_folder = survey_config["curated_folder"]
    sentiment_active = survey_config["sentiment_pipeline_active"]

    if event_date_override:
        event_date = event_date_override
    else:
        if sentiment_active:
            date_source_prefix = f"{SENTIMENT_PREFIX_BASE}/{survey_config['sentiment_folder']}"
        else:
            date_source_prefix = f"{CURATED_PREFIX_BASE}/{curated_folder}"
        event_date = get_latest_event_date(CURATED_BUCKET, date_source_prefix)

        if event_date is None:
            logger.info(f"{survey_name}: no data found under {date_source_prefix}, skipping.")
            return None

    if already_reconciled(survey_name, event_date):
        logger.info(f"{survey_name}: {event_date} already reconciled, skipping.")
        return None

    logger.info(f"{survey_name}: reconciling event_date={event_date}")

    raw_counts = get_raw_counts_for_date(event_date)
    raw_count = raw_counts.get(survey_config["raw_survey_name"].strip().lower(), 0)

    if sentiment_active:
        return reconcile_active_survey(survey_config, event_date, raw_count)
    else:
        return reconcile_raw_only_survey(survey_config, event_date, raw_count)


# =============================================================================
# OUTPUT WRITERS
# =============================================================================

def write_summary_to_s3(summary):
    """
    Writes to a SINGLE unified table folder (data_reconciliation_report).
    event_date and survey_name are excluded from the JSON body -- already
    encoded in the S3 partition path.

    reason_breakdown is a plain STRING (e.g. "DEDUP_CASE_2: 4", or
    "NONE"), not an array of structs.
    """
    key = (
        f"{S3_REPORT_PREFIX}/{REPORT_TABLE_FOLDER}/"
        f"event_date={summary['event_date']}/"
        f"survey_name={summary['survey_name']}/"
        f"report.json"
    )

    reason_breakdown_source = summary.get("reason_breakdown", {})
    if reason_breakdown_source:
        reason_breakdown_str = ", ".join(
            f"{reason}: {count}" for reason, count in reason_breakdown_source.items()
        )
    else:
        reason_breakdown_str = "NONE"

    record = dict(summary)
    record["reason_breakdown"] = reason_breakdown_str
    record.pop("event_date", None)
    record.pop("survey_name", None)

    s3_client.put_object(
        Bucket=S3_REPORT_BUCKET,
        Key=key,
        Body=json.dumps(record, default=str),
        ContentType="application/json",
    )
    return f"s3://{S3_REPORT_BUCKET}/{key}"


def write_anomaly_detail_to_s3(survey_name, event_date, unaccounted_ids, generated_at):
    key = (
        f"{S3_REPORT_PREFIX}/anomaly_detail/"
        f"event_date={event_date}/"
        f"survey_name={survey_name}/"
        f"detail.json"
    )
    detail = {
        "generated_at": generated_at,
        "unaccounted_contact_record_ids": unaccounted_ids,
    }
    s3_client.put_object(
        Bucket=S3_REPORT_BUCKET,
        Key=key,
        Body=json.dumps(detail, default=str),
        ContentType="application/json",
    )


def trigger_glue_crawler():
    if not GLUE_CRAWLER_NAME:
        return
    try:
        glue_client.start_crawler(Name=GLUE_CRAWLER_NAME)
        logger.info(f"Started Glue crawler: {GLUE_CRAWLER_NAME}")
    except glue_client.exceptions.CrawlerRunningException:
        logger.info(f"Glue crawler {GLUE_CRAWLER_NAME} already running, skipping trigger.")
    except Exception as e:
        logger.error(f"Failed to start Glue crawler: {e}")
        raise


# =============================================================================
# MAIN
# =============================================================================

def main():
    dates_to_process = resolve_dates_to_process()
    logger.info(f"Processing {len(dates_to_process)} date(s): mode determined by job parameters.")

    raw_counts_cache = {}

    def get_raw_counts_for_date(event_date):
        if event_date not in raw_counts_cache:
            raw_counts_cache[event_date] = count_all_raw_survey_records(event_date)
        return raw_counts_cache[event_date]

    total_reconciled = 0
    total_skipped = 0
    total_errors = 0

    for i, event_date_override in enumerate(dates_to_process, 1):
        logger.info(f"[{i}/{len(dates_to_process)}] event_date_override={event_date_override}")

        for survey_config in SURVEY_CONFIGS:
            survey_name = survey_config["survey_name"]
            try:
                summary = reconcile_survey(survey_config, event_date_override, get_raw_counts_for_date)
                if summary is None:
                    total_skipped += 1
                else:
                    total_reconciled += 1
            except Exception as e:
                logger.error(f"FAILED: {survey_name} on {event_date_override}: {e}")
                total_errors += 1
                continue

    logger.info(
        f"Reconciliation complete. Reconciled: {total_reconciled}, "
        f"Skipped: {total_skipped}, Errors: {total_errors}"
    )

    if total_reconciled > 0:
        trigger_glue_crawler()

    job.commit()

    if total_errors > 0:
        raise RuntimeError(f"{total_errors} reconciliation(s) failed. Check job logs.")


if __name__ == "__main__":
    main()