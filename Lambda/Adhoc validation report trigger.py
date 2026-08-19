"""
datalake-reconciliation-report-dev (PySpark version)

Reconciles record counts across Raw -> Curated -> Sentiment for all
survey types, explains removed records via quarantine Parquet files,
and flags any records that vanished WITHOUT a quarantine entry
(unaccounted_count).

Reads Parquet natively via Spark (spark.read.parquet), matching the
pattern already used across this project's other Glue jobs -- no
pyarrow dependency, no additional Python modules to install.

Raw JSON is also read via Spark (spark.read.json with multiLine),
except for finding the LATEST file per event_date partition, which
still needs a boto3 listing since Spark has no concept of "most
recently modified file" -- mirrors the get_latest_raw_file() pattern
already used in the survey curation job.

Boto3 is used only for things Spark doesn't do: checking whether a
partition path has any data before attempting a Spark read (avoids
AnalysisException on a missing path), writing the small JSON report +
DynamoDB items, and triggering the Glue crawler.

Job modes (set via job parameters):
    --EVENT_DATE            reconcile exactly one date
    --EVENT_DATES_S3_PATH    reconcile every date listed in
                             event_dates.json (same control file the
                             curation/sentiment jobs already use)
    (neither set)            live mode -- each survey resolves its own
                             latest available date independently

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

args = getResolvedOptions(
    sys.argv,
    ['JOB_NAME', 'ENV', 'EVENT_DATE', 'EVENT_DATES_S3_PATH']
)
# EVENT_DATE and EVENT_DATES_S3_PATH are optional -- getResolvedOptions
# requires them to be listed to be parsed if present, but we treat
# missing/empty values as "not provided" below.
ENV = args.get('ENV', 'dev')
EVENT_DATE_PARAM = args.get('EVENT_DATE', '').strip()
EVENT_DATES_S3_PATH_PARAM = args.get('EVENT_DATES_S3_PATH', '').strip()

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

S3_REPORT_BUCKET = "psegli-datalakenonprodli-datalake-curated-dev"
S3_REPORT_PREFIX = "ccaas/Survey_Reconciliation_Report"

DYNAMODB_TABLE_NAME = "datalake-ccaas-reconciliation-dev"
GLUE_CRAWLER_NAME = "datalake-reconciliation-report-dev"

s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
glue_client = boto3.client("glue")


# =============================================================================
# EVENT DATE INPUT
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
    Decides which date(s) this run covers, based on job parameters:
      1. EVENT_DATE set        -> exactly that one date
      2. EVENT_DATES_S3_PATH set -> every date in the control file
      3. neither set            -> [None], meaning "live mode": each
                                    survey resolves its own latest date
                                    independently inside reconcile_survey()
    """
    if EVENT_DATE_PARAM:
        return [EVENT_DATE_PARAM]
    if EVENT_DATES_S3_PATH_PARAM:
        return get_dates_from_control_file(EVENT_DATES_S3_PATH_PARAM)
    return [None]


# =============================================================================
# S3 / PARQUET / JSON HELPERS
# =============================================================================

def partition_has_data(bucket, prefix):
    """Checks whether an S3 prefix has any objects, before a Spark read."""
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    return response.get("KeyCount", 0) > 0


def get_latest_event_date(bucket, prefix):
    """
    Returns the most recent event_date=YYYY-MM-DD partition folder under
    a prefix, or None. Used only in live mode (no EVENT_DATE / 
    EVENT_DATES_S3_PATH param given).
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


def get_latest_raw_file_key(event_date):
    """
    An event_date=X/ partition in Raw can contain MULTIPLE files, since
    each day's delivery carries a rolling 7-day window and lands in
    every date's own folder it touches. Only the MOST RECENTLY MODIFIED
    file is trusted. Spark has no native notion of "pick the newest
    file by LastModified," so this stays a boto3 listing, mirroring
    get_latest_raw_file() from the survey curation job.
    """
    prefix = f"{RAW_PREFIX}/event_date={event_date}/"
    paginator = s3_client.get_paginator("list_objects_v2")

    latest_key = None
    latest_modified = None
    for page in paginator.paginate(Bucket=RAW_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if not obj["Key"].endswith(".json"):
                continue
            if latest_modified is None or obj["LastModified"] > latest_modified:
                latest_modified = obj["LastModified"]
                latest_key = obj["Key"]
    return latest_key


def count_all_raw_survey_records(event_date):
    """
    Reads ONLY the most recently modified Raw JSON file for event_date,
    via Spark, and counts records per survey type -- all in one pass.

    Even the single latest file spans a rolling ~7-day window
    internally, so each record is only counted toward event_date if its
    own "Invitation Date" falls on that exact day. Records with a
    missing/blank Invitation Date are excluded (not counted toward any
    date), rather than guessed at.

    Returns a dict: {raw_survey_name_lowercase: count}
    """
    latest_key = get_latest_raw_file_key(event_date)
    if latest_key is None:
        return {}

    raw_path = f"s3://{RAW_BUCKET}/{latest_key}"
    # The Raw file's top level is a single flat JSON array, not
    # newline-delimited JSON -- multiLine is required for Spark to
    # parse it correctly.
    raw_df = spark.read.option("multiLine", "true").json(raw_path)

    filtered_df = (
        raw_df
        .withColumn("_invitation_date_str", F.substring(F.col(RAW_INVITATION_DATE_FIELD), 1, 10))
        .filter(F.col(RAW_INVITATION_DATE_FIELD).isNotNull())
        .filter(F.trim(F.col(RAW_INVITATION_DATE_FIELD)) != "")
        .filter(F.col("_invitation_date_str") == event_date)
        .withColumn("_survey_name_lower", F.lower(F.trim(F.col(RAW_SURVEY_TYPE_FIELD))))
    )

    rows = (
        filtered_df
        .groupBy("_survey_name_lower")
        .count()
        .collect()
    )

    return {row["_survey_name_lower"]: row["count"] for row in rows}


def read_parquet_df(bucket, prefix):
    """Reads a Parquet partition via Spark, or None if the path has no data."""
    if not partition_has_data(bucket, prefix):
        return None
    return spark.read.parquet(f"s3://{bucket}/{prefix}")


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
        "sentiment_count": "N/A",
        "removed_count": "N/A",
        "unaccounted_count": "N/A",
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
    quarantine_df = read_parquet_df(CURATED_BUCKET, quarantine_prefix)

    curated_count = count_df_rows(curated_df)
    sentiment_count = count_df_rows(sentiment_df)

    raw_curated_mismatch = raw_count != curated_count
    if raw_curated_mismatch:
        logger.warning(
            f"{survey_name} {event_date}: RAW/CURATED MISMATCH "
            f"(raw={raw_count}, curated={curated_count})."
        )

    # --- Anti-join via Spark's subtract(), instead of Python set() diff ---
    if curated_df is not None and sentiment_df is not None:
        missing_ids_df = (
            curated_df.select(CONTACT_RECORD_ID_COLUMN).distinct()
            .subtract(sentiment_df.select(CONTACT_RECORD_ID_COLUMN).distinct())
        )
        missing_ids = [row[CONTACT_RECORD_ID_COLUMN] for row in missing_ids_df.collect()]
    elif curated_df is not None:
        # Sentiment has no data at all for this date -- everything in
        # Curated is "missing" from Sentiment.
        missing_ids = [
            row[CONTACT_RECORD_ID_COLUMN]
            for row in curated_df.select(CONTACT_RECORD_ID_COLUMN).distinct().collect()
        ]
    else:
        missing_ids = []

    # --- Explain missing IDs via quarantine ---
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
        # Live mode: resolve each survey's own latest available date.
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
    event_date and survey_name are deliberately EXCLUDED from the JSON
    body -- they're already encoded in the S3 partition path, and
    including them in both places causes the Glue Crawler to register
    duplicate columns (Trino/Starburst rejects this outright).
    """
    is_active = summary["pipeline_status"] == "ACTIVE"
    subfolder = "summary_active" if is_active else "summary_raw_only"

    key = (
        f"{S3_REPORT_PREFIX}/{subfolder}/"
        f"event_date={summary['event_date']}/"
        f"survey_name={summary['survey_name']}/"
        f"report.json"
    )

    reason_breakdown_list = [
        {"removal_reason": reason, "record_count": count}
        for reason, count in summary.get("reason_breakdown", {}).items()
    ]

    record = dict(summary)
    record["reason_breakdown"] = reason_breakdown_list
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