"""
datalake-ccaas-reconciliation-report-dev (PySpark version)

Reconciles record counts across Raw -> Curated -> Sentiment -> Dashboard
for all survey types, explains removed records via quarantine CSV
files, and flags any records that vanished WITHOUT a quarantine entry
(unaccounted_count).

Reads Curated/Sentiment Parquet natively via Spark (spark.read.parquet).
Quarantine (removed_records) is CSV, read via spark.read.csv().
Dashboard output is a single, always-overwritten CSV per survey --
filtered by its own raw_invitation_date column (MIXED "M/d/yyyy H:mm"
and "yyyy-MM-dd HH:mm:ss" formats, handled via coalesce()) and
deduplicated by raw_contact_record_id.

RAW COUNTING: Deliveries only happen on SOME days, and each delivery's
file spans a rolling ~7-day window of Invitation Dates. If NO delivery
landed on a given event_date itself, records for that date can still
exist -- filed under a LATER delivery's folder. build_raw_counts_for_
date_range() reads every Raw delivery folder across the FULL target
date range EXACTLY ONCE, tags every record by its own Invitation Date,
and returns a single lookup -- avoiding re-reading the same overlapping
delivery file once per target date. Live mode falls back to a lazy,
per-date read since dates aren't known upfront there.

FORCE RERUN: already_reconciled() normally skips any (survey, date)
combo that already has a DynamoDB row, regardless of whether the
underlying data or the reconciliation logic changed since that row was
written. Setting --FORCE_RERUN true bypasses this skip for the current
run's date list, so already-reconciled dates get recomputed and
overwritten instead of skipped. Use this when refreshing dates after a
data reload or a logic fix, when hand-deleting the specific affected
DynamoDB rows isn't practical. Downsides: recomputes EVERY date in the
current run's list (not just the ones that actually need it), costs
full compute even for dates that were already correct, and has no
per-survey granularity -- for a small number of known-affected dates/
surveys, deleting just those specific DynamoDB rows before a normal run
is cheaper and more surgical than a blanket force rerun.

Output: ONE unified table (data_reconciliation_report). Raw-only
surveys write Python None (-> JSON null) for sentiment_count/
dashboard_count/removed_count/unaccounted_count. reason_breakdown is a
single human-readable STRING (e.g. "DEDUP_CASE_2: 4", or "NONE").

Job modes (set via job parameters):
    --EVENT_DATE             reconcile exactly one date
    --EVENT_DATES_S3_PATH    reconcile every date listed in
                              event_dates.json
    (neither set)             live mode -- each survey resolves its own
                              latest available date independently
    --FORCE_RERUN true        (optional, any mode) bypass the
                              already-reconciled skip and recompute
                              every date in this run's list

Starburst never appears in this script. It only ever sees this
pipeline's OUTPUT, via the Glue Crawler triggered at the end.
"""

import sys
import boto3
import json
import logging
from datetime import datetime, timedelta, timezone
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
FORCE_RERUN_PARAM = _get_optional_arg('FORCE_RERUN').lower() == 'true'

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

RAW_LOOKBACK_WINDOW_DAYS = 7

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

DASHBOARD_BUCKET = "psegli-datalakenonprodli-datalake-temp-dev"
DASHBOARD_PREFIX = "Sentiment_Analysis/dashboard_output"

RAW_DASHBOARD_CONTACT_ID_FIELD = "raw_contact_record_id"
RAW_DASHBOARD_INVITATION_DATE_FIELD = "raw_invitation_date"

DASHBOARD_FILE_MAP = {
    "IVR": "ivr_final_output_all_columns_dashboard.csv",
    "API_REL": "api_web_relational_final_output_all_columns_dashboard.csv",
    "API_TXN": "api_web_transactional_final_output_all_columns_dashboard.csv",
    "SMS_REL": "sms_web_relational_final_output_all_columns_dashboard.csv",
    "SMS_TXN": "sms_web_transactional_final_output_all_columns_dashboard.csv",
}

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

def partition_has_data(bucket, prefix_or_key):
    """
    Checks whether an S3 prefix (or exact key) has any objects, before
    a Spark read. Works identically for a partition prefix or a single
    flat file key.
    """
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix_or_key, MaxKeys=1)
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


def _list_raw_json_keys_for_folder(folder_date):
    """Lists all .json object keys under one Raw event_date=X/ folder."""
    prefix = f"{RAW_PREFIX}/event_date={folder_date}/"
    paginator = s3_client.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=RAW_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                keys.append(obj["Key"])
    return keys


def build_raw_counts_for_date_range(all_target_dates):
    """
    Reads every Raw delivery folder covering the full target date range
    EXACTLY ONCE, tags every record by its own Invitation Date, and
    returns a lookup keyed by (event_date, survey_name_lower) ->
    distinct_count.

    Returns: dict of {(event_date, survey_name_lower): distinct_count}
    """
    if not all_target_dates:
        return {}

    folders_to_read = set()
    for target_date in all_target_dates:
        base = datetime.strptime(target_date, "%Y-%m-%d")
        for offset in range(0, RAW_LOOKBACK_WINDOW_DAYS + 1):
            folders_to_read.add((base + timedelta(days=offset)).strftime("%Y-%m-%d"))

    logger.info(
        f"Reading {len(folders_to_read)} Raw delivery folder(s) once each, "
        f"covering {len(all_target_dates)} target date(s)."
    )

    all_json_keys = []
    for folder_date in sorted(folders_to_read):
        all_json_keys.extend(_list_raw_json_keys_for_folder(folder_date))

    if not all_json_keys:
        logger.warning("No Raw files found across the entire target date range.")
        return {}

    logger.info(f"Found {len(all_json_keys)} total Raw file(s) to read once each.")

    combined_df = None
    target_dates_set = set(all_target_dates)

    for key in all_json_keys:
        raw_path = f"s3://{RAW_BUCKET}/{key}"
        try:
            raw_df = spark.read.option("multiLine", "true").json(raw_path)

            tagged_df = (
                raw_df
                .withColumn("_invitation_date_str", F.substring(F.col(RAW_INVITATION_DATE_FIELD), 1, 10))
                .filter(F.col(RAW_INVITATION_DATE_FIELD).isNotNull())
                .filter(F.trim(F.col(RAW_INVITATION_DATE_FIELD)) != "")
                .filter(F.col("_invitation_date_str").isin(list(target_dates_set)))
                .withColumn("_survey_name_lower", F.lower(F.trim(F.col(RAW_SURVEY_TYPE_FIELD))))
                .select(
                    F.col(RAW_CONTACT_ID_FIELD).alias("_contact_id"),
                    "_survey_name_lower",
                    F.col("_invitation_date_str").alias("_event_date")
                )
            )

            combined_df = tagged_df if combined_df is None else combined_df.unionByName(tagged_df)

        except Exception as e:
            logger.error(f"RAW READ FAILED for path={raw_path}: {e}")
            raise

    if combined_df is None:
        return {}

    distinct_df = combined_df.dropDuplicates(["_contact_id"])

    rows = distinct_df.groupBy("_event_date", "_survey_name_lower").count().collect()
    result = {(row["_event_date"], row["_survey_name_lower"]): row["count"] for row in rows}

    logger.info(f"Built raw count lookup for {len(result)} (event_date, survey) combinations.")

    return result


def count_all_raw_survey_records_single_date(event_date):
    """
    Fallback for LIVE mode only, where dates aren't known upfront so the
    bulk lookup can't be pre-built. Searches the RAW_LOOKBACK_WINDOW_DAYS
    window for just this one date, on demand.

    Returns a dict: {raw_survey_name_lowercase: distinct_count}
    """
    base = datetime.strptime(event_date, "%Y-%m-%d")
    folder_dates = [
        (base + timedelta(days=offset)).strftime("%Y-%m-%d")
        for offset in range(0, RAW_LOOKBACK_WINDOW_DAYS + 1)
    ]

    json_keys = []
    for folder_date in folder_dates:
        json_keys.extend(_list_raw_json_keys_for_folder(folder_date))

    if not json_keys:
        logger.warning(f"No Raw files found for event_date={event_date} across lookback window {folder_dates}")
        return {}

    logger.info(f"event_date={event_date}: found {len(json_keys)} Raw file(s) across lookback window")

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


def count_dashboard_records(survey_name, event_date):
    """
    Reads the single, always-overwritten dashboard CSV for this survey,
    filters by raw_invitation_date == event_date, and counts DISTINCT
    raw_contact_record_id.

    raw_invitation_date contains a MIX of "M/d/yyyy H:mm" and
    "yyyy-MM-dd HH:mm:ss" formats within the SAME column. coalesce()
    tries both patterns and takes whichever one actually parses.

    Returns the distinct count as an int, or None if the file doesn't
    exist yet for this survey.
    """
    filename = DASHBOARD_FILE_MAP.get(survey_name)
    if filename is None:
        return None

    key = f"{DASHBOARD_PREFIX}/{filename}"

    if not partition_has_data(DASHBOARD_BUCKET, key):
        logger.warning(f"{survey_name}: dashboard file not found at s3://{DASHBOARD_BUCKET}/{key}")
        return None

    dashboard_path = f"s3://{DASHBOARD_BUCKET}/{key}"

    try:
        dashboard_df = spark.read.option("header", "true").csv(dashboard_path)
        total_records = dashboard_df.count()
        logger.info(f"{survey_name} {event_date}: dashboard file {dashboard_path} has {total_records} total records")

        date_only = F.split(F.col(RAW_DASHBOARD_INVITATION_DATE_FIELD), " ").getItem(0)

        filtered_df = (
            dashboard_df
            .withColumn(
                "_invitation_date_parsed",
                F.coalesce(
                    F.to_date(date_only, "M/d/yyyy"),
                    F.to_date(date_only, "yyyy-MM-dd"),
                )
            )
            .filter(F.col(RAW_DASHBOARD_INVITATION_DATE_FIELD).isNotNull())
            .filter(F.col("_invitation_date_parsed") == F.to_date(F.lit(event_date), "yyyy-MM-dd"))
        )

        filtered_count = filtered_df.count()
        logger.info(f"{survey_name} {event_date}: {filtered_count} dashboard records matched after date parsing")

        distinct_count = (
            filtered_df
            .select(RAW_DASHBOARD_CONTACT_ID_FIELD)
            .distinct()
            .count()
        )

        logger.info(f"{survey_name} {event_date}: dashboard_count={distinct_count} (from {dashboard_path})")
        return distinct_count

    except Exception as e:
        logger.error(f"DASHBOARD READ FAILED for {survey_name}, event_date={event_date}, path={dashboard_path}: {e}")
        raise


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
    """
    Checks whether this survey_name + event_date has already been
    reconciled. Bypassed entirely when FORCE_RERUN_PARAM is set, so a
    forced run recomputes and overwrites existing rows instead of
    skipping them.
    """
    if FORCE_RERUN_PARAM:
        return False

    table = dynamodb.Table(DYNAMODB_TABLE_NAME)
    response = table.get_item(
        Key={"event_date": event_date, "survey_type": survey_name}
    )
    return "Item" in response


def write_dynamodb_summary(summary):
    """
    put_item() overwrites any existing row for this key automatically --
    no delete step needed before writing, whether this is a first-time
    write or a forced recompute of an already-reconciled date.
    """
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)
    table.put_item(
        Item={
            "event_date": summary["event_date"],
            "survey_type": summary["survey_name"],
            "raw_count": summary["raw_count"],
            "curated_count": summary["curated_count"],
            "sentiment_count": summary["sentiment_count"],
            "dashboard_count": summary["dashboard_count"],
            "removed_count": summary["removed_count"],
            "unaccounted_count": summary["unaccounted_count"],
            "raw_curated_mismatch": summary["raw_curated_mismatch"],
            "dashboard_curated_mismatch": summary["dashboard_curated_mismatch"],
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
        "sentiment_count": None,
        "dashboard_count": None,
        "dashboard_curated_mismatch": None,
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
    dashboard_count = count_dashboard_records(survey_name, event_date)

    raw_curated_mismatch = raw_count != curated_count
    if raw_curated_mismatch:
        logger.warning(
            f"{survey_name} {event_date}: RAW/CURATED MISMATCH "
            f"(raw={raw_count}, curated={curated_count})."
        )

    dashboard_curated_mismatch = (
        dashboard_count is not None and dashboard_count != curated_count
    )
    if dashboard_curated_mismatch:
        logger.warning(
            f"{survey_name} {event_date}: DASHBOARD/CURATED MISMATCH "
            f"(dashboard={dashboard_count}, curated={curated_count})."
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
        "dashboard_count": dashboard_count,
        "dashboard_curated_mismatch": dashboard_curated_mismatch,
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

    logger.info(f"{survey_name}: reconciling event_date={event_date}" + (" [FORCE_RERUN]" if FORCE_RERUN_PARAM else ""))

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
    logger.info(
        f"Processing {len(dates_to_process)} date(s): mode determined by job parameters. "
        f"FORCE_RERUN={FORCE_RERUN_PARAM}"
    )

    is_live_mode = dates_to_process == [None]

    if is_live_mode:
        raw_counts_cache = {}

        def get_raw_counts_for_date(event_date):
            if event_date not in raw_counts_cache:
                raw_counts_cache[event_date] = count_all_raw_survey_records_single_date(event_date)
            return raw_counts_cache[event_date]
    else:
        raw_counts_lookup = build_raw_counts_for_date_range(dates_to_process)

        def get_raw_counts_for_date(event_date):
            return {
                survey_key: count
                for (date_key, survey_key), count in raw_counts_lookup.items()
                if date_key == event_date
            }

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