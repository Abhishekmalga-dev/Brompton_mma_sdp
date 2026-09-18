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
# CHANGED: reconciliation now counts by Response Received Date everywhere,
# to match Feedback Manager's own filtering semantics, rather than
# Invitation Date. Folder PLACEMENT in Raw/Curated/Sentiment is still
# governed by Invitation Date upstream -- only the counting field changed.
RAW_RESPONSE_RECEIVED_DATE_FIELD = "Response Received Date"

CONTACT_RECORD_ID_COLUMN = "contact_record_id"
REMOVAL_REASON_COLUMN = "removal_reason"
# CHANGED: confirmed field names for the new counting field on each layer.
CURATED_RESPONSE_RECEIVED_DATE_COLUMN = "response_received_date"
SENTIMENT_RESPONSE_RECEIVED_DATE_COLUMN = "raw_response_received_date"

DASHBOARD_BUCKET = "psegli-datalakenonprodli-datalake-temp-dev"
DASHBOARD_PREFIX = "Sentiment_Analysis/dashboard_output"

RAW_DASHBOARD_CONTACT_ID_FIELD = "raw_contact_record_id"
RAW_DASHBOARD_INVITATION_DATE_FIELD = "raw_invitation_date"
# CHANGED: dashboard's counting field, confirmed field name.
RAW_DASHBOARD_RESPONSE_RECEIVED_DATE_FIELD = "raw_response_received_date"

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
    3. neither set             -> [None] (live mode)
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


def list_event_date_partitions(bucket, prefix):
    """Lists every existing event_date=YYYY-MM-DD/ partition folder name under a prefix."""
    if not prefix.endswith("/"):
        prefix += "/"
    paginator = s3_client.get_paginator("list_objects_v2")
    dates = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            folder_name = cp["Prefix"].rstrip("/").split("/")[-1]
            if folder_name.startswith("event_date="):
                dates.append(folder_name.replace("event_date=", ""))
    return dates


def build_raw_counts_for_date_range(all_target_dates, forward_window_days=RAW_LOOKBACK_WINDOW_DAYS, max_search_days=120):
    """
    Reads every Raw delivery folder covering the full target date range,
    tags every record by its OWN Response Received Date (CHANGED from
    Invitation Date -- see module-level note), and returns a lookup
    keyed by (event_date, survey_name_lower) -> distinct_count.

    Two regimes:

    1. NORMAL CADENCE: pools every folder in a window SYMMETRIC around
       each target date, event_date=X-forward_window_days through
       X+forward_window_days.
       CHANGED (2026-09-18): widened from forward-only to symmetric.
       Folder placement is still keyed by Invitation Date, but the
       counting field is now Response Received Date, and
       response_received_date >= invitation_date by up to 6 days
       (confirmed across all 6 surveys) while folder_date >=
       invitation_date by up to forward_window_days -- so relative to a
       target RESPONSE date, the record's folder can now be BEFORE that
       date too, not just after. This is a structural consequence of
       changing the counting field, not a generalization of the one-off
       2026-07-15 backward-filing anomaly (that stays undone -- see
       engineering-learnings.md).

    2. SPARSE HISTORICAL FALLBACK (unchanged in shape): for target dates
       where NOT A SINGLE folder exists anywhere in the normal window --
       confirmed pattern in Jan/Feb 2026 sparse delivery -- expands
       outward in both directions, day by day, until it finds the
       nearest folder(s) that exist, capped at max_search_days.
    """
    if not all_target_dates:
        return {}

    existing_folders = set(list_event_date_partitions(RAW_BUCKET, RAW_PREFIX))

    # --- Regime 1: symmetric window ---
    folders_to_read = set()
    dates_with_no_folder_in_window = []

    for target_date in all_target_dates:
        base = datetime.strptime(target_date, "%Y-%m-%d")
        window_folders = [
            (base + timedelta(days=offset)).strftime("%Y-%m-%d")
            # CHANGED: was range(0, forward_window_days + 1) -- forward only.
            for offset in range(-forward_window_days, forward_window_days + 1)
        ]
        matched = [f for f in window_folders if f in existing_folders]
        if matched:
            folders_to_read.update(matched)
        else:
            dates_with_no_folder_in_window.append(target_date)

    # --- Regime 2: sparse historical fallback -- ONLY for dates that
    #     found nothing at all in Regime 1 ---
    if dates_with_no_folder_in_window:
        logger.info(
            f"{len(dates_with_no_folder_in_window)} date(s) found no folder in the "
            f"normal +/-{forward_window_days}-day window -- expanding search "
            f"(sparse historical delivery period): {dates_with_no_folder_in_window}"
        )
        for target_date in dates_with_no_folder_in_window:
            base = datetime.strptime(target_date, "%Y-%m-%d")
            found = False
            for offset in range(forward_window_days + 1, max_search_days + 1):
                forward_candidate = (base + timedelta(days=offset)).strftime("%Y-%m-%d")
                backward_candidate = (base - timedelta(days=offset)).strftime("%Y-%m-%d")
                for candidate in (forward_candidate, backward_candidate):
                    if candidate in existing_folders:
                        folders_to_read.add(candidate)
                        found = True
                if found:
                    break
            if not found:
                logger.warning(
                    f"No Raw folder found within {max_search_days} days of "
                    f"{target_date} in either direction."
                )

    logger.info(f"Reading {len(folders_to_read)} Raw delivery folder(s) once each, covering {len(all_target_dates)} target date(s).")

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
            # CHANGED: tag by Response Received Date via the flexible
            # parser (this field carries the same mixed-format risk
            # confirmed on Curated), not a naive 10-char substring of
            # Invitation Date.
            tagged_df = (
                raw_df
                .filter(F.col(RAW_RESPONSE_RECEIVED_DATE_FIELD).isNotNull())
                .filter(F.trim(F.col(RAW_RESPONSE_RECEIVED_DATE_FIELD)) != "")
                .withColumn(
                    "_event_date_str",
                    F.date_format(_parse_invitation_date(F.col(RAW_RESPONSE_RECEIVED_DATE_FIELD)), "yyyy-MM-dd")
                )
                .filter(F.col("_event_date_str").isin(list(target_dates_set)))
                .withColumn("_survey_name_lower", F.lower(F.trim(F.col(RAW_SURVEY_TYPE_FIELD))))
                .select(
                    F.col(RAW_CONTACT_ID_FIELD).alias("_contact_id"),
                    "_survey_name_lower",
                    F.col("_event_date_str").alias("_event_date")
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
    bulk lookup can't be pre-built. Searches a window SYMMETRIC around
    RAW_LOOKBACK_WINDOW_DAYS for just this one date, on demand, and tags
    by Response Received Date -- same reasoning as
    build_raw_counts_for_date_range above.

    Returns a dict: {raw_survey_name_lowercase: distinct_count}
    """
    base = datetime.strptime(event_date, "%Y-%m-%d")
    folder_dates = [
        (base + timedelta(days=offset)).strftime("%Y-%m-%d")
        # CHANGED: was range(0, ... + 1) -- forward only.
        for offset in range(-RAW_LOOKBACK_WINDOW_DAYS, RAW_LOOKBACK_WINDOW_DAYS + 1)
    ]

    json_keys = []
    for folder_date in folder_dates:
        json_keys.extend(_list_raw_json_keys_for_folder(folder_date))

    if not json_keys:
        logger.warning(f"No Raw files found for event_date={event_date} across window {folder_dates}")
        return {}

    logger.info(f"event_date={event_date}: found {len(json_keys)} Raw file(s) across window")

    combined_df = None
    for key in json_keys:
        raw_path = f"s3://{RAW_BUCKET}/{key}"
        try:
            raw_df = spark.read.option("multiLine", "true").json(raw_path)

            filtered_df = (
                raw_df
                .filter(F.col(RAW_RESPONSE_RECEIVED_DATE_FIELD).isNotNull())
                .filter(F.trim(F.col(RAW_RESPONSE_RECEIVED_DATE_FIELD)) != "")
                .withColumn(
                    "_event_date_str",
                    F.date_format(_parse_invitation_date(F.col(RAW_RESPONSE_RECEIVED_DATE_FIELD)), "yyyy-MM-dd")
                )
                .filter(F.col("_event_date_str") == event_date)
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


def _parse_invitation_date(date_col):
    """
    Normalizes a date column across every format variant we've
    encountered or might reasonably encounter on this pipeline's data
    (name kept for continuity -- used on Invitation Date originally,
    now also reused for Response Received Date on every layer, since
    the same mixed-format risk applies to both fields):
      - "yyyy-MM-dd HH:mm:ss"   (e.g. 2026-08-07 05:42:39)
      - "yyyy-MM-dd HH:mm"      (same, no seconds)
      - "M/d/yyyy H:mm:ss"      (e.g. 8/7/2026 5:42:39)
      - "M/d/yyyy H:mm"         (e.g. 8/7/2026 5:42, no seconds)
      - "yyyy-MM-dd"            (date only, no time)
      - "M/d/yyyy"              (date only, no time)
    coalesce() tries each pattern in order and returns the first one
    that successfully parses -- any row matching none of these returns
    NULL, which is intentionally excluded by the isNotNull() filter
    downstream rather than silently miscounted.
    """
    date_part = F.split(date_col, " ").getItem(0)
    return F.coalesce(
        F.to_date(date_col, "yyyy-MM-dd HH:mm:ss"),
        F.to_date(date_col, "yyyy-MM-dd HH:mm"),
        F.to_date(date_col, "M/d/yyyy H:mm:ss"),
        F.to_date(date_col, "M/d/yyyy H:mm"),
        F.to_date(date_part, "yyyy-MM-dd"),
        F.to_date(date_part, "M/d/yyyy"),
    )


def count_dashboard_records(survey_name, event_date):
    """
    Reads the single, always-overwritten dashboard CSV for this survey,
    filters by raw_response_received_date == event_date (CHANGED from
    raw_invitation_date), and counts DISTINCT raw_contact_record_id.

    CONFIRMED ROOT CAUSE (2026-09-15) of the ORIGINAL undercount bug:
    the default CSV reader (no multiLine/quote/escape options) mis-parses
    row boundaries whenever a field contains an embedded quote or newline
    character -- shifting columns and silently corrupting or dropping
    individual records. Invisible in Excel, which parses quoted CSV
    structure correctly. Fixed by reading with multiLine=True and
    explicit quote/escape handling, matching Spark's proper CSV/RFC-4180
    parsing. That fix is unrelated to, and unaffected by, the field
    change below.
    """
    file_key = f"{DASHBOARD_PREFIX}/{DASHBOARD_FILE_MAP[survey_name]}"
    df = (
        spark.read
            .option("header", True)
            .option("multiLine", True)
            .option("quote", '"')
            .option("escape", '"')
            .option("mode", "PERMISSIVE")
            .csv(f"s3://{DASHBOARD_BUCKET}/{file_key}")
    )

    # CHANGED: was RAW_DASHBOARD_INVITATION_DATE_FIELD with an inline
    # 2-format coalesce; now uses the response-received field and the
    # shared _parse_invitation_date() helper (6 formats).
    filtered = df.filter(
        _parse_invitation_date(F.col(RAW_DASHBOARD_RESPONSE_RECEIVED_DATE_FIELD)) == F.lit(event_date)
    )

    return filtered.select(RAW_DASHBOARD_CONTACT_ID_FIELD).distinct().count()


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


def read_curated_or_sentiment_by_response_date(bucket, prefix_base, folder, event_date, response_date_column, window_days=RAW_LOOKBACK_WINDOW_DAYS):
    """
    NEW (2026-09-18): reads every event_date= partition within a
    symmetric window_days window of the target date and filters to rows
    whose OWN response_date_column matches event_date.

    Required because Curated/Sentiment partitions are keyed by
    Invitation Date (folder placement UNCHANGED), which can now diverge
    from the counting field (Response Received Date) by up to 6 days in
    either direction (confirmed across all 6 surveys, 2026-09-18) -- the
    same class of gap Raw's window search already accounted for.
    Previously these two layers did NO per-row date filtering at all
    (count_df_rows() just counted every row physically in the
    Invitation-Date-keyed partition); this closes that gap.

    response_date_column differs by source: "response_received_date" on
    Curated, "raw_response_received_date" on Sentiment -- confirmed
    field names, pass explicitly rather than assuming they match.

    Returns None if no partition in the window has any data at all.
    """
    base = datetime.strptime(event_date, "%Y-%m-%d")
    combined_df = None

    for offset in range(-window_days, window_days + 1):
        candidate_date = (base + timedelta(days=offset)).strftime("%Y-%m-%d")
        prefix = f"{prefix_base}/{folder}/event_date={candidate_date}/"
        df = read_parquet_df(bucket, prefix)
        if df is None:
            continue

        tagged = (
            df
            .filter(F.col(response_date_column).isNotNull())
            .filter(_parse_invitation_date(F.col(response_date_column)) == F.lit(event_date))
        )
        combined_df = tagged if combined_df is None else combined_df.unionByName(tagged, allowMissingColumns=True)

    if combined_df is None:
        return None
    return combined_df.dropDuplicates([CONTACT_RECORD_ID_COLUMN])


def read_csv_df(bucket, prefix):
    """
    Reads quarantine (removed_records) data via Spark's CSV reader.
    Returns None if the partition has no data.

    NOTE -- open item, not yet changed: quarantine is still read from a
    single fixed event_date= partition (Invitation-Date-keyed), not the
    symmetric response-date window. If a record's true response date
    lands outside its own Invitation-Date partition, its quarantine
    removal_reason (if any) may not be found by the missing_ids lookup
    in reconcile_active_survey. Flagged for follow-up, not addressed in
    this change set.
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
            "generated_at": summary["generated_at"],
        }
    )

# =============================================================================
# CORE RECONCILIATION LOGIC
# =============================================================================

def reconcile_raw_only_survey(survey_config, event_date, raw_count):
    survey_name = survey_config["survey_name"]
    curated_folder = survey_config["curated_folder"]

    # CHANGED: was a single fixed-partition read_parquet_df() call;
    # now a symmetric response-date-windowed read.
    curated_df = read_curated_or_sentiment_by_response_date(
        CURATED_BUCKET, CURATED_PREFIX_BASE, curated_folder, event_date,
        CURATED_RESPONSE_RECEIVED_DATE_COLUMN
    )
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

    # CHANGED: curated_df and sentiment_df now use the symmetric
    # response-date-windowed read, each with its own confirmed column
    # name, instead of a single fixed Invitation-Date partition.
    curated_df = read_curated_or_sentiment_by_response_date(
        CURATED_BUCKET, CURATED_PREFIX_BASE, curated_folder, event_date,
        CURATED_RESPONSE_RECEIVED_DATE_COLUMN
    )
    sentiment_df = read_curated_or_sentiment_by_response_date(
        CURATED_BUCKET, SENTIMENT_PREFIX_BASE, sentiment_folder, event_date,
        SENTIMENT_RESPONSE_RECEIVED_DATE_COLUMN
    )

    # NOT changed -- see open item noted on read_csv_df().
    quarantine_prefix = f"{QUARANTINE_PREFIX_BASE}/{quarantine_folder}/event_date={event_date}/"
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
        f"Processing {len(dates_to_process)} date(s): "
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