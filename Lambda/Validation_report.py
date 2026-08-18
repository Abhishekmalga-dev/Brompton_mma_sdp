"""
datalake-reconciliation-report-dev

Reconciles record counts across Raw -> Curated -> Sentiment for all
survey types, explains removed records via quarantine Parquet files, and
flags any records that vanished WITHOUT a quarantine entry
(unaccounted_count).

Reads data DIRECTLY from S3 via boto3 + pyarrow -- no Starburst, no
Athena, no query engine of any kind. Starburst only ever sees this
pipeline's OUTPUT, via the Glue Crawler triggered at the end of this
script; this script has zero awareness of Starburst.

Parquet files are downloaded as raw bytes via boto3 and parsed in-memory
with pyarrow.parquet -- deliberately NOT using pyarrow.fs.S3FileSystem,
since that requires the pyarrow build to include optional S3/libcurl
support, which isn't guaranteed present in every Lambda layer build
(confirmed failure: "pyarrow installation is not built with support for
'S3FileSystem'" on AWSSDKPandas-Python314). Downloading bytes via boto3
and parsing with plain pyarrow.parquet avoids this dependency entirely.

IMPORTANT: event_date and survey_name are written into the S3 KEY PATH
as Hive-style partitions (event_date=.../survey_name=.../), and are
deliberately EXCLUDED from the JSON body itself. Including them in both
places causes the Glue Crawler to register two columns with the same
name (one from the partition, one from the file content), which
Trino/Starburst rejects with "Table descriptor contains duplicate
columns."

Requires a Lambda layer with `pyarrow` installed (e.g. AWSSDKPandas) --
not in the default Lambda runtime.

Trigger: EventBridge rule on the Sentiment-writing Step Function's
         "ExecutionSucceeded" event.
Backfill: invoke directly with {"event_date": "YYYY-MM-DD"} in the payload
          to reconcile a specific historical date instead of "latest".
          For a full date range, invoke this Lambda once per date from
          outside (manually via the Console Test button, or a small
          driver script) -- do not pass a date range into this Lambda.
"""

import boto3
import json
import logging
import io
import pyarrow.parquet as pq
from datetime import datetime, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# =============================================================================
# CONFIG
# =============================================================================

RAW_BUCKET = "psegli-datalakenonprodli-datalake-raw-dev"
RAW_PREFIX = "ccaas/survey_api_json"  # + /event_date=YYYY-MM-DD/

CURATED_BUCKET = "psegli-datalakenonprodli-datalake-curated-dev"
CURATED_PREFIX_BASE = "ccaas"  # + /{curated_folder}/event_date=YYYY-MM-DD/

SENTIMENT_PREFIX_BASE = "sentiment_analysis/final"  # + /{sentiment_folder}/event_date=.../

QUARANTINE_PREFIX_BASE = "sentiment_analysis/removed_records"  # + /{quarantine_folder}/event_date=.../

# Confirmed against an actual Raw JSON record.
RAW_SURVEY_TYPE_FIELD = "Survey Name"
RAW_CONTACT_ID_FIELD = "Contact Record ID"

# Curated/Sentiment/Quarantine Parquet layers use snake_case column names
# (normalized by the curation job) -- different from the Raw JSON field
# names above.
CONTACT_RECORD_ID_COLUMN = "contact_record_id"
REMOVAL_REASON_COLUMN = "removal_reason"

# Full survey configuration, confirmed against the curation job's own
# SURVEY_PREFIX_MAP. raw_survey_name is matched case-insensitively
# against the Raw file's "Survey Name" field, matching the same
# case-insensitive pattern the curation job itself already uses.
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

# Report storage
S3_REPORT_BUCKET = "psegli-datalakenonprodli-datalake-curated-dev"
S3_REPORT_PREFIX = "ccaas/Survey_Reconciliation_Report"

DYNAMODB_TABLE_NAME = "datalake-ccaas-reconciliation-dev"

# Glue crawler that registers the S3 report output in the Data Catalog,
# making it queryable in Starburst automatically.
GLUE_CRAWLER_NAME = "datalake-reconciliation-report-dev"

# =============================================================================
# AWS CLIENTS
# =============================================================================

s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")


# =============================================================================
# S3 / PARQUET / JSON HELPERS
# =============================================================================

def list_event_date_partitions(bucket, prefix):
    """
    Lists the event_date=YYYY-MM-DD partition folders directly under a
    given S3 prefix, using a delimiter so we only get one level of
    "folders" back (not every object recursively).
    Returns a sorted list of date strings, e.g. ["2026-03-24", "2026-03-25"].
    """
    if not prefix.endswith("/"):
        prefix += "/"

    paginator = s3_client.get_paginator("list_objects_v2")
    dates = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for common_prefix in page.get("CommonPrefixes", []):
            folder_name = common_prefix["Prefix"].rstrip("/").split("/")[-1]
            if folder_name.startswith("event_date="):
                dates.append(folder_name.replace("event_date=", ""))

    return sorted(dates)


def get_max_event_date(bucket, prefix):
    """Returns the most recent event_date partition under a prefix, or None."""
    dates = list_event_date_partitions(bucket, prefix)
    return dates[-1] if dates else None


def _list_parquet_keys(bucket, prefix):
    """Lists all .parquet object keys under a given S3 prefix."""
    paginator = s3_client.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append(obj["Key"])
    return keys


def _read_parquet_table(bucket, key, columns=None):
    """
    Downloads a single Parquet file's bytes via boto3 and parses it with
    pyarrow from memory. Avoids pyarrow.fs.S3FileSystem entirely, which
    requires the pyarrow build to include optional S3/libcurl support --
    not guaranteed present in every Lambda layer build. boto3 always
    supports plain object downloads, so this works regardless of how
    pyarrow itself was compiled.
    """
    response = s3_client.get_object(Bucket=bucket, Key=key)
    data = response["Body"].read()
    return pq.read_table(io.BytesIO(data), columns=columns)


def partition_has_data(bucket, prefix):
    """Checks whether a specific S3 prefix has any objects at all."""
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    return response.get("KeyCount", 0) > 0


def count_parquet_rows(bucket, prefix):
    """Counts total rows across all Parquet files under an S3 prefix."""
    keys = _list_parquet_keys(bucket, prefix)
    if not keys:
        return 0

    total = 0
    for key in keys:
        table = _read_parquet_table(bucket, key)
        total += table.num_rows
    return total


def read_parquet_column(bucket, prefix, column_name):
    """
    Reads a single column from all Parquet files under an S3 prefix.
    Used to pull contact_record_id sets for the anti-join.
    Returns an empty set if the prefix has no data.
    """
    keys = _list_parquet_keys(bucket, prefix)
    if not keys:
        return set()

    values = set()
    for key in keys:
        table = _read_parquet_table(bucket, key, columns=[column_name])
        values.update(table.column(column_name).to_pylist())
    return values


def read_parquet_columns_as_dict(bucket, prefix, key_column, value_column):
    """
    Reads two columns from Parquet files under a prefix and returns a
    dict of key_column -> value_column. Used for quarantine tables to
    build contact_record_id -> removal_reason lookups.
    Returns an empty dict if the prefix has no data.
    """
    keys = _list_parquet_keys(bucket, prefix)
    if not keys:
        return {}

    result = {}
    for key in keys:
        table = _read_parquet_table(bucket, key, columns=[key_column, value_column])
        ids = table.column(key_column).to_pylist()
        vals = table.column(value_column).to_pylist()
        result.update(dict(zip(ids, vals)))
    return result


def count_all_raw_survey_records(event_date):
    """
    Reads the Raw JSON file(s) for a given event_date ONCE, and counts
    records for ALL survey types in a single pass -- rather than
    re-reading and re-parsing the same files once per survey (7 reads of
    identical data for one date). Raw has all surveys mixed together in
    one flat JSON array per day, so this single pass is strictly more
    efficient given there are always multiple surveys to count.

    Returns a dict: {raw_survey_name_lowercase: count}
    """
    prefix = f"{RAW_PREFIX}/event_date={event_date}/"
    paginator = s3_client.get_paginator("list_objects_v2")

    counts = {}

    for page in paginator.paginate(Bucket=RAW_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".json"):
                continue

            response = s3_client.get_object(Bucket=RAW_BUCKET, Key=key)
            content = response["Body"].read().decode("utf-8")
            records = json.loads(content)  # top-level is a flat JSON array

            for record in records:
                survey_value = record.get(RAW_SURVEY_TYPE_FIELD, "")
                key_lower = survey_value.strip().lower()
                counts[key_lower] = counts.get(key_lower, 0) + 1

    return counts


# =============================================================================
# DYNAMODB HELPERS
# =============================================================================

def already_reconciled(survey_name, event_date):
    """
    Checks whether this survey_name + event_date has already been
    reconciled. Used to skip surveys with no new data (e.g., relational
    surveys that don't run daily) instead of re-writing the same report.
    """
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)
    response = table.get_item(
        Key={"event_date": event_date, "survey_type": survey_name}
    )
    return "Item" in response


def write_dynamodb_summary(summary):
    """Writes the lightweight summary row used for fast programmatic checks."""
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
    Handles surveys that exist in Raw/Curated but haven't been wired into
    Sentiment yet. Only raw_count and curated_count are real; everything
    downstream is reported as N/A until the pipeline catches up.
    """
    survey_name = survey_config["survey_name"]
    curated_folder = survey_config["curated_folder"]

    curated_prefix = f"{CURATED_PREFIX_BASE}/{curated_folder}/event_date={event_date}/"
    curated_count = count_parquet_rows(CURATED_BUCKET, curated_prefix)

    raw_curated_mismatch = raw_count != curated_count
    if raw_curated_mismatch:
        logger.warning(
            f"{survey_name} {event_date}: RAW/CURATED MISMATCH "
            f"(raw={raw_count}, curated={curated_count}). "
            f"This hop has no expected removal logic -- investigate."
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

    logger.info(f"{survey_name}: RAW_ONLY, raw={raw_count}, curated={curated_count} for {event_date}")
    return summary


def reconcile_active_survey(survey_config, event_date, raw_count):
    """Runs the full reconciliation for an active (Sentiment-wired) survey."""
    survey_name = survey_config["survey_name"]
    curated_folder = survey_config["curated_folder"]
    sentiment_folder = survey_config["sentiment_folder"]
    quarantine_folder = survey_config["quarantine_folder"]

    curated_prefix = f"{CURATED_PREFIX_BASE}/{curated_folder}/event_date={event_date}/"
    sentiment_prefix = f"{SENTIMENT_PREFIX_BASE}/{sentiment_folder}/event_date={event_date}/"
    quarantine_prefix = f"{QUARANTINE_PREFIX_BASE}/{quarantine_folder}/event_date={event_date}/"

    curated_count = count_parquet_rows(CURATED_BUCKET, curated_prefix)
    sentiment_count = count_parquet_rows(CURATED_BUCKET, sentiment_prefix)

    raw_curated_mismatch = raw_count != curated_count
    if raw_curated_mismatch:
        logger.warning(
            f"{survey_name} {event_date}: RAW/CURATED MISMATCH "
            f"(raw={raw_count}, curated={curated_count}). "
            f"This hop has no expected removal logic -- investigate."
        )

    # --- Anti-join: which contact_record_ids are in Curated but not Sentiment ---
    curated_ids = read_parquet_column(CURATED_BUCKET, curated_prefix, CONTACT_RECORD_ID_COLUMN)
    sentiment_ids = read_parquet_column(CURATED_BUCKET, sentiment_prefix, CONTACT_RECORD_ID_COLUMN)
    missing_ids = curated_ids - sentiment_ids

    # --- Explain the missing IDs via quarantine ---
    quarantine_reasons = read_parquet_columns_as_dict(
        CURATED_BUCKET, quarantine_prefix, CONTACT_RECORD_ID_COLUMN, REMOVAL_REASON_COLUMN
    )

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
            f"{survey_name} {event_date}: {unaccounted_count} records missing "
            f"with NO quarantine explanation. See anomaly detail report."
        )

    write_dynamodb_summary(summary)

    return summary


def reconcile_survey(survey_config, event_date_override, get_raw_counts_for_date):
    """
    Resolves which event_date to reconcile for this survey, checks the
    already-reconciled skip condition, then dispatches to the active or
    raw-only reconciliation path.
    Returns None if skipped (no new data, or already done).
    """
    survey_name = survey_config["survey_name"]
    curated_folder = survey_config["curated_folder"]
    sentiment_active = survey_config["sentiment_pipeline_active"]

    # Date resolution: active surveys resolve from Sentiment's own latest
    # partition; raw-only surveys resolve from Curated instead, since
    # Sentiment has no data for them at all.
    if sentiment_active:
        date_source_prefix = f"{SENTIMENT_PREFIX_BASE}/{survey_config['sentiment_folder']}"
    else:
        date_source_prefix = f"{CURATED_PREFIX_BASE}/{curated_folder}"

    if event_date_override:
        event_date = event_date_override
    else:
        event_date = get_max_event_date(CURATED_BUCKET, date_source_prefix)
        if event_date is None:
            logger.info(f"{survey_name}: no data found under {date_source_prefix}, skipping.")
            return None

    if not event_date_override and already_reconciled(survey_name, event_date):
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
    Writes the daily summary as JSON to S3, Hive-partitioned by event_date
    and survey_name so a Glue crawler picks these up as partition columns.

    IMPORTANT: event_date and survey_name are deliberately EXCLUDED from
    the JSON body itself. They're already encoded in the S3 path via
    Hive-style partitioning (event_date=.../survey_name=.../), and the
    crawler infers them as partition columns from the path automatically.
    Including them again inside the JSON body causes the crawler to
    register two columns with the same name (one from the partition,
    one from the file content), which Trino/Starburst rejects with
    "Table descriptor contains duplicate columns."

    Active and raw-only surveys go to SEPARATE prefixes because their
    schemas differ (raw-only has "N/A" strings for sentiment/removed/
    unaccounted counts, active surveys have real integers). Mixing them
    would make the crawler infer STRING for those columns everywhere,
    breaking numeric queries (SUM/AVG/comparisons) on the active surveys.
    """
    is_active = summary["pipeline_status"] == "ACTIVE"
    subfolder = "summary_active" if is_active else "summary_raw_only"

    key = (
        f"{S3_REPORT_PREFIX}/{subfolder}/"
        f"event_date={summary['event_date']}/"
        f"survey_name={summary['survey_name']}/"
        f"report.json"
    )

    # Flatten reason_breakdown (dict with variable keys per day) into a
    # fixed-shape array of {reason, count} objects, so the crawler infers
    # a stable schema regardless of which specific reasons show up.
    reason_breakdown_list = [
        {"removal_reason": reason, "record_count": count}
        for reason, count in summary.get("reason_breakdown", {}).items()
    ]

    record = dict(summary)
    record["reason_breakdown"] = reason_breakdown_list

    # Remove the partition-carried fields from the JSON body itself --
    # see docstring above for why.
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
    """
    Writes record-level detail ONLY when something is genuinely
    unaccounted for. Keeps the summary tables lean on normal days.

    event_date and survey_name are excluded from the JSON body for the
    same reason as write_summary_to_s3() -- they're already encoded in
    the S3 partition path, and duplicating them in the body causes
    crawler schema conflicts.
    """
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
    """
    Kicks off the Glue crawler after this run finishes, so new partitions
    show up in Starburst automatically. This script never writes to
    Starburst directly -- the crawler handles catalog registration.
    Matches the existing crawler-start pattern used elsewhere in this
    project (e.g. datalake-crw-*-crawler-dev Lambdas).

    NOTE: CrawlerRunningException is treated as a non-fatal skip, since
    the reports above have already written successfully by this point --
    an overlapping crawler run is a scheduling detail, not a
    reconciliation failure.
    """
    if not GLUE_CRAWLER_NAME:
        return

    client = boto3.client('glue')

    try:
        client.start_crawler(
            Name=GLUE_CRAWLER_NAME
        )
    except client.exceptions.CrawlerRunningException:
        logger.info(f"Glue crawler {GLUE_CRAWLER_NAME} already running, skipping trigger.")
        return
    except Exception as e:
        logger.info("ERROR running the lambda script - {}... Please check...".format("start-crawler"))
        logger.info("error starting crawler")
        logger.info("ERROR is the following - {}... Please check...".format(e))
        raise e

    logger.info(f"Started Glue crawler: {GLUE_CRAWLER_NAME}")


# =============================================================================
# LAMBDA HANDLER
# =============================================================================

def lambda_handler(event, context):
    """
    event (optional, for backfill):
        {
            "event_date": "2026-03-24",
            "survey_names": ["API_TXN", "IVR"]   # optional, defaults to all
        }
    Live invocations (via EventBridge) pass no meaningful payload --
    each survey resolves its own latest event_date independently.
    """
    event = event or {}
    event_date_override = event.get("event_date")
    requested_surveys = event.get("survey_names")

    configs_to_run = SURVEY_CONFIGS
    if requested_surveys:
        configs_to_run = [
            c for c in SURVEY_CONFIGS if c["survey_name"] in requested_surveys
        ]

    # Raw is read ONCE per event_date, not once per survey. In a live run
    # different surveys may resolve different "latest" dates (relational
    # surveys don't run daily), so we don't know the full set of dates
    # up front -- we cache results per date as we discover them instead.
    raw_counts_cache = {}

    def get_raw_counts_for_date(event_date):
        if event_date not in raw_counts_cache:
            raw_counts_cache[event_date] = count_all_raw_survey_records(event_date)
        return raw_counts_cache[event_date]

    results = []
    skipped = []
    errors = []

    for survey_config in configs_to_run:
        survey_name = survey_config["survey_name"]
        try:
            summary = reconcile_survey(survey_config, event_date_override, get_raw_counts_for_date)
            if summary is None:
                skipped.append(survey_name)
            else:
                results.append(summary)
        except Exception as e:
            logger.error(f"Reconciliation FAILED for {survey_name}: {e}")
            errors.append({"survey_name": survey_name, "error": str(e)})
            continue

    logger.info(
        f"Reconciliation complete. "
        f"Succeeded: {len(results)}, Skipped: {len(skipped)}, Failed: {len(errors)}"
    )

    if results:
        trigger_glue_crawler()

    response = {
        "statusCode": 200 if not errors else 500,
        "reconciled": [r["survey_name"] for r in results],
        "skipped": skipped,
        "errors": errors,
    }

    if errors:
        raise RuntimeError(f"Partial reconciliation failure: {json.dumps(errors)}")

    return response