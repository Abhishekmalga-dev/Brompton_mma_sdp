"""
lambda_function.py
Lists every JSON file under SOURCE_S3_PATH (a folder/prefix), reads and
combines all of their records, and writes ONE combined CSV to DEST_S3_PATH.

SOURCE_S3_PATH and DEST_S3_PATH are hardcoded on purpose — edit them
directly for each run.

Handles:
- Multiple JSON files in one S3 "folder" (prefix), combined into one CSV
- A JSON array of objects, or a single JSON object, per file
- JSON Lines format (one JSON object per line), via LINES_MODE
- Nested objects, flattened into columns like "address_city"
- Records with different/missing keys across files (union of all columns)
- Lists inside a record (kept as a JSON string in one cell)
- S3 "folder marker" placeholder objects (zero-byte keys ending in "/")
  are skipped automatically when listing files

Lambda config required:
- Runtime settings -> Handler = lambda_function.lambda_handler
- Execution role needs s3:GetObject + s3:ListBucket on the source bucket,
  and s3:PutObject on the destination bucket
- Timeout/memory: increase if the partition can contain many/large files
  (current pipeline default of 1024MB / 900s should be a safe starting point)
"""

import json
import csv
import io
import sys
import boto3
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# EDIT THESE FOR EACH RUN
# ---------------------------------------------------------------------------
SOURCE_S3_PATH = "s3://psegli-datalakenonprodli-datalake-raw-dev/ccaas/survey_api_json/event_date=2026-05-11/"
DEST_S3_PATH = "s3://psegli-datalakenonprodli-datalake-raw-dev/ccass/"

LINES_MODE = False   # Set True if the JSON files are JSON Lines (.jsonl)
SEP = "_"            # Separator used when flattening nested JSON keys
# ---------------------------------------------------------------------------


def parse_s3_uri(uri):
    """
    Splits 's3://bucket-name/key/path/' into ('bucket-name', 'key/path/').
    Works for both a folder prefix (ends in '/') and a specific file key.
    """
    if not uri.startswith("s3://"):
        raise ValueError(f"Not a valid S3 URI (must start with s3://): {uri}")
    without_scheme = uri[len("s3://"):]
    bucket, _, key = without_scheme.partition("/")
    if not bucket or not key:
        raise ValueError(f"Could not parse bucket/key from URI: {uri}")
    return bucket, key


def list_json_keys(s3_client, bucket, prefix):
    """
    Lists every object key under a given S3 prefix (folder), skipping
    zero-byte "folder marker" objects (keys that end in "/", sometimes
    created when a folder is made via the S3 console) and, when not in
    JSON Lines mode, anything that isn't a .json file.
    """
    keys = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):
                continue
            if not LINES_MODE and not key.lower().endswith(".json"):
                continue
            keys.append(key)
    return keys


def flatten(obj, parent_key="", sep="_"):
    """
    Recursively flattens a nested dictionary into a single-level dict.
    Example: {"a": {"b": 1}} -> {"a_b": 1}
    """
    items = {}
    for key, value in obj.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key
        if isinstance(value, dict):
            items.update(flatten(value, new_key, sep=sep))
        elif isinstance(value, list):
            items[new_key] = json.dumps(value)
        else:
            items[new_key] = value
    return items


def parse_records_from_body(body_text, lines_mode):
    """Parses one S3 object's text content into a list of dict records."""
    records = []
    if lines_mode:
        for line_num, line in enumerate(body_text.splitlines(), start=1):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as e:
                print(f"Skipping malformed line {line_num}: {e}", file=sys.stderr)
    else:
        data = json.loads(body_text)
        if isinstance(data, list):
            records = data
        elif isinstance(data, dict):
            records = [data]
        else:
            raise ValueError(
                "Top-level JSON must be an object or an array of objects."
            )
    return records


def lambda_handler(event, context):
    s3 = boto3.client("s3")

    src_bucket, src_prefix = parse_s3_uri(SOURCE_S3_PATH)
    dest_bucket, dest_prefix = parse_s3_uri(DEST_S3_PATH)

    print(f"Listing JSON files under s3://{src_bucket}/{src_prefix} ...")
    json_keys = list_json_keys(s3, src_bucket, src_prefix)

    if not json_keys:
        msg = f"No JSON files found under s3://{src_bucket}/{src_prefix}"
        print(msg, file=sys.stderr)
        return {"statusCode": 404, "body": msg}

    print(f"Found {len(json_keys)} file(s). Reading and combining...")

    all_raw_records = []
    for key in json_keys:
        response = s3.get_object(Bucket=src_bucket, Key=key)
        body_text = response["Body"].read().decode("utf-8")
        try:
            file_records = parse_records_from_body(body_text, LINES_MODE)
        except (json.JSONDecodeError, ValueError) as e:
            print(f"Skipping unreadable file {key}: {e}", file=sys.stderr)
            continue
        all_raw_records.extend(file_records)

    if not all_raw_records:
        msg = "No records could be parsed from any file under this prefix."
        print(msg, file=sys.stderr)
        return {"statusCode": 422, "body": msg}

    flat_records = [
        flatten(r, sep=SEP) if isinstance(r, dict) else r for r in all_raw_records
    ]

    # Union of all keys across every record from every file, preserving
    # first-seen order — this is what lets files with slightly different
    # fields still combine into one consistent CSV.
    fieldnames = []
    seen = set()
    for rec in flat_records:
        for k in rec.keys():
            if k not in seen:
                seen.add(k)
                fieldnames.append(k)

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for rec in flat_records:
        writer.writerow(rec)

    # DEST_S3_PATH is a folder, not a filename, so we generate one here.
    # Using a UTC timestamp keeps re-runs from silently overwriting a
    # previous combined file with the same name.
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    dest_key = f"{dest_prefix.rstrip('/')}/combined_{timestamp}.csv"

    print(f"Writing combined CSV to s3://{dest_bucket}/{dest_key} ...")
    s3.put_object(
        Bucket=dest_bucket,
        Key=dest_key,
        Body=buffer.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )

    result_msg = (
        f"Combined {len(json_keys)} file(s) into {len(flat_records)} rows and "
        f"{len(fieldnames)} columns -> s3://{dest_bucket}/{dest_key}"
    )
    print(result_msg)
    return {"statusCode": 200, "body": result_msg}