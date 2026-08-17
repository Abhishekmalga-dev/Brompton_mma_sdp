"""
json_to_csv_s3.py
Read a JSON file from S3, convert it to CSV, and write the result to a
(different) S3 location.

SOURCE_S3_PATH and DEST_S3_PATH below are hardcoded on purpose — edit
them directly each time you point this at a new file. If you later want
this to take paths as arguments/parameters instead, that's a one-line
change (see the note above main()).

Handles:
- A JSON array of objects: [ {...}, {...} ]
- A single JSON object: { ... }
- JSON Lines format (one JSON object per line), via LINES_MODE
- Nested objects, flattened into columns like "address_city"
- Records with different/missing keys (writes the union of all columns)
- Lists inside a record (kept as a JSON string in one cell, not exploded
  into extra rows — see notes in the chat response for why)

Requires: boto3 (already present in any standard AWS Python environment)
IAM permissions needed: s3:GetObject on the source, s3:PutObject on the
destination.
"""

import json
import csv
import io
import sys
import boto3

# ---------------------------------------------------------------------------
# EDIT THESE FOR EACH RUN
# ---------------------------------------------------------------------------
SOURCE_S3_PATH = "s3://your-source-bucket/path/to/input.json"
DEST_S3_PATH = "s3://your-destination-bucket/path/to/output.csv"

LINES_MODE = False   # Set True if SOURCE_S3_PATH is JSON Lines (.jsonl)
SEP = "_"            # Separator used when flattening nested JSON keys
# ---------------------------------------------------------------------------


def parse_s3_uri(uri):
    """
    Splits 's3://bucket-name/key/path/file.json' into
    ('bucket-name', 'key/path/file.json').
    """
    if not uri.startswith("s3://"):
        raise ValueError(f"Not a valid S3 URI (must start with s3://): {uri}")
    without_scheme = uri[len("s3://"):]
    bucket, _, key = without_scheme.partition("/")
    if not bucket or not key:
        raise ValueError(f"Could not parse bucket/key from URI: {uri}")
    return bucket, key


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
            # Lists are stored as a JSON string rather than exploded into
            # separate rows, to keep a 1:1 mapping between input records
            # and output rows (predictable row count).
            items[new_key] = json.dumps(value)
        else:
            items[new_key] = value
    return items


def load_records_from_s3(s3_client, bucket, key, lines_mode):
    """Downloads the JSON object from S3 and returns a list of dicts."""
    response = s3_client.get_object(Bucket=bucket, Key=key)
    body_text = response["Body"].read().decode("utf-8")

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


def main():
    s3 = boto3.client("s3")

    src_bucket, src_key = parse_s3_uri(SOURCE_S3_PATH)
    dest_bucket, dest_key = parse_s3_uri(DEST_S3_PATH)

    print(f"Reading s3://{src_bucket}/{src_key} ...")
    raw_records = load_records_from_s3(s3, src_bucket, src_key, LINES_MODE)

    if not raw_records:
        print("No records found in input file.", file=sys.stderr)
        sys.exit(1)

    flat_records = [
        flatten(r, sep=SEP) if isinstance(r, dict) else r for r in raw_records
    ]

    # Union of all keys across all records, preserving first-seen order.
    # This is what makes the script tolerant of records with missing
    # or extra fields (schema drift), instead of crashing on the first
    # record that doesn't match the others.
    fieldnames = []
    seen = set()
    for rec in flat_records:
        for key in rec.keys():
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)

    # Write CSV into an in-memory buffer first (no local disk involved),
    # then upload that buffer's contents to S3 in one put_object call.
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for rec in flat_records:
        writer.writerow(rec)

    print(f"Writing s3://{dest_bucket}/{dest_key} ...")
    s3.put_object(
        Bucket=dest_bucket,
        Key=dest_key,
        Body=buffer.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )

    print(
        f"Wrote {len(flat_records)} rows and {len(fieldnames)} columns "
        f"to s3://{dest_bucket}/{dest_key}"
    )


if __name__ == "__main__":
    main()