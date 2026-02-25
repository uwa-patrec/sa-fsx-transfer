#!/usr/bin/env python3
"""
Stage 1a: FSx to S3 Manifest Uploader

Uploads a file from FSx to S3 (typically the chunks_manifest.json produced
by preprocessing).  Designed to run in the same lightweight downloader
container that already has boto3 and FSx access.

Environment Variables:
    INPUT_PATH: Absolute path to the local file on FSx to upload
    S3_BUCKET:  Destination S3 bucket name
    S3_KEY:     Destination S3 object key
"""

import os
import sys
import boto3
from pathlib import Path
from datetime import datetime


def upload_to_s3():
    """Upload a local file (on FSx) to S3."""

    input_path = os.environ.get("INPUT_PATH")
    s3_bucket = os.environ.get("S3_BUCKET")
    s3_key = os.environ.get("S3_KEY")

    if not input_path or not s3_bucket or not s3_key:
        print(
            "ERROR: INPUT_PATH, S3_BUCKET, and S3_KEY environment variables are required",
            file=sys.stderr,
        )
        print(f"  INPUT_PATH: {input_path}", file=sys.stderr)
        print(f"  S3_BUCKET:  {s3_bucket}", file=sys.stderr)
        print(f"  S3_KEY:     {s3_key}", file=sys.stderr)
        sys.exit(1)

    local_file = Path(input_path)

    if not local_file.exists():
        print(f"ERROR: Input file not found: {local_file}", file=sys.stderr)
        sys.exit(1)

    file_size = local_file.stat().st_size

    # Determine content type from extension
    content_type = "application/octet-stream"
    if local_file.suffix == ".json":
        content_type = "application/json"
    elif local_file.suffix == ".jsonl":
        content_type = "application/x-ndjson"

    print("=" * 70)
    print("Stage 1a: FSx to S3 Manifest Upload")
    print("=" * 70)
    print(f"Source:       {local_file}")
    print(f"Destination:  s3://{s3_bucket}/{s3_key}")
    print(f"File size:    {file_size:,} bytes")
    print(f"Content-Type: {content_type}")
    print(f"Started:      {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    s3_client = boto3.client("s3")

    start_time = datetime.now()

    try:
        s3_client.upload_file(
            Filename=str(local_file),
            Bucket=s3_bucket,
            Key=s3_key,
            ExtraArgs={"ContentType": content_type},
        )
    except Exception as e:
        print(f"\nERROR: Upload failed: {e}", file=sys.stderr)
        sys.exit(1)

    duration = (datetime.now() - start_time).total_seconds()

    # Verify the upload
    try:
        head = s3_client.head_object(Bucket=s3_bucket, Key=s3_key)
        remote_size = head["ContentLength"]
    except Exception as e:
        print(f"ERROR: Failed to verify uploaded object: {e}", file=sys.stderr)
        sys.exit(1)

    if remote_size != file_size:
        print("ERROR: Size mismatch after upload!", file=sys.stderr)
        print(f"  Local:  {file_size:,} bytes", file=sys.stderr)
        print(f"  Remote: {remote_size:,} bytes", file=sys.stderr)
        sys.exit(1)

    print("=" * 70)
    print("Upload Complete!")
    print("=" * 70)
    print(f"Duration:  {duration:.1f} seconds")
    print(f"Verified:  {remote_size:,} bytes on S3")
    print(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    return 0


if __name__ == "__main__":
    sys.exit(upload_to_s3())
