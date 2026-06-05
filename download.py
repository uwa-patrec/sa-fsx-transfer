#!/usr/bin/env python3
"""
Stage 0: S3 to FSx Downloader

This script downloads a file from S3 to FSx using optimized multipart download.
It's designed to run on cheap CPU instances before GPU processing begins.

Environment Variables:
    S3_BUCKET: Source S3 bucket name
    S3_KEY: Source S3 object key
    OUTPUT_PATH: Destination path on FSx (default: /fsx/input)
"""

import os
import sys
import boto3
from pathlib import Path
from datetime import datetime
from boto3.s3.transfer import TransferConfig


def format_bytes(bytes_val):
    """Convert bytes to human-readable format"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_val < 1024.0:
            return f"{bytes_val:.2f} {unit}"
        bytes_val /= 1024.0
    return f"{bytes_val:.2f} PB"


def download_from_s3():
    """Download file from S3 to FSx with optimized transfer"""

    # Get parameters from environment
    s3_bucket = os.environ.get('S3_BUCKET')
    s3_key = os.environ.get('S3_KEY')
    output_path = os.environ.get('OUTPUT_PATH', '/fsx/input')

    # Validate required parameters
    if not s3_bucket or not s3_key:
        print("ERROR: S3_BUCKET and S3_KEY environment variables are required", file=sys.stderr)
        print(f"  S3_BUCKET: {s3_bucket}", file=sys.stderr)
        print(f"  S3_KEY: {s3_key}", file=sys.stderr)
        sys.exit(1)

    # Calculate destination path
    filename = Path(s3_key).name
    fsx_file = Path(output_path) / filename

    # Create directory if needed
    fsx_file.parent.mkdir(parents=True, exist_ok=True)

    # Configure optimized transfer for large video files (typically 500 MB - 2 GB+).
    # Previous config used 1024 * 25 = 25 KB chunks (unit bug: comment said 25 MB),
    # which produced ~25,600 range-GETs for a 640 MB file and capped throughput
    # at ~8 MB/s due to per-request overhead dominating bandwidth.
    #
    # Memory note: boto3 keeps up to max_concurrency parts in flight, each up to
    # multipart_chunksize, so peak resident memory ~= max_concurrency * chunksize.
    # 20 * 64 MB = 1.28 GB overflowed the 2 GB job container and raised
    # "[Errno 12] Cannot allocate memory" on a 3.5 GB file. 8 * 16 MB = 128 MB
    # stays well within 2 GB while keeping enough parallelism for throughput.
    transfer_config = TransferConfig(
        multipart_threshold=64 * 1024 * 1024,   # 64 MB — use multipart for files > 64 MB
        max_concurrency=8,                       # 8 parallel threads (was 20 — bounded memory)
        multipart_chunksize=16 * 1024 * 1024,   # 16 MB per part (was 64 MB)
        use_threads=True,                        # Enable threading
    )

    # Initialize S3 client
    s3_client = boto3.client('s3')

    # Get file size
    try:
        response = s3_client.head_object(Bucket=s3_bucket, Key=s3_key)
        file_size = response['ContentLength']
    except Exception as e:
        print(f"ERROR: Failed to get file metadata from S3: {e}", file=sys.stderr)
        sys.exit(1)

    # Print download info
    print("=" * 70)
    print("Stage 0: S3 to FSx Download")
    print("=" * 70)
    print(f"Source:      s3://{s3_bucket}/{s3_key}")
    print(f"Destination: {fsx_file}")
    print(f"File size:   {format_bytes(file_size)}")
    print(f"Started:     {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    # Download file with progress tracking
    start_time = datetime.now()

    try:
        s3_client.download_file(
            Bucket=s3_bucket,
            Key=s3_key,
            Filename=str(fsx_file),
            Config=transfer_config
        )
    except Exception as e:
        print(f"\nERROR: Download failed: {e}", file=sys.stderr)
        sys.exit(1)

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    # Calculate transfer speed
    if duration > 0:
        speed_mbps = (file_size / (1024**2)) / duration
    else:
        speed_mbps = 0

    # Print success info
    print("=" * 70)
    print("Download Complete!")
    print("=" * 70)
    print(f"Duration:    {duration:.1f} seconds")
    print(f"Speed:       {speed_mbps:.1f} MB/s")
    print(f"Output:      {fsx_file}")
    print(f"Completed:   {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    # Verify file exists and has correct size
    if not fsx_file.exists():
        print(f"ERROR: Downloaded file not found at {fsx_file}", file=sys.stderr)
        sys.exit(1)

    actual_size = fsx_file.stat().st_size
    if actual_size != file_size:
        print(f"ERROR: File size mismatch!", file=sys.stderr)
        print(f"  Expected: {format_bytes(file_size)}", file=sys.stderr)
        print(f"  Actual:   {format_bytes(actual_size)}", file=sys.stderr)
        sys.exit(1)

    print("\nFile verified successfully. Ready for GPU processing.")
    return 0


if __name__ == "__main__":
    sys.exit(download_from_s3())
