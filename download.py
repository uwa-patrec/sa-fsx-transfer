#!/usr/bin/env python3
"""
Stage 0: S3 to FSx Downloader

This script downloads a file from S3 to FSx using a memory-bounded, parallel
ranged download. It's designed to run on cheap CPU instances before GPU
processing begins.

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
from concurrent.futures import ThreadPoolExecutor, as_completed


# Download tuning.
#
# Memory note: the previous implementation used boto3 download_file, which writes
# to the destination with default buffered I/O. Writing a multi-GB video to the
# FSx Lustre mount accumulates dirty page-cache pages that are charged to the
# container's memory cgroup. On a 3.5 GB file in a 2 GB container this overflowed
# the cgroup ("[Errno 12] Cannot allocate memory", exit 1) — independent of
# boto3's part buffers. Past a few GB no container size is large enough, because
# dirty pages grow while ingest outpaces FSx writeback.
#
# This implementation keeps memory bounded regardless of file size: it downloads
# fixed-size ranges in parallel (to saturate FSx throughput) and, as parts land,
# periodically flushes written data to FSx and drops it from the page cache via
# posix_fadvise(DONTNEED). Peak resident pages stay ~= a couple of EVICT_BYTES.
DEFAULT_PART_SIZE = 32 * 1024 * 1024     # 32 MB per ranged GET
DEFAULT_MAX_WORKERS = 8                   # parallel S3 connections
DEFAULT_READ_SIZE = 8 * 1024 * 1024      # 8 MB socket read granularity
DEFAULT_EVICT_BYTES = 256 * 1024 * 1024  # flush + drop page cache every ~256 MB
_PART_RETRIES = 3


def format_bytes(bytes_val):
    """Convert bytes to human-readable format"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_val < 1024.0:
            return f"{bytes_val:.2f} {unit}"
        bytes_val /= 1024.0
    return f"{bytes_val:.2f} PB"


def _drop_cache(fd):
    """Flush dirty pages to FSx and evict the whole file from the page cache.

    posix_fadvise(DONTNEED) only drops clean pages, so fdatasync first to turn
    written (dirty) pages into clean ones. Both are Linux-only; on platforms
    without them (e.g. macOS dev machines) this is a no-op and the download still
    produces a correct file — it just won't bound the page cache there.
    """
    try:
        os.fdatasync(fd)
    except (OSError, AttributeError):
        pass
    if hasattr(os, "posix_fadvise"):
        try:
            # offset 0, length 0 => to end of file
            os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
        except OSError:
            pass


def _download_part(s3_client, bucket, key, fd, start, end):
    """Download bytes [start, end] (inclusive) and pwrite them at their offset."""
    last_err = None
    for _ in range(_PART_RETRIES):
        try:
            resp = s3_client.get_object(
                Bucket=bucket, Key=key, Range=f"bytes={start}-{end}"
            )
            body = resp["Body"]
            offset = start
            while True:
                data = body.read(DEFAULT_READ_SIZE)
                if not data:
                    break
                os.pwrite(fd, data, offset)
                offset += len(data)
            return end - start + 1
        except Exception as e:  # noqa: BLE001 — retry any transient S3/socket error
            last_err = e
    raise last_err


def download_object(
    s3_client,
    bucket,
    key,
    dest_path,
    file_size,
    *,
    part_size=DEFAULT_PART_SIZE,
    max_workers=DEFAULT_MAX_WORKERS,
    evict_bytes=DEFAULT_EVICT_BYTES,
):
    """Download an S3 object to dest_path with bounded memory usage.

    Ranges are fetched in parallel and written directly to the destination fd;
    the page cache is periodically flushed and dropped so dirty pages cannot
    accumulate against the container's memory cgroup.
    """
    Path(dest_path).parent.mkdir(parents=True, exist_ok=True)

    fd = os.open(dest_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        if file_size == 0:
            return

        os.ftruncate(fd, file_size)

        parts = [
            (start, min(start + part_size, file_size) - 1)
            for start in range(0, file_size, part_size)
        ]

        done_bytes = 0
        last_evict = 0
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(_download_part, s3_client, bucket, key, fd, s, e): (s, e)
                for s, e in parts
            }
            for fut in as_completed(futures):
                done_bytes += fut.result()  # propagate part failures
                if done_bytes - last_evict >= evict_bytes:
                    _drop_cache(fd)
                    last_evict = done_bytes
    finally:
        _drop_cache(fd)
        os.close(fd)


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

    # Download file (memory-bounded, parallel ranged GETs)
    start_time = datetime.now()

    try:
        download_object(s3_client, s3_bucket, s3_key, str(fsx_file), file_size)
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
