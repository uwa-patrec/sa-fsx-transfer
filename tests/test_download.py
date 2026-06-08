"""Tests for the memory-bounded chunked S3 -> FSx download.

The download must produce a byte-exact copy of the source object regardless of
how the object is split into ranged GET parts. These tests drive the chunking
correctness (the page-cache eviction behaviour is Linux-only and not asserted
here — see download.py for the os.posix_fadvise guard).
"""

import boto3
import pytest
from moto import mock_aws

import download


BUCKET = "test-bucket"
REGION = "ap-southeast-2"


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name=REGION)
        client.create_bucket(
            Bucket=BUCKET,
            CreateBucketConfiguration={"LocationConstraint": REGION},
        )
        yield client


def _put(s3, key, data):
    s3.put_object(Bucket=BUCKET, Key=key, Body=data)


def test_download_object_multipart_is_byte_exact(s3, tmp_path):
    # 3500 bytes with a 1024-byte part size => 4 ranges, last one 428 bytes.
    data = bytes((i * 31 + 7) % 256 for i in range(3500))
    _put(s3, "vid.bin", data)
    dest = tmp_path / "out.bin"

    download.download_object(s3, BUCKET, "vid.bin", str(dest), len(data), part_size=1024)

    assert dest.read_bytes() == data


def test_download_object_single_part(s3, tmp_path):
    data = b"hello fsx" * 5  # 45 bytes, smaller than one part
    _put(s3, "small.bin", data)
    dest = tmp_path / "small.bin"

    download.download_object(s3, BUCKET, "small.bin", str(dest), len(data), part_size=1024)

    assert dest.read_bytes() == data


def test_download_object_exact_multiple_of_part_size(s3, tmp_path):
    # Boundary: size is an exact multiple of part_size (no short final part).
    data = bytes(range(256)) * 8  # 2048 bytes, part_size 1024 => exactly 2 parts
    _put(s3, "aligned.bin", data)
    dest = tmp_path / "aligned.bin"

    download.download_object(s3, BUCKET, "aligned.bin", str(dest), len(data), part_size=1024)

    assert dest.read_bytes() == data


def test_download_object_empty_file(s3, tmp_path):
    _put(s3, "empty.bin", b"")
    dest = tmp_path / "empty.bin"

    download.download_object(s3, BUCKET, "empty.bin", str(dest), 0, part_size=1024)

    assert dest.exists()
    assert dest.stat().st_size == 0


def test_download_object_raises_when_object_missing(s3, tmp_path):
    # A part that cannot be fetched must surface as an exception so the caller
    # (download_from_s3) reports failure and exits non-zero — not a silent
    # truncated file.
    dest = tmp_path / "missing.bin"

    with pytest.raises(Exception):
        download.download_object(
            s3, BUCKET, "does-not-exist.bin", str(dest), 100, part_size=1024
        )


def test_download_object_byte_exact_across_multiple_evictions(s3, tmp_path):
    # Force the periodic flush/drop-cache branch to run several times mid-download
    # by setting evict_bytes well below the file size, and assert the result is
    # still byte-exact.
    data = bytes((i * 17 + 3) % 256 for i in range(5000))
    _put(s3, "big.bin", data)
    dest = tmp_path / "big.bin"

    download.download_object(
        s3, BUCKET, "big.bin", str(dest), len(data),
        part_size=512, max_workers=4, evict_bytes=1024,
    )

    assert dest.read_bytes() == data
