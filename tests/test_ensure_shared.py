"""Tests for ensure_shared_object: idempotent copy of a shared artifact
(e.g. the detection TensorRT engine) from S3 to a fixed FSx path.

The downloader runs first in the pipeline, so it's a natural place to make
sure shared prerequisites exist on FSx after a filesystem rebuild — copy the
object only when it's missing or the wrong size; skip otherwise.
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
        c = boto3.client("s3", region_name=REGION)
        c.create_bucket(Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": REGION})
        yield c


def test_copies_when_missing(s3, tmp_path):
    data = bytes((i * 13 + 1) % 256 for i in range(5000))
    s3.put_object(Bucket=BUCKET, Key="models/engine.bin", Body=data)
    dest = tmp_path / "shared" / "engine" / "engine.bin"

    copied = download.ensure_shared_object(s3, BUCKET, "models/engine.bin", str(dest))

    assert copied is True
    assert dest.read_bytes() == data


def test_skips_when_present_and_correct_size(s3, tmp_path):
    data = b"engine-bytes" * 100
    s3.put_object(Bucket=BUCKET, Key="models/engine.bin", Body=data)
    dest = tmp_path / "shared" / "engine" / "engine.bin"
    dest.parent.mkdir(parents=True)
    dest.write_bytes(data)  # already present, correct size

    copied = download.ensure_shared_object(s3, BUCKET, "models/engine.bin", str(dest))

    assert copied is False           # skipped, no re-copy
    assert dest.read_bytes() == data


def test_recopies_when_present_but_wrong_size(s3, tmp_path):
    data = b"x" * 4096
    s3.put_object(Bucket=BUCKET, Key="models/engine.bin", Body=data)
    dest = tmp_path / "shared" / "engine" / "engine.bin"
    dest.parent.mkdir(parents=True)
    dest.write_bytes(b"truncated")   # wrong size -> must be corrected

    copied = download.ensure_shared_object(s3, BUCKET, "models/engine.bin", str(dest))

    assert copied is True
    assert dest.read_bytes() == data
