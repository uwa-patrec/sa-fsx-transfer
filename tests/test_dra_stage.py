"""Tests for the DRA symlink-bridge staging path.

When FSx is configured with a Data Repository Association, input videos are
lazy-loaded from S3 by FSx itself and appear under the import path
(e.g. /fsx/s3input/<tenant>/<project>/<file>). The downloader is repurposed to
symlink that file into the run-scoped dir (/fsx/runs/<job_id>/input/) that the
rest of the pipeline expects — no bytes pass through the container.
"""

import os
import threading

import pytest

import download


# --- path mapping: S3 key -> DRA filesystem path -----------------------------

def test_dra_source_path_strips_import_prefix():
    src = download.dra_source_path(
        "input/tenant1/project1/abc-vid.MP4",
        dra_mount="/fsx/s3input",
        import_prefix="input/",
    )
    assert src == "/fsx/s3input/tenant1/project1/abc-vid.MP4"


def test_dra_source_path_handles_key_without_prefix():
    # Defensive: if the key doesn't start with the prefix, map it verbatim.
    src = download.dra_source_path(
        "tenant1/project1/abc-vid.MP4",
        dra_mount="/fsx/s3input",
        import_prefix="input/",
    )
    assert src == "/fsx/s3input/tenant1/project1/abc-vid.MP4"


# --- staging: symlink the imported file into the run dir ----------------------

def test_stage_via_dra_symlinks_present_file(tmp_path):
    dra_mount = tmp_path / "s3input"
    src = dra_mount / "tenant1" / "project1" / "abc-vid.MP4"
    src.parent.mkdir(parents=True)
    src.write_bytes(b"x" * 2048)

    out = tmp_path / "runs" / "job123" / "input"

    dest = download.stage_via_dra(
        "input/tenant1/project1/abc-vid.MP4",
        str(out),
        dra_mount=str(dra_mount),
        import_prefix="input/",
        expected_size=2048,
        wait_seconds=1,
        poll_seconds=0.02,
    )

    # The run dir now contains the file (via symlink) the way preprocessing globs it.
    assert os.path.islink(dest)
    assert os.path.realpath(dest) == str(src)
    assert os.path.getsize(dest) == 2048
    listed = [p for p in out.iterdir() if p.suffix.lower() == ".mp4"]
    assert [p.name for p in listed] == ["abc-vid.MP4"]


def test_stage_via_dra_waits_then_succeeds_when_import_lands(tmp_path):
    # Simulate auto-import latency: the source appears shortly after staging starts.
    dra_mount = tmp_path / "s3input"
    src = dra_mount / "t" / "p" / "late.MP4"
    out = tmp_path / "runs" / "j" / "input"

    def create_later():
        src.parent.mkdir(parents=True, exist_ok=True)
        src.write_bytes(b"y" * 100)

    threading.Timer(0.15, create_later).start()

    dest = download.stage_via_dra(
        "input/t/p/late.MP4", str(out),
        dra_mount=str(dra_mount), import_prefix="input/",
        expected_size=100, wait_seconds=3, poll_seconds=0.02,
    )
    assert os.path.realpath(dest) == str(src)


def test_stage_via_dra_raises_if_source_never_appears(tmp_path):
    dra_mount = tmp_path / "s3input"
    out = tmp_path / "runs" / "j" / "input"

    with pytest.raises(TimeoutError):
        download.stage_via_dra(
            "input/t/p/missing.MP4", str(out),
            dra_mount=str(dra_mount), import_prefix="input/",
            expected_size=100, wait_seconds=0.2, poll_seconds=0.02,
        )


def test_stage_via_dra_waits_for_correct_size(tmp_path):
    # File present but wrong size (import incomplete) must not be accepted.
    dra_mount = tmp_path / "s3input"
    src = dra_mount / "t" / "p" / "partial.MP4"
    src.parent.mkdir(parents=True)
    src.write_bytes(b"z" * 10)  # expected 100 — never grows
    out = tmp_path / "runs" / "j" / "input"

    with pytest.raises(TimeoutError):
        download.stage_via_dra(
            "input/t/p/partial.MP4", str(out),
            dra_mount=str(dra_mount), import_prefix="input/",
            expected_size=100, wait_seconds=0.2, poll_seconds=0.02,
        )
