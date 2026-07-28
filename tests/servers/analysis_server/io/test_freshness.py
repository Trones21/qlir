import os

import pytest

from qlir.servers.analysis_server.io.freshness import dir_data_fingerprint

pytestmark = pytest.mark.analysis_server


def test_empty_dir_is_zero(tmp_path):
    assert dir_data_fingerprint(tmp_path) == (0, 0)


def test_ignores_non_parquet(tmp_path):
    (tmp_path / "notes.txt").write_bytes(b"x")
    assert dir_data_fingerprint(tmp_path) == (0, 0)


def test_stable_when_unchanged(tmp_path):
    (tmp_path / "head.parquet").write_bytes(b"x")
    fp1 = dir_data_fingerprint(tmp_path)
    fp2 = dir_data_fingerprint(tmp_path)
    assert fp1 == fp2
    assert fp1[0] == 1


def test_changes_when_file_added(tmp_path):
    (tmp_path / "a.parquet").write_bytes(b"x")
    fp1 = dir_data_fingerprint(tmp_path)
    (tmp_path / "b.parquet").write_bytes(b"y")
    fp2 = dir_data_fingerprint(tmp_path)
    assert fp2[0] == fp1[0] + 1
    assert fp2 != fp1


def test_changes_when_head_mtime_bumps(tmp_path):
    """Head growing (append) bumps mtime -> fingerprint changes -> ETL runs."""
    head = tmp_path / "head.parquet"
    head.write_bytes(b"x")
    fp1 = dir_data_fingerprint(tmp_path)

    # Bump mtime deterministically rather than relying on clock resolution.
    future_ns = fp1[1] + 1_000_000_000
    os.utime(head, ns=(future_ns, future_ns))

    fp2 = dir_data_fingerprint(tmp_path)
    assert fp2[1] > fp1[1]
    assert fp2 != fp1


def test_rotation_changes_fingerprint(tmp_path):
    """Head rotating into a sealed chunk (+ new head) changes the file count."""
    (tmp_path / "head.parquet").write_bytes(b"x")
    fp1 = dir_data_fingerprint(tmp_path)

    # old head sealed into a numbered chunk, fresh head appears
    (tmp_path / "chunk_0001.parquet").write_bytes(b"x")
    fp2 = dir_data_fingerprint(tmp_path)
    assert fp2 != fp1
    assert fp2[0] == 2
