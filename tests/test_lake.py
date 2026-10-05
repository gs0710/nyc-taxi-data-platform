from pathlib import Path

import pytest
import upload_to_lake as u
from botocore.exceptions import ClientError


def client_error(code, operation):
    return ClientError({"Error": {"Code": code, "Message": code}}, operation)


class FakeS3:
    """Minimal in-memory stand-in for the S3 client."""

    def __init__(self, existing=None, bucket_exists=True):
        self.objects = dict(existing or {})  # key -> size in bytes
        self.uploaded = []
        self.bucket_exists = bucket_exists
        self.created = False

    def head_bucket(self, Bucket):
        if not self.bucket_exists:
            raise client_error("404", "HeadBucket")
        return {}

    def create_bucket(self, Bucket):
        self.created = True

    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise client_error("404", "HeadObject")
        return {"ContentLength": self.objects[Key]}

    def upload_file(self, filename, bucket, key):
        self.objects[key] = Path(filename).stat().st_size
        self.uploaded.append(key)


def make_raw(tmp_path, monkeypatch):
    """Create a small fake local raw layer, including files that must be ignored."""
    monkeypatch.setattr(u, "DATA_DIR", tmp_path)
    raw = tmp_path / "raw"
    (raw / "yellow" / "2025-01").mkdir(parents=True)
    (raw / "zones").mkdir()
    (raw / "_metadata").mkdir()
    (raw / "yellow" / "2025-01" / "a.parquet").write_bytes(b"aaaa")
    (raw / "yellow" / "2025-01" / "a.parquet.part").write_bytes(b"half")
    (raw / "zones" / "z.csv").write_bytes(b"LocationID\n1\n")
    (raw / "_metadata" / "ingestion_log.jsonl").write_text("{}\n")
    return raw


def test_object_key_keeps_layer_and_partition_path(tmp_path, monkeypatch):
    raw = make_raw(tmp_path, monkeypatch)
    path = raw / "yellow" / "2025-01" / "a.parquet"
    assert u.object_key(path) == "raw/yellow/2025-01/a.parquet"


def test_remote_size_returns_none_when_missing():
    assert u.remote_size(FakeS3(), "b", "nope") is None


def test_remote_size_returns_size_when_present():
    assert u.remote_size(FakeS3({"k": 42}), "b", "k") == 42


def test_remote_size_reraises_unexpected_errors():
    class Forbidden(FakeS3):
        def head_object(self, Bucket, Key):
            raise client_error("403", "HeadObject")

    with pytest.raises(ClientError):
        u.remote_size(Forbidden(), "b", "k")


def test_ensure_bucket_creates_when_missing():
    s3 = FakeS3(bucket_exists=False)
    u.ensure_bucket(s3, "b")
    assert s3.created


def test_ensure_bucket_does_nothing_when_present():
    s3 = FakeS3(bucket_exists=True)
    u.ensure_bucket(s3, "b")
    assert not s3.created


def test_upload_all_is_idempotent_and_ignores_temp_files(tmp_path, monkeypatch):
    make_raw(tmp_path, monkeypatch)
    s3 = FakeS3()

    u.upload_all(s3, "b", force=False)
    assert sorted(s3.uploaded) == ["raw/yellow/2025-01/a.parquet", "raw/zones/z.csv"]

    u.upload_all(s3, "b", force=False)  # second run must change nothing
    assert len(s3.uploaded) == 2


def test_upload_all_force_reuploads(tmp_path, monkeypatch):
    make_raw(tmp_path, monkeypatch)
    s3 = FakeS3()
    u.upload_all(s3, "b", force=False)
    u.upload_all(s3, "b", force=True)
    assert len(s3.uploaded) == 4


def test_changed_size_triggers_reupload(tmp_path, monkeypatch):
    make_raw(tmp_path, monkeypatch)
    s3 = FakeS3({"raw/yellow/2025-01/a.parquet": 999})  # wrong size in the lake
    u.upload_all(s3, "b", force=False)
    assert "raw/yellow/2025-01/a.parquet" in s3.uploaded
    assert s3.objects["raw/yellow/2025-01/a.parquet"] == 4
