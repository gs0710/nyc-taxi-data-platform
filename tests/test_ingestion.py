import json

import download_tlc as d
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests

# ---------- validation ----------

def test_validate_zones_accepts_good_file(tmp_path):
    f = tmp_path / "zones.csv"
    f.write_text("LocationID,Borough,Zone\n" + "\n".join(f"{i},X,Y" for i in range(1, 266)))
    assert d.validate_zones(f) == 265


def test_validate_zones_rejects_bad_header(tmp_path):
    f = tmp_path / "zones.csv"
    f.write_text("<html>Access Denied</html>\n")
    with pytest.raises(d.ValidationError):
        d.validate_zones(f)


def test_validate_zones_rejects_too_few_rows(tmp_path):
    f = tmp_path / "zones.csv"
    f.write_text("LocationID,Borough,Zone\n1,X,Y\n")
    with pytest.raises(d.ValidationError):
        d.validate_zones(f)


def test_validate_parquet_returns_row_count(tmp_path):
    f = tmp_path / "t.parquet"
    pq.write_table(pa.table({"a": [1, 2, 3]}), f)
    assert d.validate_parquet(f) == 3


def test_validate_parquet_rejects_garbage(tmp_path):
    f = tmp_path / "t.parquet"
    f.write_bytes(b"this is not parquet")
    with pytest.raises(d.ValidationError):
        d.validate_parquet(f)


# ---------- retries ----------

def test_retries_temporary_failure_then_succeeds(monkeypatch, tmp_path):
    calls = []

    def flaky(url, tmp):
        calls.append(1)
        if len(calls) < 3:
            raise requests.ConnectionError("network down")
        return 10, "abc"

    monkeypatch.setattr(d, "stream_to_file", flaky)
    monkeypatch.setattr(d.time, "sleep", lambda s: None)  # do not really wait
    assert d.fetch_with_retries("http://x", tmp_path / "f") == (10, "abc")
    assert len(calls) == 3


def test_gives_up_after_max_attempts(monkeypatch, tmp_path):
    calls = []

    def always_fails(url, tmp):
        calls.append(1)
        raise requests.Timeout("slow")

    monkeypatch.setattr(d, "stream_to_file", always_fails)
    monkeypatch.setattr(d.time, "sleep", lambda s: None)
    with pytest.raises(requests.Timeout):
        d.fetch_with_retries("http://x", tmp_path / "f")
    assert len(calls) == d.MAX_ATTEMPTS


def test_does_not_retry_404(monkeypatch, tmp_path):
    calls = []

    def not_found(url, tmp):
        calls.append(1)
        resp = requests.Response()
        resp.status_code = 404
        raise requests.HTTPError(response=resp)

    monkeypatch.setattr(d, "stream_to_file", not_found)
    monkeypatch.setattr(d.time, "sleep", lambda s: None)
    with pytest.raises(requests.HTTPError):
        d.fetch_with_retries("http://x", tmp_path / "f")
    assert len(calls) == 1


# ---------- ingest: idempotency, cleanup, metadata ----------

def test_ingest_skips_existing_file(monkeypatch, tmp_path):
    dest = tmp_path / "raw" / "f.parquet"
    dest.parent.mkdir(parents=True)
    dest.write_bytes(b"already here")

    def must_not_be_called(url, tmp):
        raise AssertionError("should have skipped the download")

    monkeypatch.setattr(d, "fetch_with_retries", must_not_be_called)
    d.ingest("http://x", dest, lambda p: 1, "demo", force=False)
    assert dest.read_bytes() == b"already here"


def test_failed_validation_leaves_no_files_or_folders(monkeypatch, tmp_path):
    dest = tmp_path / "raw" / "2025-01" / "f.parquet"

    def fake_fetch(url, tmp):
        tmp.write_bytes(b"x")
        return 1, "abc"

    def bad_validate(path):
        raise d.ValidationError("bad file")

    monkeypatch.setattr(d, "fetch_with_retries", fake_fetch)
    with pytest.raises(d.ValidationError):
        d.ingest("http://x", dest, bad_validate, "demo", force=False)

    assert not dest.exists()
    assert not dest.with_name(dest.name + ".part").exists()
    assert not dest.parent.exists()  # the empty month folder was removed


def test_successful_ingest_writes_metadata(monkeypatch, tmp_path):
    dest = tmp_path / "raw" / "2025-01" / "f.parquet"
    meta = tmp_path / "meta" / "log.jsonl"

    def fake_fetch(url, tmp):
        tmp.write_bytes(b"data")
        return 4, "deadbeef"

    monkeypatch.setattr(d, "fetch_with_retries", fake_fetch)
    monkeypatch.setattr(d, "METADATA_FILE", meta)
    d.ingest("http://x", dest, lambda p: 5, "demo", force=False)

    assert dest.read_bytes() == b"data"
    record = json.loads(meta.read_text().splitlines()[0])
    assert record["dataset"] == "demo"
    assert record["rows"] == 5
    assert record["sha256"] == "deadbeef"
