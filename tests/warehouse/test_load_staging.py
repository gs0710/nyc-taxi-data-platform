"""Unit tests for warehouse/load_staging.py. No database needed.

They run inside the loader container. Without psycopg (the host venv) they are skipped.
"""
import re
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

pytest.importorskip("psycopg")
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "warehouse"))

import load_staging as ls

SQL_FILE = Path(__file__).resolve().parents[2] / "warehouse" / "init" / "02_tables.sql"


def staging_table_columns():
    """Column names of staging.yellow_trips, read from the CREATE TABLE statement."""
    text = SQL_FILE.read_text()
    body = re.search(r"CREATE TABLE IF NOT EXISTS staging\.yellow_trips \((.*?)\n\);", text, re.DOTALL).group(1)
    return [
        line.split()[0]
        for line in body.splitlines()
        if line.strip() and not line.strip().startswith("--")
    ]


def write_sample(path, rows=3):
    data = {c: pa.array([None] * rows) for c in ls.COLUMNS}
    data["vendor_id"] = pa.array(list(range(rows)))
    pq.write_table(pa.table(data), path)
    return path


# ---------- fakes ----------

class FakeCopy:
    def __init__(self):
        self.rows = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def write_row(self, row):
        self.rows.append(row)


class CopyCursor:
    def __init__(self):
        self.copy_obj = FakeCopy()
        self.sql = None

    def copy(self, sql):
        self.sql = sql
        return self.copy_obj


class FakeCursor:
    def __init__(self, count, log):
        self.count = count
        self.log = log
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.log.append(sql)
        if sql.startswith("DELETE"):
            self.rowcount = 7

    def fetchone(self):
        return (self.count,)


class FakeConn:
    def __init__(self, count, log):
        self.count = count
        self.log = log

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def cursor(self):
        return FakeCursor(self.count, self.log)

    def execute(self, sql, params=None):
        self.log.append(sql)


def setup_load(monkeypatch, tmp_path, db_count):
    path = write_sample(tmp_path / "a.parquet", rows=3)
    log = []
    monkeypatch.setattr(ls, "parquet_files", lambda y, m: [path])
    monkeypatch.setattr(ls, "copy_rows", lambda cur, files, y, m: 3)
    monkeypatch.setattr(ls, "connect", lambda: FakeConn(db_count, log))
    return log


# ---------- tests ----------

def test_copy_columns_match_the_staging_table():
    table = staging_table_columns()
    assert len(ls.COLUMNS) == len(set(ls.COLUMNS))
    assert set(ls.COLUMNS) - set(table) == set()
    assert [c for c in table if c not in ls.COLUMNS] == ["year", "month", "loaded_at"]


def test_parquet_files_reads_only_the_requested_partition(tmp_path, monkeypatch):
    monkeypatch.setattr(ls, "DATA", tmp_path)
    base = tmp_path / "processed" / "yellow_trips"
    for part, name in [
        ("year=2025/month=1", "b.parquet"),
        ("year=2025/month=1", "a.parquet"),
        ("year=2025/month=1", "_SUCCESS"),
        ("year=2025/month=2", "c.parquet"),
    ]:
        folder = base / part
        folder.mkdir(parents=True, exist_ok=True)
        (folder / name).write_bytes(b"x")
    assert [p.name for p in ls.parquet_files(2025, 1)] == ["a.parquet", "b.parquet"]
    assert ls.parquet_files(2025, 3) == []


def test_copy_rows_appends_year_and_month_to_every_row(tmp_path):
    path = write_sample(tmp_path / "a.parquet", rows=3)
    cur = CopyCursor()
    assert ls.copy_rows(cur, [path], 2025, 1) == 3
    assert "staging.yellow_trips" in cur.sql
    assert "year, month" in cur.sql
    assert len(cur.copy_obj.rows) == 3
    assert all(len(r) == len(ls.COLUMNS) + 2 for r in cur.copy_obj.rows)
    assert all(r[-2:] == (2025, 1) for r in cur.copy_obj.rows)


def test_load_month_success_deletes_first_and_logs_success(monkeypatch, tmp_path):
    log = setup_load(monkeypatch, tmp_path, db_count=3)
    assert ls.load_month("2025-01") == 3
    assert log[0].startswith("DELETE FROM staging.yellow_trips")
    assert any("ANALYZE" in s for s in log)
    assert any("'success'" in s for s in log)
    assert not any("'failed'" in s for s in log)


def test_load_month_count_mismatch_logs_failure_not_success(monkeypatch, tmp_path):
    log = setup_load(monkeypatch, tmp_path, db_count=2)  # Postgres "has" 2, Parquet has 3
    with pytest.raises(ls.CountMismatch):
        ls.load_month("2025-01")
    assert not any("'success'" in s for s in log)
    assert not any("ANALYZE" in s for s in log)
    assert any("'failed'" in s for s in log)


def test_main_returns_2_for_an_invalid_month(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["load_staging.py", "--month", "2025-13"])
    assert ls.main() == 2


def test_main_returns_3_on_count_mismatch(monkeypatch):
    def boom(month):
        raise ls.CountMismatch("test")

    monkeypatch.setattr(ls, "load_month", boom)
    monkeypatch.setattr(sys, "argv", ["load_staging.py", "--month", "2025-01"])
    assert ls.main() == 3
