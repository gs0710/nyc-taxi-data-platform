"""Unit tests for quality/check_run.py (pure Python, no Docker needed)."""
import json
import sys

import check_run as q

JAN = {
    "rows_in": 3475226,
    "rows_clean": 3330824,
    "rows_rejected_or_quarantined": 144402,
    "counts_by_reason": {
        "clean": 3330824,
        "negative_fare": 144094,
        "distance_over_100_miles": 162,
        "dropoff_before_pickup": 124,
        "pickup_outside_month": 22,
    },
}


def results(summary):
    return {c["name"]: c["passed"] for c in q.evaluate(summary, 0.10, 0.001)}


def run_main(monkeypatch, tmp_path, summary, month="2025-01"):
    runs = tmp_path / "runs"
    runs.mkdir()
    if summary is not None:
        (runs / f"clean_{month}.json").write_text(json.dumps(summary))
    argv = ["check_run.py", "--month", month, "--runs-dir", str(runs),
            "--out-dir", str(tmp_path / "out")]
    monkeypatch.setattr(sys, "argv", argv)
    return q.main()


def test_real_january_numbers_pass_every_check():
    assert all(results(JAN).values())


def test_high_non_clean_rate_fails_only_that_check():
    bad = {**JAN, "rows_clean": 1000, "rows_rejected_or_quarantined": 3474226,
           "counts_by_reason": {"clean": 1000, "negative_fare": 3474226}}
    r = results(bad)
    assert not r["non_clean_rate"]
    assert r["hard_reject_rate"] and r["row_conservation"] and r["reasons_add_up"]


def test_high_hard_reject_rate_fails_even_when_non_clean_rate_is_ok():
    bad = {"rows_in": 1000, "rows_clean": 900, "rows_rejected_or_quarantined": 100,
           "counts_by_reason": {"clean": 900, "dropoff_before_pickup": 100}}
    r = results(bad)
    assert r["non_clean_rate"]
    assert not r["hard_reject_rate"]


def test_lost_row_fails_row_conservation():
    r = results({**JAN, "rows_clean": JAN["rows_clean"] - 1})
    assert not r["row_conservation"]
    assert r["reasons_add_up"]


def test_reasons_that_do_not_add_up_fail():
    reasons = {**JAN["counts_by_reason"], "clean": JAN["counts_by_reason"]["clean"] - 5}
    r = results({**JAN, "counts_by_reason": reasons})
    assert not r["reasons_add_up"]
    assert r["row_conservation"]


def test_empty_run_fails():
    empty = {"rows_in": 0, "rows_clean": 0, "rows_rejected_or_quarantined": 0,
             "counts_by_reason": {}}
    assert not results(empty)["has_rows"]


def test_main_passes_and_writes_a_report(monkeypatch, tmp_path):
    assert run_main(monkeypatch, tmp_path, JAN) == 0
    report = json.loads((tmp_path / "out" / "quality_2025-01.json").read_text())
    assert report["status"] == "passed"
    assert len(report["checks"]) == 5


def test_main_returns_3_and_writes_a_failed_report(monkeypatch, tmp_path):
    bad = {"rows_in": 1000, "rows_clean": 600, "rows_rejected_or_quarantined": 400,
           "counts_by_reason": {"clean": 600, "negative_fare": 300, "dropoff_before_pickup": 100}}
    assert run_main(monkeypatch, tmp_path, bad) == 3
    report = json.loads((tmp_path / "out" / "quality_2025-01.json").read_text())
    assert report["status"] == "failed"


def test_main_returns_2_when_the_summary_is_missing(monkeypatch, tmp_path):
    assert run_main(monkeypatch, tmp_path, None) == 2


def test_main_returns_2_when_the_summary_has_missing_keys(monkeypatch, tmp_path):
    assert run_main(monkeypatch, tmp_path, {"rows_in": 1}) == 2


def test_main_returns_2_for_an_invalid_month(monkeypatch, tmp_path):
    assert run_main(monkeypatch, tmp_path, None, month="2025-13") == 2
