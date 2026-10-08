"""Quality gate: read the Spark run summary and fail the pipeline if the data looks wrong.

Exit codes: 0 = all checks passed, 2 = bad input or missing summary, 3 = a check failed.
"""
import argparse
import json
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger("quality_gate")

# Reasons that mean the source row itself is invalid. They should be very rare.
HARD_REASONS = ("missing_timestamp", "dropoff_before_pickup", "pickup_outside_month")
REQUIRED_KEYS = ("rows_in", "rows_clean", "rows_rejected_or_quarantined", "counts_by_reason")


def evaluate(summary, max_non_clean_rate, max_hard_reject_rate):
    """Return a list of check results for one Spark run summary."""
    rows_in = summary["rows_in"]
    clean = summary["rows_clean"]
    rejected = summary["rows_rejected_or_quarantined"]
    reasons = summary["counts_by_reason"]
    hard = sum(reasons.get(r, 0) for r in HARD_REASONS)
    non_clean_rate = rejected / rows_in if rows_in else 1.0
    hard_rate = hard / rows_in if rows_in else 1.0
    return [
        {"name": "has_rows", "passed": rows_in > 0 and clean > 0,
         "value": rows_in, "threshold": "> 0"},
        {"name": "row_conservation", "passed": clean + rejected == rows_in,
         "value": clean + rejected, "threshold": rows_in},
        {"name": "reasons_add_up", "passed": sum(reasons.values()) == rows_in,
         "value": sum(reasons.values()), "threshold": rows_in},
        {"name": "non_clean_rate", "passed": non_clean_rate <= max_non_clean_rate,
         "value": round(non_clean_rate, 5), "threshold": max_non_clean_rate},
        {"name": "hard_reject_rate", "passed": hard_rate <= max_hard_reject_rate,
         "value": round(hard_rate, 6), "threshold": max_hard_reject_rate},
    ]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", required=True, help="YYYY-MM")
    parser.add_argument("--runs-dir", default="data/processed/_runs")
    parser.add_argument("--out-dir", default="data/processed/_quality")
    parser.add_argument("--max-non-clean-rate", type=float, default=0.10)
    parser.add_argument("--max-hard-reject-rate", type=float, default=0.001)
    args = parser.parse_args()
    logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", args.month):
        log.error("Invalid month %r, expected YYYY-MM", args.month)
        return 2

    path = Path(args.runs_dir) / f"clean_{args.month}.json"
    try:
        summary = json.loads(path.read_text())
        missing = [k for k in REQUIRED_KEYS if k not in summary]
        if missing:
            raise ValueError(f"summary is missing keys: {missing}")
    except (OSError, ValueError) as exc:
        log.error("Cannot read a valid run summary at %s: %s", path, exc)
        return 2

    checks = evaluate(summary, args.max_non_clean_rate, args.max_hard_reject_rate)
    for c in checks:
        log.info("%s  %-18s value=%s threshold=%s",
                 "PASS" if c["passed"] else "FAIL", c["name"], c["value"], c["threshold"])

    status = "passed" if all(c["passed"] for c in checks) else "failed"
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    report = {
        "month": args.month,
        "status": status,
        "checks": checks,
        "evaluated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    (out_dir / f"quality_{args.month}.json").write_text(json.dumps(report, indent=2))

    if status == "failed":
        log.error("QUALITY GATE FAILED for %s", args.month)
        return 3
    log.info("Quality gate passed for %s", args.month)
    return 0


if __name__ == "__main__":
    sys.exit(main())
