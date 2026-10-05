"""Load one month of cleaned trips (Parquet) into Postgres staging, idempotently."""
import argparse
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg
import pyarrow.parquet as pq

DATA = Path(os.getenv("DATA_DIR", "/app/data"))
BATCH_ROWS = 100_000
log = logging.getLogger("load_staging")

# Columns stored inside the Parquet files, in the order we COPY them.
# year and month are NOT in the files: Spark encodes them in the folder names.
COLUMNS = [
    "vendor_id", "pickup_datetime", "dropoff_datetime", "passenger_count",
    "trip_distance", "rate_code_id", "store_and_fwd_flag", "pickup_location_id",
    "dropoff_location_id", "payment_type", "fare_amount", "extra", "mta_tax",
    "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount",
    "congestion_surcharge", "airport_fee", "cbd_congestion_fee", "pickup_borough",
    "pickup_zone", "dropoff_borough", "dropoff_zone", "passenger_count_missing",
    "zero_distance", "trip_duration_minutes",
]


class CountMismatch(Exception):
    """Rows in Postgres do not match rows in the Parquet files."""


def connect():
    return psycopg.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def parquet_files(year, month):
    folder = DATA / "processed" / "yellow_trips" / f"year={year}" / f"month={month}"
    return sorted(folder.glob("*.parquet"))


def copy_rows(cur, files, year, month):
    """Stream every row of the Parquet files into Postgres with COPY."""
    cols = ", ".join(COLUMNS + ["year", "month"])
    sent = 0
    with cur.copy(f"COPY staging.yellow_trips ({cols}) FROM STDIN") as copy:
        for path in files:
            parquet = pq.ParquetFile(path)
            for batch in parquet.iter_batches(batch_size=BATCH_ROWS, columns=COLUMNS):
                data = batch.to_pydict()
                for row in zip(*(data[c] for c in COLUMNS)):
                    copy.write_row((*row, year, month))
                sent += batch.num_rows
    return sent


def record_failure(month, expected, started, message):
    """Log the failed attempt in its own transaction (the load was rolled back)."""
    try:
        with connect() as conn:
            conn.execute(
                "INSERT INTO audit.load_history (dataset, source_month, rows_in_parquet,"
                " rows_loaded, status, error_message, started_at, finished_at)"
                " VALUES ('yellow_trips', %s, %s, NULL, 'failed', %s, %s, now())",
                (month, expected, message[:1000], started),
            )
    except Exception:
        log.exception("Could not write the failure to audit.load_history")


def load_month(month):
    year, mon = map(int, month.split("-"))
    files = parquet_files(year, mon)
    if not files:
        raise FileNotFoundError(f"No processed Parquet files for {month}")
    expected = sum(pq.ParquetFile(f).metadata.num_rows for f in files)
    started = datetime.now(timezone.utc)
    log.info("Loading %s: %d file(s), %d rows in Parquet", month, len(files), expected)

    try:
        # One transaction: commits when the block ends, rolls back on any exception.
        with connect() as conn, conn.cursor() as cur:
            cur.execute(
                "DELETE FROM staging.yellow_trips WHERE year = %s AND month = %s",
                (year, mon),
            )
            log.info("Removed %d existing rows for %s (idempotent reload)", cur.rowcount, month)

            sent = copy_rows(cur, files, year, mon)
            log.info("COPY sent %d rows", sent)

            cur.execute(
                "SELECT count(*) FROM staging.yellow_trips WHERE year = %s AND month = %s",
                (year, mon),
            )
            loaded = cur.fetchone()[0]
            if loaded != expected:
                raise CountMismatch(f"Postgres has {loaded} rows but Parquet has {expected}")

            cur.execute("ANALYZE staging.yellow_trips")
            cur.execute(
                "INSERT INTO audit.load_history (dataset, source_month, rows_in_parquet,"
                " rows_loaded, status, started_at, finished_at)"
                " VALUES ('yellow_trips', %s, %s, %s, 'success', %s, now())",
                (month, expected, loaded, started),
            )
    except Exception as exc:
        record_failure(month, expected, started, f"{type(exc).__name__}: {exc}")
        raise
    return loaded


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", required=True, help="YYYY-MM")
    args = parser.parse_args()
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", args.month):
        log.error("Invalid month %r, expected YYYY-MM", args.month)
        return 2

    started = time.time()
    try:
        loaded = load_month(args.month)
    except CountMismatch as exc:
        log.error("ROW COUNT MISMATCH, load rolled back: %s", exc)
        return 3
    except (FileNotFoundError, psycopg.Error) as exc:
        log.error("Load failed: %s", exc)
        return 1
    log.info("OK: %s loaded %d rows in %.1fs", args.month, loaded, time.time() - started)
    return 0


if __name__ == "__main__":
    sys.exit(main())
