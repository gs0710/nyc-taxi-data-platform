"""Clean one month of NYC yellow taxi trips: raw -> processed (+ rejected)."""
import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

DATA = Path("/app/data")
log = logging.getLogger("clean_trips")

# canonical name -> (lowercase source name, type). Matching on lowercase source
# names makes the job tolerant of Airport_fee vs airport_fee between files.
COLUMNS = {
    "vendor_id": ("vendorid", "int"),
    "pickup_datetime": ("tpep_pickup_datetime", "timestamp_ntz"),
    "dropoff_datetime": ("tpep_dropoff_datetime", "timestamp_ntz"),
    "passenger_count": ("passenger_count", "int"),
    "trip_distance": ("trip_distance", "double"),
    "rate_code_id": ("ratecodeid", "int"),
    "store_and_fwd_flag": ("store_and_fwd_flag", "string"),
    "pickup_location_id": ("pulocationid", "int"),
    "dropoff_location_id": ("dolocationid", "int"),
    "payment_type": ("payment_type", "int"),
    "fare_amount": ("fare_amount", "double"),
    "extra": ("extra", "double"),
    "mta_tax": ("mta_tax", "double"),
    "tip_amount": ("tip_amount", "double"),
    "tolls_amount": ("tolls_amount", "double"),
    "improvement_surcharge": ("improvement_surcharge", "double"),
    "total_amount": ("total_amount", "double"),
    "congestion_surcharge": ("congestion_surcharge", "double"),
    "airport_fee": ("airport_fee", "double"),
    "cbd_congestion_fee": ("cbd_congestion_fee", "double"),
}
QUARANTINE_REASONS = ["negative_fare", "distance_over_100_miles"]


def standardize(df: DataFrame) -> DataFrame:
    """Rename to snake_case, enforce types, fill columns missing from older/newer files."""
    lookup = {c.lower(): c for c in df.columns}
    cols = []
    for name, (src, dtype) in COLUMNS.items():
        if src in lookup:
            cols.append(F.col(lookup[src]).cast(dtype).alias(name))
        else:
            log.warning("Source column %s missing, filling with NULL (schema drift)", src)
            cols.append(F.lit(None).cast(dtype).alias(name))
    return df.select(*cols)


def tag_rows(df: DataFrame, month: str) -> DataFrame:
    """Add reject_reason: NULL means the row is clean. First matching rule wins."""
    y, m = map(int, month.split("-"))
    ny, nm = (y + 1, 1) if m == 12 else (y, m + 1)
    start = F.lit(f"{y:04d}-{m:02d}-01 00:00:00").cast("timestamp_ntz")
    end = F.lit(f"{ny:04d}-{nm:02d}-01 00:00:00").cast("timestamp_ntz")
    pickup, dropoff = F.col("pickup_datetime"), F.col("dropoff_datetime")

    reason = (
        F.when(pickup.isNull() | dropoff.isNull(), "missing_timestamp")
        .when(dropoff < pickup, "dropoff_before_pickup")
        .when((pickup < start) | (pickup >= end), "pickup_outside_month")
        .when(F.col("trip_distance") > 100, "distance_over_100_miles")
        .when(F.col("fare_amount") < 0, "negative_fare")
    )
    return df.withColumn("reject_reason", reason)


def enrich(clean: DataFrame, zones: DataFrame) -> DataFrame:
    """Add zone names (broadcast left join) plus flags and derived columns."""
    pu = zones.select(
        F.col("LocationID").cast("int").alias("pickup_location_id"),
        F.col("Borough").alias("pickup_borough"),
        F.col("Zone").alias("pickup_zone"),
    )
    do = zones.select(
        F.col("LocationID").cast("int").alias("dropoff_location_id"),
        F.col("Borough").alias("dropoff_borough"),
        F.col("Zone").alias("dropoff_zone"),
    )
    return (
        clean.join(F.broadcast(pu), "pickup_location_id", "left")
        .join(F.broadcast(do), "dropoff_location_id", "left")
        .withColumn("passenger_count_missing", F.col("passenger_count").isNull())
        .withColumn("zero_distance", F.col("trip_distance") == 0)
        .withColumn(
            "trip_duration_minutes",
            F.expr("timestampdiff(SECOND, pickup_datetime, dropoff_datetime) / 60.0"),
        )
        .withColumn("year", F.year("pickup_datetime"))
        .withColumn("month", F.month("pickup_datetime"))
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", required=True, help="YYYY-MM")
    args = parser.parse_args()
    logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", args.month):
        log.error("Invalid month %r", args.month)
        return 2
    month = args.month
    y, m = map(int, month.split("-"))

    raw_path = DATA / "raw" / "yellow" / month / f"yellow_tripdata_{month}.parquet"
    zones_path = DATA / "raw" / "zones" / "taxi_zone_lookup.csv"
    clean_path = DATA / "processed" / "yellow_trips"
    rejected_path = DATA / "processed" / "rejected_trips"
    for p in (raw_path, zones_path):
        if not p.exists():
            log.error("Missing input file: %s", p)
            return 1

    spark = (
        SparkSession.builder.appName(f"clean_trips_{month}")
        .master("local[4]")
        .config("spark.driver.memory", "3g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    started = time.time()

    try:
        raw = spark.read.parquet(str(raw_path))
        zones = spark.read.csv(str(zones_path), header=True)
        total_in = raw.count()
        log.info("Rows in raw file: %d", total_in)

        tagged = tag_rows(standardize(raw), month)

        counts = {
            r["reason"]: r["n"]
            for r in tagged.groupBy(F.coalesce(F.col("reject_reason"), F.lit("clean")).alias("reason"))
            .agg(F.count("*").alias("n"))
            .collect()
        }
        for reason, n in sorted(counts.items(), key=lambda kv: -kv[1]):
            log.info("  %-26s %10d  (%.3f%%)", reason, n, 100 * n / total_in)

        clean = enrich(tagged.filter(F.col("reject_reason").isNull()).drop("reject_reason"), zones)
        rejected = (
            tagged.filter(F.col("reject_reason").isNotNull())
            .withColumn(
                "disposition",
                F.when(F.col("reject_reason").isin(QUARANTINE_REASONS), "quarantined").otherwise("rejected"),
            )
            .withColumn("source_month", F.lit(month))
        )

        clean.write.mode("overwrite").partitionBy("year", "month").parquet(str(clean_path))
        rejected.write.mode("overwrite").partitionBy("source_month").parquet(str(rejected_path))

        # Verify by reading the outputs back (independent of the in-memory plan).
        clean_out = (
            spark.read.parquet(str(clean_path))
            .filter((F.col("year") == y) & (F.col("month") == m))
            .count()
        )
        rejected_out = (
            spark.read.parquet(str(rejected_path)).filter(F.col("source_month") == month).count()
        )
        log.info("Written: clean=%d rejected=%d (sum=%d, raw=%d)",
                 clean_out, rejected_out, clean_out + rejected_out, total_in)

        if clean_out + rejected_out != total_in or sum(counts.values()) != total_in:
            log.error("ROW COUNT MISMATCH: rows were lost or duplicated")
            return 3

        summary = {
            "month": month,
            "rows_in": total_in,
            "rows_clean": clean_out,
            "rows_rejected_or_quarantined": rejected_out,
            "counts_by_reason": counts,
            "finished_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "duration_seconds": round(time.time() - started, 1),
        }
        runs_dir = DATA / "processed" / "_runs"
        runs_dir.mkdir(parents=True, exist_ok=True)
        (runs_dir / f"clean_{month}.json").write_text(json.dumps(summary, indent=2))
        log.info("OK: %s finished in %.1fs", month, time.time() - started)
        return 0
    except Exception:
        log.exception("clean_trips failed for %s", month)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
