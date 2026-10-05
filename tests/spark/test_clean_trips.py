"""Unit tests for the cleaning rules in spark/clean_trips.py.

These tests need PySpark, so they run inside the Spark container. On a machine
without PySpark (for example the host virtual environment) they are skipped.
"""
import sys
from datetime import datetime
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "spark"))

import clean_trips as c
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StructField,
    StructType,
    TimestampNTZType,
)

RAW_SCHEMA = StructType([
    StructField("tpep_pickup_datetime", TimestampNTZType()),
    StructField("tpep_dropoff_datetime", TimestampNTZType()),
    StructField("trip_distance", DoubleType()),
    StructField("fare_amount", DoubleType()),
    StructField("PULocationID", IntegerType()),
    StructField("DOLocationID", IntegerType()),
    StructField("passenger_count", LongType()),
    StructField("Airport_fee", DoubleType()),
])


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("test_clean_trips")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "1g")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def dt(text):
    return datetime.fromisoformat(text) if text else None


def make_row(pickup, dropoff, dist=1.0, fare=10.0, pu=1, do=2, pax=1, airport=0.0):
    """One raw trip as a tuple in RAW_SCHEMA order. Times are 'YYYY-MM-DD HH:MM:SS'."""
    return (dt(pickup), dt(dropoff), dist, fare, pu, do, pax, airport)


def raw_df(spark, rows):
    return spark.createDataFrame(rows, RAW_SCHEMA)


def reasons(spark, rows, month):
    tagged = c.tag_rows(c.standardize(raw_df(spark, rows)), month)
    return [r["reject_reason"] for r in tagged.collect()]


# ---------- standardize ----------

def test_standardize_renames_and_casts_columns(spark):
    row = make_row("2025-01-15 10:00:00", "2025-01-15 10:30:00", airport=5.0)
    out = c.standardize(raw_df(spark, [row]))
    assert out.columns == list(c.COLUMNS.keys())
    types = dict(out.dtypes)
    assert types["pickup_datetime"] == "timestamp_ntz"
    assert types["passenger_count"] == "int"
    assert types["trip_distance"] == "double"
    assert out.collect()[0]["airport_fee"] == 5.0


def test_standardize_matches_source_columns_case_insensitively(spark):
    row = make_row("2025-01-15 10:00:00", "2025-01-15 10:30:00", airport=7.0)
    df = raw_df(spark, [row]).withColumnRenamed("Airport_fee", "airport_fee")
    assert c.standardize(df).collect()[0]["airport_fee"] == 7.0


def test_standardize_fills_missing_columns_with_null(spark):
    row = make_row("2025-01-15 10:00:00", "2025-01-15 10:30:00")
    out = c.standardize(raw_df(spark, [row]))
    assert "cbd_congestion_fee" in out.columns  # not in the raw data
    assert out.filter(F.col("cbd_congestion_fee").isNotNull()).count() == 0


# ---------- tag_rows: one case per rule, plus boundaries and rule order ----------

CASES = [
    pytest.param("2025-01", make_row("2025-01-15 10:00:00", "2025-01-15 10:30:00"),
                 None, id="clean_row"),
    pytest.param("2025-01", make_row("2025-01-15 10:30:00", "2025-01-15 10:00:00"),
                 "dropoff_before_pickup", id="dropoff_before_pickup"),
    pytest.param("2025-01", make_row("2024-12-31 23:59:59", "2025-01-01 00:10:00"),
                 "pickup_outside_month", id="pickup_just_before_month"),
    pytest.param("2025-01", make_row("2025-02-01 00:00:00", "2025-02-01 00:10:00"),
                 "pickup_outside_month", id="next_month_start_is_outside"),
    pytest.param("2025-01", make_row("2025-01-01 00:00:00", "2025-01-01 00:10:00"),
                 None, id="month_start_is_clean"),
    pytest.param("2025-01", make_row("2025-01-15 10:00:00", "2025-01-15 12:00:00", dist=150.0),
                 "distance_over_100_miles", id="distance_over_100"),
    pytest.param("2025-01", make_row("2025-01-15 10:00:00", "2025-01-15 12:00:00", dist=100.0),
                 None, id="distance_exactly_100_is_clean"),
    pytest.param("2025-01", make_row("2025-01-15 10:00:00", "2025-01-15 10:30:00", fare=-5.0),
                 "negative_fare", id="negative_fare"),
    pytest.param("2025-01", make_row(None, "2025-01-15 10:30:00"),
                 "missing_timestamp", id="missing_pickup"),
    pytest.param("2025-01", make_row("2025-01-15 10:00:00", None),
                 "missing_timestamp", id="missing_dropoff"),
    pytest.param("2025-01", make_row("2025-01-15 10:30:00", "2025-01-15 10:00:00", fare=-5.0),
                 "dropoff_before_pickup", id="dropoff_rule_beats_negative_fare"),
    pytest.param("2025-01", make_row("2025-01-15 10:00:00", "2025-01-15 12:00:00",
                                     dist=150.0, fare=-5.0),
                 "distance_over_100_miles", id="distance_rule_beats_negative_fare"),
    pytest.param("2024-12", make_row("2024-12-31 23:00:00", "2024-12-31 23:30:00"),
                 None, id="december_last_day_is_clean"),
    pytest.param("2024-12", make_row("2025-01-01 00:00:00", "2025-01-01 00:10:00"),
                 "pickup_outside_month", id="december_rollover_to_january"),
    pytest.param("2024-12", make_row("2024-12-31 23:00:00", "2024-12-31 22:00:00"),
                 "dropoff_before_pickup", id="dropoff_rule_beats_outside_month"),
]


@pytest.mark.parametrize("month,row,expected", CASES)
def test_tag_rows_assigns_the_expected_reason(spark, month, row, expected):
    assert reasons(spark, [row], month) == [expected]


def test_tagging_never_changes_the_row_count(spark):
    rows = [
        make_row("2025-01-15 10:00:00", "2025-01-15 10:30:00"),
        make_row("2025-01-15 10:00:00", "2025-01-15 10:30:00", fare=-1.0),
        make_row("2025-01-15 10:30:00", "2025-01-15 10:00:00"),
    ]
    tagged = c.tag_rows(c.standardize(raw_df(spark, rows)), "2025-01")
    assert tagged.count() == len(rows)


def test_quarantine_reasons_are_the_reviewable_ones():
    assert set(c.QUARANTINE_REASONS) == {"negative_fare", "distance_over_100_miles"}


# ---------- enrich ----------

def test_enrich_adds_zone_names_flags_and_keeps_all_rows(spark):
    zones = spark.createDataFrame(
        [("1", "Manhattan", "Midtown"), ("2", "Queens", "Jamaica")],
        ["LocationID", "Borough", "Zone"],
    )
    clean = c.standardize(raw_df(spark, [
        make_row("2025-01-15 10:00:00", "2025-01-15 10:30:00", dist=0.0, pu=1, do=2, pax=None),
        make_row("2025-01-15 11:00:00", "2025-01-15 11:10:00", dist=2.5, pu=999, do=1, pax=2),
    ]))
    out = c.enrich(clean, zones)
    by_pickup = {r["pickup_location_id"]: r for r in out.collect()}

    assert out.count() == 2  # a left join must not drop the unmatched zone
    first = by_pickup[1]
    assert first["pickup_borough"] == "Manhattan"
    assert first["dropoff_zone"] == "Jamaica"
    assert first["passenger_count_missing"] is True
    assert first["zero_distance"] is True
    assert first["trip_duration_minutes"] == 30.0
    assert (first["year"], first["month"]) == (2025, 1)

    unmatched = by_pickup[999]
    assert unmatched["pickup_borough"] is None
    assert unmatched["passenger_count_missing"] is False
    assert unmatched["zero_distance"] is False
