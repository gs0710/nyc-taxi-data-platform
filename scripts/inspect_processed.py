"""Quick look at the processed layer using DuckDB (host Python, no Spark needed)."""
import duckdb

base = "data/processed"

print("\n== Clean rows per partition ==")
duckdb.sql(f"""
    SELECT year, month, COUNT(*) AS rows
    FROM read_parquet('{base}/yellow_trips/*/*/*.parquet', hive_partitioning=true)
    GROUP BY ALL ORDER BY ALL
""").show()

print("\n== Rejected / quarantined rows ==")
duckdb.sql(f"""
    SELECT source_month, disposition, reject_reason, COUNT(*) AS rows
    FROM read_parquet('{base}/rejected_trips/*/*.parquet', hive_partitioning=true)
    GROUP BY ALL ORDER BY source_month, rows DESC
""").show(max_rows=30)

print("\n== Flags in clean data (Jan 2025) ==")
duckdb.sql(f"""
    SELECT
      SUM(CASE WHEN passenger_count_missing THEN 1 ELSE 0 END) AS passenger_missing,
      SUM(CASE WHEN zero_distance THEN 1 ELSE 0 END) AS zero_distance,
      SUM(CASE WHEN pickup_borough IS NULL THEN 1 ELSE 0 END) AS no_zone_match,
      ROUND(AVG(trip_duration_minutes), 1) AS avg_minutes
    FROM read_parquet('{base}/yellow_trips/*/*/*.parquet', hive_partitioning=true)
    WHERE year = 2025 AND month = 1
""").show()
