import duckdb

f = "data/raw/yellow/2025-01/yellow_tripdata_2025-01.parquet"


def run(title, sql):
    print(f"\n=== {title} ===")
    duckdb.sql(sql).show(max_rows=50, max_width=250)


run("Row count",
    f"SELECT COUNT(*) AS total_rows FROM '{f}'")

run("Schema (columns and types)",
    f"DESCRIBE SELECT * FROM '{f}'")

run("First 5 rows",
    f"SELECT * FROM '{f}' LIMIT 5")

run("Problem 1: negative fares",
    f"SELECT COUNT(*) AS negative_fares FROM '{f}' WHERE fare_amount < 0")

run("Problem 2: dropoff before pickup",
    f"""SELECT COUNT(*) AS bad_times FROM '{f}'
        WHERE tpep_dropoff_datetime < tpep_pickup_datetime""")

run("Problem 3: pickup date range",
    f"""SELECT MIN(tpep_pickup_datetime) AS earliest,
               MAX(tpep_pickup_datetime) AS latest
        FROM '{f}'""")

run("Problem 4: missing passenger counts",
    f"SELECT COUNT(*) AS null_passengers FROM '{f}' WHERE passenger_count IS NULL")

run("Problem 5: suspicious distances",
    f"""SELECT
          SUM(CASE WHEN trip_distance = 0 THEN 1 ELSE 0 END) AS zero_distance,
          SUM(CASE WHEN trip_distance > 100 THEN 1 ELSE 0 END) AS over_100_miles,
          MAX(trip_distance) AS max_distance
        FROM '{f}'""")
