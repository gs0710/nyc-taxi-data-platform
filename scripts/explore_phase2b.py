import duckdb

f = "data/raw/yellow/2025-01/yellow_tripdata_2025-01.parquet"


def run(title, sql):
    print(f"\n=== {title} ===")
    duckdb.sql(sql).show(max_rows=50, max_width=250)


run("Pickups outside January 2025",
    f"""SELECT COUNT(*) AS outside_january FROM '{f}'
        WHERE tpep_pickup_datetime < TIMESTAMP '2025-01-01'
           OR tpep_pickup_datetime >= TIMESTAMP '2025-02-01'""")

run("Negative fares by payment_type",
    f"""SELECT payment_type, COUNT(*) AS trips FROM '{f}'
        WHERE fare_amount < 0
        GROUP BY payment_type ORDER BY trips DESC""")

run("When passenger_count is NULL, what else is NULL?",
    f"""SELECT
          COUNT(*) AS null_passenger_rows,
          SUM(CASE WHEN RatecodeID IS NULL THEN 1 ELSE 0 END) AS also_null_ratecode,
          SUM(CASE WHEN store_and_fwd_flag IS NULL THEN 1 ELSE 0 END) AS also_null_flag,
          SUM(CASE WHEN congestion_surcharge IS NULL THEN 1 ELSE 0 END) AS also_null_congestion,
          SUM(CASE WHEN Airport_fee IS NULL THEN 1 ELSE 0 END) AS also_null_airport
        FROM '{f}'
        WHERE passenger_count IS NULL""")
