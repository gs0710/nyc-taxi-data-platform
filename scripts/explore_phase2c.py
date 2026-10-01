import duckdb

f = "data/raw/yellow/2025-01/yellow_tripdata_2025-01.parquet"


def run(title, sql):
    print(f"\n=== {title} ===")
    duckdb.sql(sql).show(max_rows=50, max_width=250)


run("Rows with NULL passenger_count, by VendorID and payment_type",
    f"""SELECT VendorID, payment_type, COUNT(*) AS trips
        FROM '{f}'
        WHERE passenger_count IS NULL
        GROUP BY VendorID, payment_type
        ORDER BY trips DESC""")
