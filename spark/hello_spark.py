"""First Spark job: see lazy evaluation, actions, partitions and shuffle."""
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder.appName("hello-spark")
    .master("local[4]")                          # run locally, use 4 cores
    .config("spark.driver.memory", "3g")
    .config("spark.sql.shuffle.partitions", "8")  # default is 200: too many for local
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

path = "/app/data/raw/yellow/2025-01/yellow_tripdata_2025-01.parquet"

t = time.time()
df = spark.read.parquet(path)
print(f"\nread.parquet() returned in {time.time() - t:.2f}s (only the schema was read)")

t = time.time()
busiest = (
    df.filter(F.col("fare_amount") > 0)
    .groupBy("PULocationID")
    .agg(F.count("*").alias("trips"))
    .orderBy(F.desc("trips"))
)
print(f"transformations defined in {time.time() - t:.2f}s (nothing computed yet: lazy)")

print("\nExecution plan (look for 'Exchange' = shuffle):")
busiest.explain()

t = time.time()
print("\nTop 5 pickup zones by trips (this is the ACTION):")
busiest.show(5)
print(f"show() took {time.time() - t:.2f}s (this is where the work happened)")

print("\nInput partitions:", df.rdd.getNumPartitions())
print("Total rows:", df.count())
spark.stop()
