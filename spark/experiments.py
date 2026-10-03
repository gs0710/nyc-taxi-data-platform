"""Spark experiments: partitions, broadcast join, partitioned Parquet."""
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder.appName("experiments")
    .master("local[4]")
    .config("spark.driver.memory", "3g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

trips = spark.read.parquet("/app/data/raw/yellow/2025-01/yellow_tripdata_2025-01.parquet")
zones = spark.read.csv("/app/data/raw/zones/taxi_zone_lookup.csv", header=True, inferSchema=True)


def section(title):
    print(f"\n{'=' * 8} {title} {'=' * 8}")


# 1. repartition vs coalesce
section("1. repartition vs coalesce")
print("trips partitions:", trips.rdd.getNumPartitions())
print("repartition(12):", trips.repartition(12).rdd.getNumPartitions())
print("coalesce(2):", trips.coalesce(2).rdd.getNumPartitions())
print("\nPlan for repartition(12):")
trips.repartition(12).explain()
print("Plan for coalesce(2):")
trips.coalesce(2).explain()

# 2. Broadcast join vs sort-merge join
section("2. Join trips with zones")
cond = trips.PULocationID == zones.LocationID


def borough_counts(zones_df):
    return (
        trips.join(zones_df, cond)
        .groupBy("Borough")
        .agg(F.count("*").alias("trips"))
        .orderBy(F.desc("trips"))
    )


spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  # disable auto-broadcast
print("A) Auto-broadcast OFF -> Spark must shuffle both sides:")
a = borough_counts(zones)
a.explain()
t = time.time()
a.collect()
print(f"   took {time.time() - t:.2f}s")

print("\nB) Explicit F.broadcast(zones) -> small table copied to every worker:")
b = borough_counts(F.broadcast(zones))
b.explain()
t = time.time()
rows = b.collect()
print(f"   took {time.time() - t:.2f}s")
for r in rows:
    print("  ", r["Borough"], r["trips"])

# 3. Partitioned Parquet write and partition pruning
section("3. Partitioned Parquet")
out = "/app/data/processed/experiment_trips"
(
    trips.withColumn("year", F.year("tpep_pickup_datetime"))
    .withColumn("month", F.month("tpep_pickup_datetime"))
    .write.mode("overwrite")
    .partitionBy("year", "month")
    .parquet(out)
)
print("Written to", out)

back = spark.read.parquet(out)
print("\nRows per partition folder:")
back.groupBy("year", "month").count().orderBy("year", "month").show()

print("Plan when filtering one partition (look for PartitionFilters):")
back.filter((F.col("year") == 2025) & (F.col("month") == 1)).explain()

spark.stop()
