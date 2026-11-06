import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum,
    xxhash64, pmod, lit
)

spark = (
    SparkSession.builder
    .appName("SkewExperimentSaltingLarge")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.shuffle.partitions", "6")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# df = (
#     spark.read.parquet("output/skew_test")
#     .filter(col("vehicle_id").isNotNull())
#     .filter(col("event_id").isNotNull())
# )

df = (
    spark.read.parquet("output/skew_test_large")
    .filter(col("vehicle_id").isNotNull())
    .filter(col("event_id").isNotNull())
)

salted_df = df.withColumn(
    "salt",
    pmod(xxhash64(col("event_id")), lit(4))
)

# partial_stats = (
#     salted_df
#     .groupBy("vehicle_id", "salt")
#     .agg(
#         count("*").alias("partial_count"),
#         spark_sum("speed_kmh").alias("partial_speed_sum")
#     )
# )

salted_df = salted_df.repartition(6, "vehicle_id", "salt")

partial_stats = (
    salted_df
    .groupBy("vehicle_id", "salt")
    .agg(
        count("*").alias("partial_count"),
        spark_sum("speed_kmh").alias("partial_speed_sum")
    )
)

vehicle_stats = (
    partial_stats
    .groupBy("vehicle_id")
    .agg(
        spark_sum("partial_count").alias("number_of_events"),
        spark_sum("partial_speed_sum").alias("total_speed")
    )
    .withColumn(
        "mean_speed",
        col("total_speed") / col("number_of_events")
    )
    .select("vehicle_id", "number_of_events", "mean_speed")
)

start = time.perf_counter()

results = vehicle_stats.collect()

duration = time.perf_counter() - start

print("Salting duration:", round(duration, 3), "seconds")

print("Top vehicles by event count:")
vehicle_stats.orderBy(
    col("number_of_events").desc()
).show(10, truncate=False)

input("Enter to stop Spark...")

spark.stop()