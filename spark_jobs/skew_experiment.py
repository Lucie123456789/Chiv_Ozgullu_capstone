import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg

spark = (
    SparkSession.builder
    .appName("SkewExperimentLargeBaseline")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.shuffle.partitions", "6")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

#df = spark.read.parquet("output/skew_test")
df = spark.read.parquet("output/skew_test_large")

vehicle_stats = (
    df.groupBy("vehicle_id")
    .agg(
        count("*").alias("number_of_events"),
        avg("speed_kmh").alias("mean_speed")
    )
)

start = time.perf_counter()

vehicle_stats.collect()

duration = time.perf_counter() - start

print("Baseline duration:", round(duration, 3), "seconds")

vehicle_stats.show(10, truncate=False)

input("Enter to stop Spark...")

spark.stop()