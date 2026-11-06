import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, col

spark = (
    SparkSession.builder
    #.appName("SkewExperimentBaseline")
    .appName("SkewExperimentAQE")
    #.config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.shuffle.partitions", "6")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# df = spark.read.parquet("output/skew_test")

df = spark.read.parquet("output/skew_test_large")
df = df.filter(col("vehicle_id").isNotNull())

# vehicle_stats = (
#     df.groupBy("vehicle_id")
#     .agg(
#         count("*").alias("number_of_events"),
#         avg("speed_kmh").alias("mean_speed")
#     )
# )

df = df.repartition(6, "vehicle_id")

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