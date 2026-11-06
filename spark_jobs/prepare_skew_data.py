from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType

spark = (
    SparkSession.builder
    .appName("PrepareSkewData")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("event_id", StringType()),
    StructField("vehicle_id", StringType()),
    StructField("event_time", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("speed_kmh", DoubleType()),
    StructField("fuel_level_pct", DoubleType()),
    StructField("harsh_braking", BooleanType())
])

df = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "vehicle_events")
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
)

events = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

events = events.filter(
    col("event_id").contains("e446e972")
)

events.write.mode("overwrite").parquet("output/skew_test")

print("Skew test rows:", events.count())

vehicle_counts = (
    events
    .groupBy("vehicle_id")
    .count()
    .orderBy(col("count").desc())
)

print("Events per vehicle:")
vehicle_counts.show(10, truncate=False)

total_events = events.count()

hot_events = events.filter(
    col("vehicle_id") == "V0001"
).count()

hot_percentage = (
    (hot_events / total_events) * 100
    if total_events > 0 else 0
)

print("Hot vehicle events:", hot_events)
print("Hot vehicle percentage:", round(hot_percentage, 2), "%")

spark.stop()