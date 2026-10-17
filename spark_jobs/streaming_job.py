from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.functions import when, to_timestamp, lit
from pyspark.sql.functions import struct, to_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    BooleanType
)

# Expected structure of a vehicle event
vehicle_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("speed_kmh", DoubleType(), True),
    StructField("fuel_level_pct", DoubleType(), True),
    StructField("harsh_braking", BooleanType(), True),
    StructField("_corrupt_record", StringType(), True)
])

# Create the Spark session
spark = (
    SparkSession.builder
    .appName("FleetMonitoring")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Read vehicle events from Kafka
events = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "vehicle_events")
    .option("startingOffsets", "earliest")
    .load()
)

# Convert Kafka messages to readable text
messages = events.selectExpr(
    "CAST(key AS STRING) AS key",
    "CAST(value AS STRING) AS value"
)

# Parse JSON using the explicit schema
parsed_events = messages.withColumn(
    "data",
    from_json(col("value"), vehicle_schema)
)

# Identify invalid events and explain why
validated_events = parsed_events.withColumn(
    "reason",
    when(col("data").isNull(), lit("invalid_json"))
    .when(col("data._corrupt_record").isNotNull(), lit("invalid_json"))
    .when(col("data.event_id").isNull(), lit("missing_event_id"))
    .when(col("data.vehicle_id").isNull(), lit("missing_vehicle_id"))
    .when(col("data.event_time").isNull(), lit("missing_event_time"))
    .when(col("data.latitude").isNull(), lit("missing_latitude"))
    .when(col("data.longitude").isNull(), lit("missing_longitude"))
    .when(col("data.speed_kmh").isNull(), lit("missing_speed_kmh"))
    .when(col("data.fuel_level_pct").isNull(), lit("missing_fuel_level_pct"))
    .when(col("data.harsh_braking").isNull(), lit("missing_harsh_braking"))
    .when(
        to_timestamp(col("data.event_time")).isNull(),
        lit("invalid_timestamp")
    )
    .when(
        (col("data.speed_kmh") < 0) | (col("data.speed_kmh") > 250),
        lit("invalid_speed")
    )
)

# Invalid events: keep the original message and its error reason
invalid_events = validated_events.filter(
    col("reason").isNotNull()
)

# Valid events: no validation error
valid_events = validated_events.filter(
    col("reason").isNull()
)

# Prepare invalid messages for the Kafka dead-letter queue
dlq_messages = invalid_events.select(
    col("key"),
    to_json(
        struct(
            col("reason"),
            col("value").alias("original_message")
        )
    ).alias("value")
)

# Send invalid events to the Kafka dead-letter queue
dlq_query = (
    dlq_messages.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "vehicle_events_dlq")
    .option("checkpointLocation", "/tmp/fleet_dlq_checkpoint")
    .outputMode("append")
    .start()
)

# Display received messages
query = (
    valid_events.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()