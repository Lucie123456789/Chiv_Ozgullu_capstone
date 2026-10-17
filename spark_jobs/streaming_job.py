from pyspark.sql import SparkSession
from query1 import start_query1
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
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

# Fleet catalog schema (B.3)
catalog_schema = StructType([
    StructField("vehicle_id", StringType(), True),
    StructField("plate", StringType(), True),
    StructField("vehicle_type", StringType(), True),
    StructField("home_region", StringType(), True),
    StructField("max_payload_kg", DoubleType(), True)
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
    .when(
        col("data.event_id").isNull() | (col("data.event_id") == ""),
        lit("missing_event_id")
    )
    .when(
        col("data.vehicle_id").isNull() | (col("data.vehicle_id") == ""),
        lit("missing_vehicle_id")
    )
    .when(
        col("data.event_time").isNull() | (col("data.event_time") == ""),
        lit("missing_event_time")
    )
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

# Convert event_time to timestamp for deduplication (B.2)
valid_events = valid_events.withColumn(
    "event_timestamp",
    to_timestamp(col("data.event_time"))
)

# Remove duplicate event IDs with a 2-minute watermark (B.2)
deduplicated_events = (
    valid_events
    .withColumn("event_id", col("data.event_id"))
    .withWatermark("event_timestamp", "2 minutes")
    .dropDuplicatesWithinWatermark(["event_id"])
)

# Read the fleet catalog from Kafka (B.3)
catalog_kafka = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "fleet_catalog")
    .option("startingOffsets", "earliest")
    .load()
)

# Parse catalog JSON messages (B.3)
catalog = (
    catalog_kafka
    .selectExpr(
        "CAST(value AS STRING) AS value",
        "timestamp",
        "offset"
    )
    .withColumn("data", from_json(col("value"), catalog_schema))
    .select("data.*", "timestamp", "offset")
    .filter(col("vehicle_id").isNotNull())
    .withColumn(
        "rn",
        row_number().over(
            Window.partitionBy("vehicle_id").orderBy(
                col("timestamp").desc(),
                col("offset").desc()
            )
        )
    )
    .filter(col("rn") == 1)
    .drop("rn", "timestamp", "offset")
)

# Enrich events with the fleet catalog (B.3)
from pyspark.sql.functions import broadcast, coalesce

enriched_events = (
    deduplicated_events
    .join(
        broadcast(catalog),
        col("data.vehicle_id") == catalog["vehicle_id"],
        "left"
    )
    .withColumn(
        "region",
        coalesce(col("home_region"), lit("UNKNOWN"))
    )
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
    .option("checkpointLocation", "/app/checkpoints/fleet_dlq")
    .outputMode("append")
    .start()
)

# Display received messages
query = (
    # valid_events.writeStream
    # deduplicated_events.select("event_id", "event_timestamp").writeStream
    enriched_events.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .start()
)

query1 = start_query1(enriched_events)
query.awaitTermination()