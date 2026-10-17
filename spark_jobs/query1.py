import os
from pyspark.sql.functions import col, to_date, lit, struct, to_json

BASE_PATH = os.getenv("SPARK_BASE_PATH", ".")

def start_query1(enriched_events):
    def process_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return

        # Add event date and batch ID
        batch = (
            batch_df
            .withColumn("event_date", to_date(col("event_timestamp")))
            .withColumn("batch_id", lit(batch_id))
        )

        # Save each batch in its own folder.
        # Overwriting the same batch makes the Parquet write idempotent.
        (
            batch.write
            .mode("overwrite")
            .partitionBy("event_date")
            .parquet(f"{BASE_PATH}/output/query1/batch_id={batch_id}")
        )

        # Select speeding or harsh braking events
        alerts = batch.filter(
            (col("data.speed_kmh") > 120)
            | (col("data.harsh_braking") == True)
        )

        # Publish alerts to Kafka
        kafka_alerts = alerts.select(
            col("data.vehicle_id").cast("string").alias("key"),
            to_json(
                struct(
                    col("data.event_id").alias("event_id"),
                    col("data.vehicle_id").alias("vehicle_id"),
                    col("data.speed_kmh").alias("speed_kmh"),
                    col("data.harsh_braking").alias("harsh_braking"),
                    col("region"),
                )
            ).alias("value")
        )

        (
            kafka_alerts.write
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("topic", "alerts")
            .save()
        )

    return (
        enriched_events.writeStream
        .foreachBatch(process_batch)
        .outputMode("append")
        .option("checkpointLocation", f"{BASE_PATH}/checkpoints/query1")
        .start()
    )
