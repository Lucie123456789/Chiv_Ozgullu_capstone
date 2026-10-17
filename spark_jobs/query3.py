from pyspark.sql.functions import col, window, count, avg
from pyspark.sql.functions import lit
from pyspark.sql.functions import to_json, struct

import os

BASE_PATH = os.getenv("SPARK_BASE_PATH", ".")

def start_query3(enriched_events):

    vehicle_events = (enriched_events
        .groupBy(
            col("data.vehicle_id").alias("vehicle_id"), #per vehicle
            window(col("event_timestamp"), "5 minutes", "1 minute") #per 5 min windows sliding every minute
        )
        
        .agg(
            count("*").alias("number_of_events"), #number of events
            avg("data.speed_kmh").alias("mean_speed") #mean speed
        )
    )
    
    #only consider vehicles with at least 5 events in the 5-minute window and mean speed exceeding 100 km/h
    sustained_speeding = (vehicle_events
        .filter(
            (col("mean_speed") > 100) & #mean speed exceeds 100 km/h over
            (col("number_of_events") >= 5) #at least 5 events raises
        )
    )
    
    #alert generation for sustained speeding
    alerts = (sustained_speeding
        .select(
            col("vehicle_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("number_of_events"),
            col("mean_speed"),
            lit("sustained_speeding").alias("alert_type")
        )
    )
    
    def publish_to_kafka(batch_df, batch_id):

        if batch_df.isEmpty():
            return
        
        kafka_alerts = batch_df.select(
            col("vehicle_id").cast("string").alias("key"),

            to_json(
                struct(
                    col("vehicle_id"),
                    col("window_start"),
                    col("window_end"),
                    col("number_of_events"),
                    col("mean_speed"),
                    col("alert_type")
                )
            ).alias("value")
        )

        (kafka_alerts.write
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("topic", "alerts")
            .save()
        )
        
    return (
        alerts.writeStream
        .outputMode("update")
        .foreachBatch(publish_to_kafka)
        .option("checkpointLocation", f"{BASE_PATH}/checkpoints/query3")
        .start()
    )