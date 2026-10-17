import os
from pyspark.sql.functions import (col, window, count, avg, max, sum, when)
from pyspark.sql.functions import to_json, struct, concat_ws

BASE_PATH = os.getenv("SPARK_BASE_PATH", ".")
def start_query2(enriched_events):
    regional_events = (enriched_events
        .groupBy(
            col("region"), #per region
            window(col("event_timestamp"), "1 minute") #per 1-min event time window
        )
        .agg(
            count("*").alias("number_of_events"), #number of events
            avg("data.speed_kmh").alias("mean_speed"), #mean speed
            max("data.speed_kmh").alias("max_speed"), #maximum speed
            sum(when(col("data.speed_kmh") > 120, 1).otherwise(0)).alias("number_of_speeding_events") #number of speeding events
        )
    )
    
    def publish_to_kafka(batch_df, batch_id):

        kafka_df = batch_df.select(
            concat_ws(
                "_",
                col("region"),
                col("window.start").cast("string")
            ).alias("key"),

            to_json(
                struct(
                    col("region"),
                    col("window.start").alias("window_start"),
                    col("window.end").alias("window_end"),
                    col("number_of_events"),
                    col("mean_speed"),
                    col("max_speed"),
                    col("number_of_speeding_events")
                )
            ).alias("value")
        )

        (
            kafka_df.write
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("topic", "region_kpis_1min")
            .save()
        )

    return (
        regional_events.writeStream
        .outputMode("update")
        .foreachBatch(publish_to_kafka)
        .option("checkpointLocation", f"{BASE_PATH}/checkpoints/query2")
        .start()
    )