import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, max, sum, when
from pyspark.sql.functions import col, count, avg, max, sum, when, desc
import json
import os

date = sys.argv[1]

spark = (
    SparkSession.builder
    .appName("FleetDailyReport")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

base_path = Path("output/query1")

batch_paths = [
    str(folder)
    for folder in base_path.iterdir()
    if folder.is_dir()
    and folder.name.startswith("batch_id=")
    and folder.name.split("=")[1].isdigit()
]

df = (
    spark.read
    .option("basePath", str(base_path))
    .parquet(*batch_paths)
)

daily_data = df.filter(col("event_date") == date)

print("Partition pruning execution plan:")
daily_data.explain(True)

daily_data.select(
    "event_id",
    "vehicle_id",
    "region",
    "event_date"
).show(5, truncate=False)

vehicle_stats = (
    daily_data
    .groupBy("vehicle_id")
    .agg(
        count("*").alias("number_of_events"),
        avg("data.speed_kmh").alias("mean_speed"),
        max("data.speed_kmh").alias("max_speed"),
        sum(
            when(col("data.speed_kmh") > 120, 1).otherwise(0)
        ).alias("speeding_events"),
        sum(
            when(col("data.harsh_braking") == True, 1).otherwise(0)
        ).alias("harsh_braking_events")
    )
)

vehicle_stats.show(10, truncate=False)

region_stats = (
    daily_data
    .groupBy("region")
    .agg(
        count("*").alias("number_of_events"),
        avg("data.speed_kmh").alias("mean_speed"),
        max("data.speed_kmh").alias("max_speed"),
        sum(
            when(col("data.speed_kmh") > 120, 1).otherwise(0)
        ).alias("speeding_events"),
        sum(
            when(col("data.harsh_braking") == True, 1).otherwise(0)
        ).alias("harsh_braking_events")
    )
)

region_stats.show(truncate=False)
vehicle_ranking = vehicle_stats.withColumn(
    "total_infractions",
    col("speeding_events") + col("harsh_braking_events")
)

worst_vehicles = (
    vehicle_ranking
    .orderBy(
        desc("total_infractions"),
        desc("max_speed"),
        col("vehicle_id")
    )
    .limit(5)
)

print("Top 5 worst vehicles:")
worst_vehicles.show(truncate=False)

report = {
    "date": date,
    "vehicle_stats": [
        row.asDict() for row in vehicle_stats.collect()
    ],
    "region_stats": [
        row.asDict() for row in region_stats.collect()
    ],
    "worst_vehicles": [
        row.asDict() for row in worst_vehicles.collect()
    ]
}

output_dir = "output/daily_reports"
os.makedirs(output_dir, exist_ok=True)

output_file = os.path.join(output_dir, f"{date}.json")

with open(output_file, "w") as f:
    json.dump(report, f, indent=4)

print("Daily report saved to:", output_file)

spark.stop()