import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pathlib import Path
import json
import os

date = sys.argv[1]

spark = (
    SparkSession.builder
    .appName("FleetQualityJob")
    .getOrCreate()
)

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

total_rows = daily_data.count()

print("Date:", date)
print("Total rows:", total_rows)

unique_events = daily_data.select("event_id").distinct().count()

duplicate_count = total_rows - unique_events

unknown_count = daily_data.filter(
    col("region") == "UNKNOWN"
).count()

unknown_pct = (
    (unknown_count / total_rows) * 100
    if total_rows > 0 else 0
)

print("UNKNOWN regions:", unknown_count)
print("UNKNOWN percentage:", round(unknown_pct, 2), "%")

reasons = []

if total_rows == 0:
    reasons.append("No events found for this date")

if duplicate_count > 0:
    reasons.append("Duplicate event IDs detected")

if unknown_pct > 5:
    reasons.append("UNKNOWN regions exceed 5%")

verdict = {
    "date": date,
    "total_rows": total_rows,
    "duplicate_event_ids": duplicate_count,
    "unknown_region_pct": round(unknown_pct, 2),
    "passed": len(reasons) == 0,
    "reasons": reasons
}

print("Quality verdict:", verdict)

output_dir = "output/quality"
os.makedirs(output_dir, exist_ok=True)

output_file = os.path.join(output_dir, f"{date}.json")

with open(output_file, "w") as f:
    json.dump(verdict, f, indent=4)

print("JSON saved to:", output_file)

spark.stop()