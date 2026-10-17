
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, lit
import os

spark = SparkSession.builder.appName("Query1ReplayTest").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Read the backup of batch 0
source = "/app/output/query1/batch_id=0_backup"
target = "/app/output/query1/batch_id=0"

batch = spark.read.parquet(source)

# Replay the same batch with the same batch_id
batch = (
    batch
    .withColumn("event_date", to_date(col("event_timestamp")))
    .withColumn("batch_id", lit(0))
)

# Count Parquet files before replay
def count_files(path):
    return sum(
        len([f for f in files if f.endswith(".parquet")])
        for _, _, files in os.walk(path)
    )

before = count_files(target)
print(f"BEFORE REPLAY: {before} Parquet files")

# Same overwrite logic as Query 1
(
    batch.write
    .mode("overwrite")
    .partitionBy("event_date")
    .parquet(target)
)

after = count_files(target)
print(f"AFTER REPLAY: {after} Parquet files")

print(f"ROWS AFTER REPLAY: {spark.read.parquet(target).count()}")

spark.stop()