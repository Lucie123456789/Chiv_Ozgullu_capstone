from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("PrepareLargeSkew")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("output/skew_test")

df = df.filter(col("vehicle_id").isNotNull())

large_df = df

for _ in range(29):
    large_df = large_df.union(df)

large_df.write.mode("overwrite").parquet(
    "output/skew_test_large"
)

print("Large skew dataset rows:", large_df.count())

spark.stop()