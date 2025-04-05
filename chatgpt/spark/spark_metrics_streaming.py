# spark/spark_metrics_streaming.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

spark = SparkSession.builder \
    .appName("KafkaMetricsToDelta") \
    .getOrCreate()

# Define schema for metrics messages
metrics_schema = StructType([
    StructField("cpu", StringType(), True),
    StructField("memory", StringType(), True),
    StructField("disk_io", StringType(), True),
    StructField("timestamp", TimestampType(), True)
])

# Read from Kafka metrics topic
raw_metrics = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka:9092") \
  .option("subscribe", "metrics_topic") \
  .load()

metrics_df = raw_metrics.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), metrics_schema).alias("metrics")).select("metrics.*")

# Write the metrics into the Delta table
query = metrics_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/tmp/delta-metrics-checkpoint") \
    .outputMode("append") \
    .start("s3a://delta-tables/metrics")

query.awaitTermination()
