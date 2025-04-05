# spark_monitor_app.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, year, month, dayofmonth
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType, IntegerType, LongType

# --- Configuration ---
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'localhost:9092')
KAFKA_METRICS_TOPIC = 'kafka_metrics'

MINIO_ENDPOINT = os.environ.get('MINIO_ENDPOINT', 'http://localhost:9000')
MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY', 'minioadmin')
DELTA_LAKE_BUCKET = os.environ.get('DELTA_LAKE_BUCKET', 'delta-lake')

METRICS_DELTA_PATH = f"s3a://{DELTA_LAKE_BUCKET}/kafka_metrics_delta"
CHECKPOINT_METRICS_PATH = f"s3a://{DELTA_LAKE_BUCKET}/_checkpoints/metrics_checkpoint"

print("--- Spark Monitor App Configuration ---")
print(f"Kafka Broker: {KAFKA_BROKER}")
print(f"Kafka Metrics Topic: {KAFKA_METRICS_TOPIC}")
print(f"MinIO Endpoint: {MINIO_ENDPOINT}")
print(f"Delta Lake Bucket: {DELTA_LAKE_BUCKET}")
print(f"Metrics Delta Path: {METRICS_DELTA_PATH}")
print("---------------------------------------")

# Define schema for the metrics JSON data
metrics_schema = StructType([
    StructField("broker_id", StringType(), True),
    StructField("timestamp", TimestampType(), True), # Assuming ISO format string
    StructField("cpu_percent", FloatType(), True),
    StructField("memory_percent", FloatType(), True),
    StructField("disk_read_mbps", FloatType(), True),
    StructField("disk_write_mbps", FloatType(), True),
    StructField("messages_in_per_sec", IntegerType(), True),
    StructField("bytes_in_per_sec", LongType(), True),
    StructField("bytes_out_per_sec", LongType(), True),
])

def create_spark_session():
    """Creates and configures the Spark Session."""
    spark = SparkSession.builder \
        .appName("KafkaMetricsPipeline") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print("Spark Session Created for Metrics Monitoring.")
    return spark

def process_metrics_stream(spark):
    """Reads metrics from Kafka and writes to a Delta table."""
    print(f"Setting up stream for Kafka topic: {KAFKA_METRICS_TOPIC} -> Delta path: {METRICS_DELTA_PATH}")

    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_METRICS_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    parsed_df = kafka_df \
        .select(from_json(col("value").cast("string"), metrics_schema).alias("data")) \
        .select("data.*")

    # Add partitioning columns (e.g., by date)
    partitioned_df = parsed_df \
        .withColumn("year", year(col("timestamp"))) \
        .withColumn("month", month(col("timestamp"))) \
        .withColumn("day", dayofmonth(col("timestamp")))

    # Write the stream to the Delta table for metrics
    query = partitioned_df \
        .writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("path", METRICS_DELTA_PATH) \
        .option("checkpointLocation", CHECKPOINT_METRICS_PATH) \
        .partitionBy("year", "month", "day") \
        .trigger(processingTime='60 seconds') \
        .start()

    print(f"Streaming query started for {KAFKA_METRICS_TOPIC}. Writing to {METRICS_DELTA_PATH}")
    return query

def main():
    spark = create_spark_session()
    metrics_query = process_metrics_stream(spark)
    print("Waiting for metrics streaming query to terminate...")
    metrics_query.awaitTermination()
    print("Metrics streaming application finished.")

if __name__ == "__main__":
    main()

