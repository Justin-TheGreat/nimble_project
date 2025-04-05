# spark_stream_app.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, year, month, dayofmonth
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# --- Configuration ---
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'localhost:9092')
KAFKA_ROBOT_TOPIC = 'robot_cdc'
KAFKA_TELEOP_TOPIC = 'teleop_cdc'

MINIO_ENDPOINT = os.environ.get('MINIO_ENDPOINT', 'http://localhost:9000')
MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY', 'minioadmin')
DELTA_LAKE_BUCKET = os.environ.get('DELTA_LAKE_BUCKET', 'delta-lake')

# Define S3A paths for Delta tables
# Partitioning by date is common
ROBOT_DELTA_PATH = f"s3a://{DELTA_LAKE_BUCKET}/robot_delta"
TELEOP_DELTA_PATH = f"s3a://{DELTA_LAKE_BUCKET}/teleop_delta"
CHECKPOINT_ROBOT_PATH = f"s3a://{DELTA_LAKE_BUCKET}/_checkpoints/robot_checkpoint"
CHECKPOINT_TELEOP_PATH = f"s3a://{DELTA_LAKE_BUCKET}/_checkpoints/teleop_checkpoint"

print("--- Spark Streaming App Configuration ---")
print(f"Kafka Broker: {KAFKA_BROKER}")
print(f"Kafka Robot Topic: {KAFKA_ROBOT_TOPIC}")
print(f"Kafka Teleop Topic: {KAFKA_TELEOP_TOPIC}")
print(f"MinIO Endpoint: {MINIO_ENDPOINT}")
print(f"Delta Lake Bucket: {DELTA_LAKE_BUCKET}")
print(f"Robot Delta Path: {ROBOT_DELTA_PATH}")
print(f"Teleop Delta Path: {TELEOP_DELTA_PATH}")
print("-----------------------------------------")


# Define schemas corresponding to the JSON data structure from Kafka
# Ensure data types match PostgreSQL DDL and JSON structure from trigger
robot_schema = StructType([
    StructField("UUID", StringType(), True),
    StructField("Robot_ID", StringType(), True),
    StructField("Timestamp", TimestampType(), True), # Assuming trigger sends ISO format parsable by Spark
    StructField("Event_ID", StringType(), True),
])

teleop_schema = StructType([
    StructField("UUID", StringType(), True),
    StructField("Robot_ID", StringType(), True),
    StructField("Timestamp", TimestampType(), True),
    StructField("Event_ID", StringType(), True),
    StructField("Teleoperator_ID", StringType(), True),
])

def create_spark_session():
    """Creates and configures the Spark Session."""
    # Configuration is mostly handled by spark-submit command in docker-compose.
    # We can add more specific configurations here if needed.
    spark = SparkSession.builder \
        .appName("RobotDataPipeline") \
        .getOrCreate() # Gets the session configured by spark-submit

    # Set log level (optional)
    spark.sparkContext.setLogLevel("WARN") # Reduce verbosity

    print("Spark Session Created.")
    print(f"Spark Version: {spark.version}")
    # Print S3A config for verification (remove sensitive keys in production logs)
    # print(f"S3A Endpoint: {spark.sparkContext.getConf().get('spark.hadoop.fs.s3a.endpoint')}")
    # print(f"S3A Path Style Access: {spark.sparkContext.getConf().get('spark.hadoop.fs.s3a.path.style.access')}")

    return spark

def process_stream(spark, kafka_topic, schema, delta_path, checkpoint_path):
    """Reads from Kafka, processes, and writes to a Delta table."""
    print(f"Setting up stream for Kafka topic: {kafka_topic} -> Delta path: {delta_path}")

    # Read from Kafka source
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # Parse the JSON data from the 'value' column
    parsed_df = kafka_df \
        .select(from_json(col("value").cast("string"), schema).alias("data")) \
        .select("data.*") # Flatten the struct

    # Add partitioning columns (e.g., year, month, day based on Timestamp)
    partitioned_df = parsed_df \
        .withColumn("year", year(col("Timestamp"))) \
        .withColumn("month", month(col("Timestamp"))) \
        .withColumn("day", dayofmonth(col("Timestamp")))

    # Write the stream to the Delta table
    # Using append mode and specifying checkpoint location
    # Partitioning by year, month, day
    query = partitioned_df \
        .writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("path", delta_path) \
        .option("checkpointLocation", checkpoint_path) \
        .partitionBy("year", "month", "day") \
        .trigger(processingTime='30 seconds') \
        .start()

    print(f"Streaming query started for {kafka_topic}. Writing to {delta_path}")
    return query # Return the query handle

def main():
    spark = create_spark_session()

    # Start processing the robot stream
    robot_query = process_stream(spark, KAFKA_ROBOT_TOPIC, robot_schema, ROBOT_DELTA_PATH, CHECKPOINT_ROBOT_PATH)

    # Start processing the teleop stream
    teleop_query = process_stream(spark, KAFKA_TELEOP_TOPIC, teleop_schema, TELEOP_DELTA_PATH, CHECKPOINT_TELEOP_PATH)

    print("Waiting for streaming queries to terminate...")
    # Wait for either query to terminate (e.g., due to error or manual stop)
    # In a real deployment, you might want more robust monitoring and restart logic.
    spark.streams.awaitAnyTermination()

    print("Streaming application finished.")


if __name__ == "__main__":
    main()

