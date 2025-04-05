from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import os
import time
import json
import logging
import psutil
import socket
from confluent_kafka import Producer
from datetime import datetime
from delta import configure_spark_with_delta_pip

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Kafka connection details
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_METRICS_TOPIC = 'kafka_metrics'

# Minio connection details
MINIO_HOST = os.environ.get('MINIO_HOST', 'minio')
MINIO_PORT = os.environ.get('MINIO_PORT', '9000')
MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY', 'minio')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY', 'minio123')
MINIO_BUCKET = 'robotdata'

# Spark master
SPARK_MASTER = os.environ.get('SPARK_MASTER', 'spark://spark-master:7077')

# Define schema for system metrics
metrics_schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("hostname", StringType(), True),
    StructField("cpu_percent", DoubleType(), True),
    StructField("memory_percent", DoubleType(), True),
    StructField("disk_io_read", DoubleType(), True),
    StructField("disk_io_write", DoubleType(), True)
])

def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result"""
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

def collect_system_metrics():
    """Collect system metrics"""
    # Get CPU, memory and disk I/O metrics
    cpu_percent = psutil.cpu_percent(interval=1)
    memory_percent = psutil.virtual_memory().percent
    
    # Get disk I/O metrics
    disk_io_before = psutil.disk_io_counters()
    time.sleep(1)
    disk_io_after = psutil.disk_io_counters()
    
    disk_io_read = disk_io_after.read_bytes - disk_io_before.read_bytes
    disk_io_write = disk_io_after.write_bytes - disk_io_before.write_bytes
    
    # Create metrics dictionary
    metrics = {
        "timestamp": datetime.now().isoformat(),
        "hostname": socket.gethostname(),
        "cpu_percent": cpu_percent,
        "memory_percent": memory_percent,
        "disk_io_read": disk_io_read,
        "disk_io_write": disk_io_write
    }
    
    return metrics

def send_metrics_to_kafka():
    """Send system metrics to Kafka"""
    # Configure Kafka producer
    producer = Producer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'kafka-metrics-producer'
    })
    
    # Collect and send metrics periodically
    try:
        while True:
            metrics = collect_system_metrics()
            producer.produce(
                topic=KAFKA_METRICS_TOPIC,
                key=metrics["hostname"],
                value=json.dumps(metrics),
                callback=delivery_report
            )
            producer.flush()
            logger.info(f"Sent metrics to Kafka: {metrics}")
            time.sleep(10)  # Send metrics every 10 seconds
    except KeyboardInterrupt:
        logger.info("Metrics collection interrupted")
    except Exception as e:
        logger.error(f"Error sending metrics to Kafka: {e}")

def create_spark_session():
    """Create and configure a Spark session"""
    builder = SparkSession.builder \
        .appName("KafkaMetricsStreaming") \
        .master(SPARK_MASTER) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_HOST}:{MINIO_PORT}") \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.streaming.checkpointLocation", "s3a://robotdata/checkpoints")
    
    # Create Spark session with Delta
    return configure_spark_with_delta_pip(builder).getOrCreate()

def create_minio_bucket(spark):
    """Create Minio bucket if it doesn't exist"""
    from minio import Minio
    from minio.error import S3Error
    
    client = Minio(
        f"{MINIO_HOST}:{MINIO_PORT}",
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    
    # Create bucket if it doesn't exist
    try:
        if not client.bucket_exists(MINIO_BUCKET):
            client.make_bucket(MINIO_BUCKET)
            logger.info(f"Bucket '{MINIO_BUCKET}' created")
        else:
            logger.info(f"Bucket '{MINIO_BUCKET}' already exists")
    except S3Error as e:
        logger.error(f"Error creating bucket: {e}")

def init_metrics_delta_table(spark):
    """Initialize metrics Delta table in Minio"""
    metrics_df = spark.createDataFrame([], metrics_schema)
    metrics_df.write.format("delta") \
        .mode("overwrite") \
        .partitionBy("hostname") \
        .save(f"s3a://{MINIO_BUCKET}/delta/kafka_metrics")
    
    logger.info("Metrics Delta table initialized in Minio")

def process_metrics(df, epoch_id):
    """Process metrics and write to Delta table"""
    logger.info(f"Processing metrics for epoch {epoch_id}")
    
    if df.isEmpty():
        logger.info("No metrics to process")
        return
    
    # Write to Delta table
    df.write.format("delta") \
        .mode("append") \
        .partitionBy("hostname") \
        .save(f"s3a://{MINIO_BUCKET}/delta/kafka_metrics")
    
    logger.info(f"Processed {df.count()} metrics records")

def stream_metrics_to_delta(spark):
    """Stream metrics from Kafka to Delta"""
    # Read from Kafka
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_METRICS_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Parse JSON data
    metrics_df = kafka_df \
        .selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), metrics_schema).alias("data")) \
        .select("data.*")
    
    # Start streaming query
    query = metrics_df \
        .writeStream \
        .foreachBatch(process_metrics) \
        .outputMode("append") \
        .option("checkpointLocation", f"s3a://{MINIO_BUCKET}/checkpoints/metrics") \
        .start()
    
    # Wait for query to terminate
    query.awaitTermination()

def main():
    """Main function"""
    # Create Spark session
    spark = create_spark_session()
    logger.info("Spark session created")
    
    # Create Minio bucket
    create_minio_bucket(spark)
    
    # Initialize Delta table
    init_metrics_delta_table(spark)
    
    # Fork process: one for sending metrics, one for streaming
    if os.fork() == 0:
        # Child process: send metrics to Kafka
        send_metrics_to_kafka()
    else:
        # Parent process: stream metrics to Delta
        stream_metrics_to_delta(spark)

if __name__ == "__main__":
    main()