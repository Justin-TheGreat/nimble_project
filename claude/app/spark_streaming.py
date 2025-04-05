from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, expr
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import os
import logging
from delta import configure_spark_with_delta_pip

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Kafka connection details
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPICS = ['robot_events', 'teleop_events']

# Minio connection details
MINIO_HOST = os.environ.get('MINIO_HOST', 'minio')
MINIO_PORT = os.environ.get('MINIO_PORT', '9000')
MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY', 'minio')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY', 'minio123')
MINIO_BUCKET = 'robotdata'

# Spark master
SPARK_MASTER = os.environ.get('SPARK_MASTER', 'spark://spark-master:7077')

# Define schema for robot events
robot_schema = StructType([
    StructField("operation", StringType(), True),
    StructField("table", StringType(), True),
    StructField("uuid", StringType(), True),
    StructField("robot_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("event_id", StringType(), True)
])

# Define schema for teleop events
teleop_schema = StructType([
    StructField("operation", StringType(), True),
    StructField("table", StringType(), True),
    StructField("uuid", StringType(), True),
    StructField("robot_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("event_id", StringType(), True),
    StructField("teleoperator_id", StringType(), True)
])

def create_spark_session():
    """Create and configure a Spark session"""
    builder = SparkSession.builder \
        .appName("RobotDataStreaming") \
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

def init_delta_tables(spark):
    """Initialize Delta tables in Minio"""
    # Create robot table
    robot_df = spark.createDataFrame([], robot_schema)
    robot_df.write.format("delta") \
        .mode("overwrite") \
        .partitionBy("robot_id") \
        .save(f"s3a://{MINIO_BUCKET}/delta/robot")
    
    # Create teleop table
    teleop_df = spark.createDataFrame([], teleop_schema)
    teleop_df.write.format("delta") \
        .mode("overwrite") \
        .partitionBy("robot_id") \
        .save(f"s3a://{MINIO_BUCKET}/delta/teleop")
    
    logger.info("Delta tables initialized in Minio")

def process_robot_events(df, epoch_id):
    """Process robot events and write to Delta table"""
    logger.info(f"Processing robot events for epoch {epoch_id}")
    
    if df.isEmpty():
        logger.info("No robot events to process")
        return
    
    # Write to Delta table
    df.write.format("delta") \
        .mode("append") \
        .partitionBy("robot_id") \
        .save(f"s3a://{MINIO_BUCKET}/delta/robot")
    
    logger.info(f"Processed {df.count()} robot events")

def process_teleop_events(df, epoch_id):
    """Process teleop events and write to Delta table"""
    logger.info(f"Processing teleop events for epoch {epoch_id}")
    
    if df.isEmpty():
        logger.info("No teleop events to process")
        return
    
    # Write to Delta table
    df.write.format("delta") \
        .mode("append") \
        .partitionBy("robot_id") \
        .save(f"s3a://{MINIO_BUCKET}/delta/teleop")
    
    logger.info(f"Processed {df.count()} teleop events")

def main():
    """Main function"""
    # Create Spark session
    spark = create_spark_session()
    logger.info("Spark session created")
    
    # Create Minio bucket
    create_minio_bucket(spark)
    
    # Initialize Delta tables
    init_delta_tables(spark)
    
    # Read from Kafka
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", ",".join(KAFKA_TOPICS)) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Extract topic and value
    kafka_df = kafka_df.selectExpr("topic", "CAST(value AS STRING) as value")
    
    # Process robot events
    robot_df = kafka_df \
        .filter(col("topic") == "robot_events") \
        .select(from_json(col("value"), robot_schema).alias("data")) \
        .select("data.*")
    
    # Process teleop events
    teleop_df = kafka_df \
        .filter(col("topic") == "teleop_events") \
        .select(from_json(col("value"), teleop_schema).alias("data")) \
        .select("data.*")
    
    # Start streaming queries
    robot_query = robot_df \
        .writeStream \
        .foreachBatch(process_robot_events) \
        .outputMode("append") \
        .option("checkpointLocation", f"s3a://{MINIO_BUCKET}/checkpoints/robot") \
        .start()
    
    teleop_query = teleop_df \
        .writeStream \
        .foreachBatch(process_teleop_events) \
        .outputMode("append") \
        .option("checkpointLocation", f"s3a://{MINIO_BUCKET}/checkpoints/teleop") \
        .start()
    
    # Wait for queries to terminate
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()