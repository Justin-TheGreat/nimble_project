from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hash, count
import psycopg2
import os
import time
import logging
from delta import configure_spark_with_delta_pip

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# PostgreSQL connection details
PG_HOST = os.environ.get('POSTGRES_HOST', 'postgres')
PG_PORT = os.environ.get('POSTGRES_PORT', '5432')
PG_DB = os.environ.get('POSTGRES_DB', 'robotdata')
PG_USER = os.environ.get('POSTGRES_USER', 'postgres')
PG_PASSWORD = os.environ.get('POSTGRES_PASSWORD', 'postgres')

# Minio connection details
MINIO_HOST = os.environ.get('MINIO_HOST', 'minio')
MINIO_PORT = os.environ.get('MINIO_PORT', '9000')
MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY', 'minio')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY', 'minio123')
MINIO_BUCKET = 'robotdata'

# Spark master
SPARK_MASTER = os.environ.get('SPARK_MASTER', 'spark://spark-master:7077')

# Tables to check
TABLES = ['robot', 'teleop']

def create_spark_session():
    """Create and configure a Spark session"""
    builder = SparkSession.builder \
        .appName("DataQualityChecks") \
        .master(SPARK_MASTER) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_HOST}:{MINIO_PORT}") \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    
    # Create Spark session with Delta
    return configure_spark_with_delta_pip(builder).getOrCreate()

def get_pg_connection():
    """Create and return a PostgreSQL connection"""
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD
        )
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        raise

def get_postgres_row_count(conn, table):
    """Get row count from PostgreSQL table"""
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        return count
    except Exception as e:
        logger.error(f"Failed to get row count from PostgreSQL table {table}: {e}")
        return -1
    finally:
        cursor.close()

def get_postgres_checksum(conn, table):
    """Get checksum from PostgreSQL table"""
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT MD5(string_agg(CAST((t.*)::text as varchar), '')) FROM (SELECT * FROM {table} ORDER BY UUID LIMIT 100) t")
        checksum = cursor.fetchone()[0]
        return checksum
    except Exception as e:
        logger.error(f"Failed to get checksum from PostgreSQL table {table}: {e}")
        return None
    finally:
        cursor.close()

def get_delta_row_count(spark, table):
    """Get row count from Delta table"""
    try:
        df = spark.read.format("delta").load(f"s3a://{MINIO_BUCKET}/delta/{table}")
        return df.count()
    except Exception as e:
        logger.error(f"Failed to get row count from Delta table {table}: {e}")
        return -1

def get_delta_checksum(spark, table):
    """Get partial checksum from Delta table"""
    try:
        df = spark.read.format("delta").load(f"s3a://{MINIO_BUCKET}/delta/{table}")
        # Sort by UUID and limit to first 100 rows for partial checksum
        df = df.orderBy("uuid").limit(100)
        # Convert all columns to string and concatenate for checksum
        df_checksum = df.select(hash(df.columns))
        checksum = str(df_checksum.collect()[0][0]) if df_checksum.count() > 0 else None
        return checksum
    except Exception as e:
        logger.error(f"Failed to get checksum from Delta table {table}: {e}")
        return None

def run_data_qa_checks():
    """Run data quality checks"""
    spark = create_spark_session()
    conn = get_pg_connection()
    
    try:
        for table in TABLES:
            logger.info(f"Running data quality checks for table: {table}")
            
            # Get row counts
            pg_count = get_postgres_row_count(conn, table)
            delta_count = get_delta_row_count(spark, table)
            
            # Get checksums
            pg_checksum = get_postgres_checksum(conn, table)
            delta_checksum = get_delta_checksum(spark, table)
            
            # Check row counts
            if pg_count >= 0 and delta_count >= 0:
                if pg_count == delta_count:
                    logger.info(f"Row count check PASSED for {table}: {pg_count} rows")
                else:
                    logger.warning(f"Row count check FAILED for {table}: PostgreSQL={pg_count}, Delta={delta_count}")
            
            # Check checksums
            if pg_checksum and delta_checksum:
                if pg_checksum == delta_checksum:
                    logger.info(f"Checksum check PASSED for {table}")
                else:
                    logger.warning(f"Checksum check FAILED for {table}: PostgreSQL={pg_checksum}, Delta={delta_checksum}")
            
            logger.info("-" * 50)
    
    except Exception as e:
        logger.error(f"Error running data quality checks: {e}")
    finally:
        if conn:
            conn.close()
        if spark:
            spark.stop()

def main():
    """Main function"""
    # Wait for services to be ready
    logger.info("Waiting for services to be ready...")
    time.sleep(60)
    
    # Run data quality checks periodically
    try:
        while True:
            run_data_qa_checks()
            logger.info("Sleeping for 5 minutes before next check...")
            time.sleep(300)  # Run checks every 5 minutes
    except KeyboardInterrupt:
        logger.info("Data quality checks interrupted")

if __name__ == "__main__":
    main()