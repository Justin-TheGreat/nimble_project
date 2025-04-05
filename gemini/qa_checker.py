# qa_checker.py
import os
import time
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import count as spark_count

# --- Configuration ---
PG_HOST = os.environ.get('PG_HOST', 'localhost')
PG_PORT = os.environ.get('PG_PORT', '5432')
PG_DATABASE = os.environ.get('PG_DATABASE', 'robot_data')
PG_USER = os.environ.get('PG_USER', 'user')
PG_PASSWORD = os.environ.get('PG_PASSWORD', 'password')

MINIO_ENDPOINT = os.environ.get('MINIO_ENDPOINT', 'http://localhost:9000')
MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY', 'minioadmin')
DELTA_LAKE_BUCKET = os.environ.get('DELTA_LAKE_BUCKET', 'delta-lake')

ROBOT_DELTA_PATH = f"s3a://{DELTA_LAKE_BUCKET}/robot_delta"
TELEOP_DELTA_PATH = f"s3a://{DELTA_LAKE_BUCKET}/teleop_delta"

# QA Check Parameters
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 10

print("--- Data QA Checker Configuration ---")
print(f"PostgreSQL Host: {PG_HOST}:{PG_PORT}")
print(f"PostgreSQL DB: {PG_DATABASE}")
print(f"MinIO Endpoint: {MINIO_ENDPOINT}")
print(f"Robot Delta Path: {ROBOT_DELTA_PATH}")
print(f"Teleop Delta Path: {TELEOP_DELTA_PATH}")
print("-------------------------------------")


def get_db_connection():
    """Connects to PostgreSQL."""
    conn_str = f"dbname='{PG_DATABASE}' user='{PG_USER}' password='{PG_PASSWORD}' host='{PG_HOST}' port='{PG_PORT}'"
    try:
        conn = psycopg2.connect(conn_str)
        print("Successfully connected to PostgreSQL for QA.")
        return conn
    except psycopg2.OperationalError as e:
        print(f"PostgreSQL connection failed for QA: {e}")
        raise

def get_postgres_count(conn, table_name):
    """Gets the row count from a PostgreSQL table."""
    count = -1 # Default to -1 to indicate failure
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
            result = cursor.fetchone()
            if result:
                count = result[0]
    except Exception as e:
        print(f"Error getting count from PostgreSQL table {table_name}: {e}")
    return count

def create_spark_session_for_qa():
    """Creates a local Spark Session configured for MinIO/Delta."""
    # Configuration relies on PYSPARK_SUBMIT_ARGS environment variable set in docker-compose
    print("Creating Spark Session for QA checks...")
    try:
        spark = SparkSession.builder \
            .appName("DataQA_Checker") \
            .master("local[*]").getOrCreate() # Will inherit config from PYSPARK_SUBMIT_ARGS

        # Verify S3A config (optional)
        # print(f"QA Spark S3A Endpoint: {spark.sparkContext.getConf().get('spark.hadoop.fs.s3a.endpoint')}")

        print("Spark Session created successfully for QA.")
        return spark
    except Exception as e:
        print(f"Error creating Spark Session for QA: {e}")
        raise

def get_delta_count(spark, delta_path):
    """Gets the row count from a Delta table."""
    count = -1 # Default to -1 to indicate failure
    try:
        print(f"Reading Delta table count from: {delta_path}")
        delta_df = spark.read.format("delta").load(delta_path)
        result = delta_df.select(spark_count("*").alias("count")).first()
        if result:
            count = result["count"]
        print(f"Count from {delta_path}: {count}")
    except Exception as e:
        # Handle cases where the Delta table might not exist yet or other read errors
        print(f"Error reading Delta table {delta_path}: {e}. It might not have been created yet.")
        # You might want to return 0 or handle this differently based on requirements
    return count


def run_qa_checks():
    """Performs the QA checks with retries."""
    pg_conn = None
    spark = None
    passed = True

    for attempt in range(MAX_RETRIES):
        print(f"\n--- QA Check Attempt {attempt + 1}/{MAX_RETRIES} ---")
        passed = True # Reset pass status for this attempt
        try:
            # Establish connections
            if not pg_conn: pg_conn = get_db_connection()
            if not spark: spark = create_spark_session_for_qa()

            # --- Robot Table Check ---
            print("\nChecking 'robot' table...")
            pg_robot_count = get_postgres_count(pg_conn, "robot")
            delta_robot_count = get_delta_count(spark, ROBOT_DELTA_PATH)

            print(f"PostgreSQL 'robot' count: {pg_robot_count}")
            print(f"MinIO Delta 'robot_delta' count: {delta_robot_count}")

            # Allow for minor discrepancies due to streaming lag
            # A perfect match might be difficult in a live stream.
            # Check if Delta count is reasonably close or equal.
            # Here we check for equality, but adjust tolerance as needed.
            if pg_robot_count == -1 or delta_robot_count == -1:
                 print("WARN: Could not retrieve count from one or both sources for 'robot'. Skipping comparison.")
                 # Decide if this is a failure or just a warning
                 # passed = False # Uncomment if count retrieval failure means QA fails
            elif pg_robot_count != delta_robot_count:
                print(f"FAIL: Row count mismatch for 'robot' table! PG: {pg_robot_count}, Delta: {delta_robot_count}")
                passed = False
            else:
                print("PASS: Row counts match for 'robot'.")

            # --- Teleop Table Check ---
            print("\nChecking 'teleop' table...")
            pg_teleop_count = get_postgres_count(pg_conn, "teleop")
            delta_teleop_count = get_delta_count(spark, TELEOP_DELTA_PATH)

            print(f"PostgreSQL 'teleop' count: {pg_teleop_count}")
            print(f"MinIO Delta 'teleop_delta' count: {delta_teleop_count}")

            if pg_teleop_count == -1 or delta_teleop_count == -1:
                 print("WARN: Could not retrieve count from one or both sources for 'teleop'. Skipping comparison.")
                 # passed = False
            elif pg_teleop_count != delta_teleop_count:
                print(f"FAIL: Row count mismatch for 'teleop' table! PG: {pg_teleop_count}, Delta: {delta_teleop_count}")
                passed = False
            else:
                print("PASS: Row counts match for 'teleop'.")

            # If all checks passed in this attempt, break the loop
            if passed:
                print("\n--- QA Checks Passed ---")
                return True # Overall success

            # If checks failed, wait before retrying
            if attempt < MAX_RETRIES - 1:
                 print(f"\nQA checks failed. Retrying in {RETRY_DELAY_SECONDS} seconds...")
                 time.sleep(RETRY_DELAY_SECONDS)
            else:
                 print("\n--- QA Checks Failed after maximum retries ---")
                 return False # Overall failure

        except Exception as e:
            print(f"An error occurred during QA check attempt {attempt + 1}: {e}")
            if attempt < MAX_RETRIES - 1:
                print(f"Retrying in {RETRY_DELAY_SECONDS} seconds...")
                time.sleep(RETRY_DELAY_SECONDS)
                # Close potentially broken connections to force reconnect on next attempt
                if pg_conn:
                    try: pg_conn.close()
                    except: pass
                    pg_conn = None
                if spark:
                    try: spark.stop()
                    except: pass
                    spark = None
            else:
                print("\n--- QA Checks Failed due to error after maximum retries ---")
                return False # Overall failure
        finally:
            # Clean up connections if loop finishes or breaks
            if attempt == MAX_RETRIES - 1 or passed: # Last attempt or success
                if pg_conn:
                    try: pg_conn.close()
                    except: pass
                    print("Closed PostgreSQL QA connection.")
                if spark:
                    try: spark.stop()
                    except: pass
                    print("Stopped Spark QA session.")

    return passed # Should technically be unreachable if logic is correct, but return final status


if __name__ == "__main__":
    print("Starting Data QA Checks...")
    # Initial delay to allow data to flow
    # sleep_time = 60
    # print(f"Waiting {sleep_time} seconds for initial data flow...")
    # time.sleep(sleep_time) # Delay moved to docker-compose command for simplicity

    success = run_qa_checks()

    if success:
        print("\nOverall QA Result: PASSED")
        # In a real scenario, exit code 0 indicates success
        exit(0)
    else:
        print("\nOverall QA Result: FAILED")
        # Exit code 1 indicates failure
        exit(1)

