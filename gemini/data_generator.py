# data_generator.py
import os
import time
import uuid
import random
from datetime import datetime
import psycopg2
from faker import Faker

# --- Configuration ---
PG_HOST = os.environ.get('PG_HOST', 'localhost')
PG_PORT = os.environ.get('PG_PORT', '5432')
PG_DATABASE = os.environ.get('PG_DATABASE', 'robot_data')
PG_USER = os.environ.get('PG_USER', 'user')
PG_PASSWORD = os.environ.get('PG_PASSWORD', 'password')

# Define the fixed list of Robot IDs
ROBOT_IDS = ['RobotID_01', 'RobotID_02', 'RobotID_03']

# Number of records to generate for each table
NUM_ROBOT_RECORDS = 30
NUM_TELEOP_RECORDS = 30

# Define sleep intervals
ROBOT_SLEEP_MIN = 1
ROBOT_SLEEP_MAX = 60
TELEOP_SLEEP_MIN = 30
TELEOP_SLEEP_MAX = 90


print(f"--- Data Generator Configuration ---")
print(f"PostgreSQL Host: {PG_HOST}:{PG_PORT}")
print(f"PostgreSQL DB: {PG_DATABASE}")
print(f"Robot IDs: {ROBOT_IDS}")
print(f"Generating {NUM_ROBOT_RECORDS} robot records (interval: {ROBOT_SLEEP_MIN}-{ROBOT_SLEEP_MAX}s)")
print(f"Generating {NUM_TELEOP_RECORDS} teleop records (interval: {TELEOP_SLEEP_MIN}-{TELEOP_SLEEP_MAX}s)")
print("------------------------------------")

fake = Faker()

def get_db_connection(retries=5, delay=5):
    """Attempts to connect to PostgreSQL with retries."""
    conn_str = f"dbname='{PG_DATABASE}' user='{PG_USER}' password='{PG_PASSWORD}' host='{PG_HOST}' port='{PG_PORT}'"
    for i in range(retries):
        try:
            print(f"Attempting to connect to PostgreSQL ({i+1}/{retries})...")
            conn = psycopg2.connect(conn_str)
            print("Successfully connected to PostgreSQL.")
            return conn
        except psycopg2.OperationalError as e:
            print(f"PostgreSQL connection failed: {e}")
            if i < retries - 1:
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print("Max retries reached. Could not connect to PostgreSQL.")
                raise
    return None

def generate_robot_data():
    """Generates a single record for the robot table using a predefined Robot ID."""
    robot_id = random.choice(ROBOT_IDS) # Select randomly from the list

    return {
        "UUID": str(uuid.uuid4()),
        "Robot_ID": robot_id,
        "Timestamp": datetime.now().isoformat(),
        "Event_ID": f"EVT_{random.choice(['STARTUP', 'MOVE', 'TASK_COMPLETE', 'IDLE', 'ERROR'])}",
    }

def generate_teleop_data():
    """Generates a single record for the teleop table using a predefined Robot ID."""
    robot_id = random.choice(ROBOT_IDS) # Select randomly from the list

    return {
        "UUID": str(uuid.uuid4()),
        "Robot_ID": robot_id,
        "Timestamp": datetime.now().isoformat(),
        "Event_ID": f"EVT_{random.choice(['CONNECT', 'COMMAND_SENT', 'VIEW_CHANGE', 'DISCONNECT'])}",
        "Teleoperator_ID": f"OPERATOR_{fake.user_name()}",
    }

def insert_data(conn, table_name, data):
    """Inserts a data dictionary into the specified table."""
    cursor = conn.cursor()
    columns = ', '.join(data.keys())
    placeholders = ', '.join(['%s'] * len(data))
    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    try:
        cursor.execute(sql, list(data.values()))
        conn.commit()
        # print(f"Inserted into {table_name}: {data['UUID']}") # Verbose logging
    except Exception as e:
        print(f"Error inserting into {table_name}: {e}")
        conn.rollback() # Rollback on error
    finally:
        cursor.close()

def main():
    conn = None
    try:
        conn = get_db_connection()
        print("Starting data generation...")

        # Generate robot data
        print(f"\n--- Generating {NUM_ROBOT_RECORDS} robot records ---")
        for i in range(NUM_ROBOT_RECORDS):
            robot_record = generate_robot_data()
            insert_data(conn, "robot", robot_record)
            print(f"Generated Robot Event {i+1}/{NUM_ROBOT_RECORDS}: {robot_record['UUID']} for {robot_record['Robot_ID']}")

            # Sleep for a random interval (except after the last record)
            if i < NUM_ROBOT_RECORDS - 1:
                sleep_time = random.uniform(ROBOT_SLEEP_MIN, ROBOT_SLEEP_MAX)
                print(f"  Sleeping for {sleep_time:.2f} seconds...")
                time.sleep(sleep_time)

        # Generate teleop data
        print(f"\n--- Generating {NUM_TELEOP_RECORDS} teleop records ---")
        for i in range(NUM_TELEOP_RECORDS):
            teleop_record = generate_teleop_data()
            insert_data(conn, "teleop", teleop_record)
            print(f"Generated Teleop Event {i+1}/{NUM_TELEOP_RECORDS}: {teleop_record['UUID']} for {teleop_record['Robot_ID']}")

            # Sleep for a random interval (except after the last record)
            if i < NUM_TELEOP_RECORDS - 1:
                sleep_time = random.uniform(TELEOP_SLEEP_MIN, TELEOP_SLEEP_MAX)
                print(f"  Sleeping for {sleep_time:.2f} seconds...")
                time.sleep(sleep_time)

        print("\nFinished generating all records.")

    except (Exception, KeyboardInterrupt) as e:
        print(f"\nGenerator stopped due to error: {e}")
    finally:
        if conn:
            print("Closing PostgreSQL connection...")
            conn.close()
        print("Data generator finished.")

if __name__ == "__main__":
    main()

