# cdc_listener.py
import os
import time
import select
import json
import psycopg2
import psycopg2.extensions
from kafka import KafkaProducer

# --- Configuration ---
PG_HOST = os.environ.get('PG_HOST', 'localhost')
PG_PORT = os.environ.get('PG_PORT', '5432')
PG_DATABASE = os.environ.get('PG_DATABASE', 'robot_data')
PG_USER = os.environ.get('PG_USER', 'user')
PG_PASSWORD = os.environ.get('PG_PASSWORD', 'password')
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'localhost:9092')

# Kafka topics should match the pg_notify channels or be derived from them
KAFKA_ROBOT_TOPIC = 'robot_cdc'
KAFKA_TELEOP_TOPIC = 'teleop_cdc'

LISTEN_CHANNELS = ['robot_changes', 'teleop_changes']

print(f"--- CDC Listener Configuration ---")
print(f"PostgreSQL Host: {PG_HOST}:{PG_PORT}")
print(f"PostgreSQL DB: {PG_DATABASE}")
print(f"Kafka Broker: {KAFKA_BROKER}")
print(f"Listening to channels: {LISTEN_CHANNELS}")
print(f"Target Kafka Topics: Robot -> {KAFKA_ROBOT_TOPIC}, Teleop -> {KAFKA_TELEOP_TOPIC}")
print("---------------------------------")

def get_kafka_producer(retries=5, delay=5):
    """Attempts to connect to Kafka with retries."""
    for i in range(retries):
        try:
            print(f"Attempting to connect to Kafka ({i+1}/{retries})...")
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all', # Ensure message is received by all in-sync replicas
                retries=3 # Kafka producer internal retries
            )
            print("Successfully connected to Kafka.")
            return producer
        except Exception as e:
            print(f"Kafka connection failed: {e}")
            if i < retries - 1:
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print("Max retries reached. Could not connect to Kafka.")
                raise
    return None # Should not be reached if exception is raised

def get_db_connection(retries=5, delay=5):
    """Attempts to connect to PostgreSQL with retries."""
    conn_str = f"dbname='{PG_DATABASE}' user='{PG_USER}' password='{PG_PASSWORD}' host='{PG_HOST}' port='{PG_PORT}'"
    for i in range(retries):
        try:
            print(f"Attempting to connect to PostgreSQL ({i+1}/{retries})...")
            conn = psycopg2.connect(conn_str)
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
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
    return None # Should not be reached if exception is raised


def main():
    producer = None
    conn = None
    try:
        producer = get_kafka_producer()
        conn = get_db_connection()
        cursor = conn.cursor()

        # Start listening on the specified channels
        for channel in LISTEN_CHANNELS:
            cursor.execute(f"LISTEN {channel};")
            print(f"Listening on PostgreSQL channel: {channel}")

        print("Waiting for notifications...")
        while True:
            # Check if there's anything to read from the connection's socket
            # Timeout ensures the loop doesn't block indefinitely if Python signal handling is needed
            if select.select([conn], [], [], 60) == ([], [], []):
                # print("Timeout: No notification received in the last 60 seconds.")
                pass # No data received, loop again
            else:
                # Process available data
                conn.poll()
                while conn.notifies:
                    notification = conn.notifies.pop(0)
                    channel_name = notification.channel
                    payload_str = notification.payload
                    print(f"\nReceived notification on channel '{channel_name}':")
                    # print(f"Payload: {payload_str}") # Debugging

                    try:
                        # Parse the JSON payload sent by the trigger
                        payload_json = json.loads(payload_str)
                        table_name = payload_json.get('table')
                        action = payload_json.get('action')
                        data = payload_json.get('data')

                        # Determine the target Kafka topic based on the channel/table
                        target_topic = None
                        if channel_name == 'robot_changes' or table_name == 'robot':
                            target_topic = KAFKA_ROBOT_TOPIC
                        elif channel_name == 'teleop_changes' or table_name == 'teleop':
                            target_topic = KAFKA_TELEOP_TOPIC

                        if target_topic and action == 'INSERT' and data: # Only process INSERTs for now
                            print(f"Forwarding INSERT from table '{table_name}' to Kafka topic '{target_topic}'")
                            # Send the 'data' part of the payload to Kafka
                            producer.send(target_topic, value=data)
                            # Flush to ensure message is sent immediately (optional, adjust for performance)
                            # producer.flush()
                        else:
                            print(f"Ignoring notification (Channel: {channel_name}, Action: {action}, Topic: {target_topic})")

                    except json.JSONDecodeError as e:
                        print(f"Error decoding JSON payload: {e}")
                        print(f"Raw Payload: {payload_str}")
                    except Exception as e:
                        print(f"Error processing notification or sending to Kafka: {e}")
                        # Consider adding error handling/retry logic here if needed

    except (Exception, KeyboardInterrupt) as e:
        print(f"\nShutting down listener due to: {e}")
    finally:
        if producer:
            print("Flushing Kafka producer...")
            producer.flush()
            print("Closing Kafka producer...")
            producer.close()
        if conn:
            print("Closing PostgreSQL connection...")
            conn.close()
        print("CDC Listener stopped.")

if __name__ == "__main__":
    main()

