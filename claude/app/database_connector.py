import psycopg2
import psycopg2.extensions
import select
import json
from confluent_kafka import Producer
import os
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# PostgreSQL connection details
PG_HOST = os.environ.get('POSTGRES_HOST', 'postgres')
PG_PORT = os.environ.get('POSTGRES_PORT', '5432')
PG_DB = os.environ.get('POSTGRES_DB', 'robotdata')
PG_USER = os.environ.get('POSTGRES_USER', 'postgres')
PG_PASSWORD = os.environ.get('POSTGRES_PASSWORD', 'postgres')

# Kafka connection details
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')

# Topic names
ROBOT_TOPIC = 'robot_events'
TELEOP_TOPIC = 'teleop_events'

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result """
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

def setup_kafka_producer():
    """Set up and return a Kafka producer"""
    return Producer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'postgres-cdc-connector'
    })

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
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        raise

def listen_for_changes(conn, producer):
    """Listen for notifications and produce to Kafka"""
    cursor = conn.cursor()
    cursor.execute("LISTEN robot_changes;")
    cursor.execute("LISTEN teleop_changes;")
    
    logger.info("Listening for PostgreSQL notifications...")
    
    while True:
        if select.select([conn], [], [], 5) == ([], [], []):
            # timeout, do nothing
            continue
        
        conn.poll()
        while conn.notifies:
            notify = conn.notifies.pop(0)
            payload = json.loads(notify.payload)
            topic = ROBOT_TOPIC if payload.get('table') == 'robot' else TELEOP_TOPIC
            
            # Produce message to Kafka
            producer.produce(
                topic=topic,
                key=payload.get('uuid'),
                value=json.dumps(payload),
                callback=delivery_report
            )
            producer.flush()
            
            logger.info(f"Message sent to Kafka topic {topic}: {payload}")

def main():
    """Main function"""
    # Wait for PostgreSQL to be ready
    time.sleep(10)
    
    max_retries = 5
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            producer = setup_kafka_producer()
            conn = get_pg_connection()
            listen_for_changes(conn, producer)
        except Exception as e:
            retry_count += 1
            logger.error(f"Error in CDC connector: {e}")
            logger.info(f"Retrying in 10 seconds... (Attempt {retry_count}/{max_retries})")
            time.sleep(10)
        finally:
            if 'conn' in locals() and conn:
                conn.close()

if __name__ == "__main__":
    main()