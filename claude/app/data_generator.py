import psycopg2
import uuid
import random
import time
import os
import logging
from datetime import datetime
from faker import Faker

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# PostgreSQL connection details
PG_HOST = os.environ.get('POSTGRES_HOST', 'postgres')
PG_PORT = os.environ.get('POSTGRES_PORT', '5432')
PG_DB = os.environ.get('POSTGRES_DB', 'robotdata')
PG_USER = os.environ.get('POSTGRES_USER', 'postgres')
PG_PASSWORD = os.environ.get('POSTGRES_PASSWORD', 'postgres')

# Initialize faker
fake = Faker()

# Configuration for specific robot IDs
ROBOT_IDS = ['RobotID_01', 'RobotID_02', 'RobotID_03']
TELEOP_COUNT = 10
ROBOT_BATCH_SIZE = 30
TELEOP_BATCH_SIZE = 30

# Event types
ROBOT_EVENTS = ['START', 'STOP', 'PAUSE', 'RESUME', 'ERROR', 'WARNING', 'INFO']
TELEOP_EVENTS = ['CONNECT', 'DISCONNECT', 'COMMAND', 'OVERRIDE', 'TAKEOVER', 'RELEASE']

def get_connection():
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

def generate_teleop_ids(count):
    """Generate teleoperator IDs"""
    return [f"TELEOP-{fake.unique.random_int(min=1000, max=9999)}" for _ in range(count)]

def generate_robot_event():
    """Generate a random robot event"""
    return {
        'UUID': str(uuid.uuid4()),
        'Robot_ID': random.choice(ROBOT_IDS),
        'Timestamp': datetime.now(),
        'Event_ID': random.choice(ROBOT_EVENTS)
    }

def generate_teleop_event():
    """Generate a random teleop event"""
    return {
        'UUID': str(uuid.uuid4()),
        'Robot_ID': random.choice(ROBOT_IDS),
        'Timestamp': datetime.now(),
        'Event_ID': random.choice(TELEOP_EVENTS),
        'Teleoperator_ID': random.choice(teleop_ids)
    }

def insert_robot_event(conn):
    """Insert a single robot event into the database"""
    cursor = conn.cursor()
    try:
        event = generate_robot_event()
        cursor.execute(
            "INSERT INTO robot (UUID, Robot_ID, Timestamp, Event_ID) VALUES (%s, %s, %s, %s)",
            (event['UUID'], event['Robot_ID'], event['Timestamp'], event['Event_ID'])
        )
        conn.commit()
        logger.info(f"Inserted robot event: {event}")
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to insert robot event: {e}")
    finally:
        cursor.close()

def insert_teleop_event(conn):
    """Insert a single teleop event into the database"""
    cursor = conn.cursor()
    try:
        event = generate_teleop_event()
        cursor.execute(
            "INSERT INTO teleop (UUID, Robot_ID, Timestamp, Event_ID, Teleoperator_ID) VALUES (%s, %s, %s, %s, %s)",
            (event['UUID'], event['Robot_ID'], event['Timestamp'], event['Event_ID'], event['Teleoperator_ID'])
        )
        conn.commit()
        logger.info(f"Inserted teleop event: {event}")
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to insert teleop event: {e}")
    finally:
        cursor.close()

def main():
    """Main function"""
    global teleop_ids
    
    # Wait for PostgreSQL to be ready
    logger.info("Waiting for PostgreSQL to be ready...")
    time.sleep(15)
    
    # Generate teleoperator IDs
    teleop_ids = generate_teleop_ids(TELEOP_COUNT)
    logger.info(f"Using robot IDs: {ROBOT_IDS}")
    logger.info(f"Generated {len(teleop_ids)} teleoperator IDs")
    
    conn = get_connection()
    
    try:
        # Generate robot data (30 rows, interval 1-60 seconds)
        robot_count = 0
        while robot_count < ROBOT_BATCH_SIZE:
            insert_robot_event(conn)
            robot_count += 1
            
            # Random sleep between 1 and 60 seconds
            sleep_time = random.uniform(1, 60)
            logger.info(f"Robot event {robot_count}/{ROBOT_BATCH_SIZE} generated. Sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
        
        logger.info(f"Completed generating {ROBOT_BATCH_SIZE} robot events")
        
        # Generate teleop data (30 rows, interval 30-90 seconds)
        teleop_count = 0
        while teleop_count < TELEOP_BATCH_SIZE:
            insert_teleop_event(conn)
            teleop_count += 1
            
            # Random sleep between 30 and 90 seconds
            sleep_time = random.uniform(30, 90)
            logger.info(f"Teleop event {teleop_count}/{TELEOP_BATCH_SIZE} generated. Sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
        
        logger.info(f"Completed generating {TELEOP_BATCH_SIZE} teleop events")
    
    except KeyboardInterrupt:
        logger.info("Data generation interrupted")
    except Exception as e:
        logger.error(f"Error in data generation: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()