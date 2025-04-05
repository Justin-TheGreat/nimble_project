# data_generator.py
import psycopg2
import random
import uuid
import datetime

# Only these Robot_IDs will be used
ROBOT_IDS = ['RobotID_01', 'RobotID_02', 'RobotID_03']
TELEOP_IDS = ['TeleOp_01', 'TeleOp_02', 'TeleOp_03']

def generate_robot_rows(conn):
    cursor = conn.cursor()
    # Start time for robot table entries
    current_time = datetime.datetime.now()
    for _ in range(30):
        # Add a random interval between 1 and 60 seconds
        delay = random.randint(1, 60)
        current_time += datetime.timedelta(seconds=delay)
        robot_id = random.choice(ROBOT_IDS)
        new_uuid = str(uuid.uuid4())
        event_id = str(uuid.uuid4())
        sql = """
            INSERT INTO robot (UUID, Robot_ID, Timestamp, Event_ID)
            VALUES (%s, %s, %s, %s)
        """
        cursor.execute(sql, (new_uuid, robot_id, current_time, event_id))
        print(f"Robot: {new_uuid}, {robot_id}, {current_time}, {event_id}")
        conn.commit()
    cursor.close()

def generate_teleop_rows(conn):
    cursor = conn.cursor()
    # Start time for teleop table entries
    current_time = datetime.datetime.now()
    for _ in range(30):
        # Add a random interval between 30 and 90 seconds
        delay = random.randint(30, 90)
        current_time += datetime.timedelta(seconds=delay)
        robot_id = random.choice(ROBOT_IDS)
        new_uuid = str(uuid.uuid4())
        event_id = str(uuid.uuid4())
        teleoperator_id = random.choice(TELEOP_IDS)
        sql = """
            INSERT INTO teleop (UUID, Robot_ID, Timestamp, Event_ID, Teleoperator_ID)
            VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(sql, (new_uuid, robot_id, current_time, event_id, teleoperator_id))
        print(f"Teleop: {new_uuid}, {robot_id}, {current_time}, {event_id}, {teleoperator_id}")
        conn.commit()
    cursor.close()

def main():
    # Connect to Postgres (adjust host if running within docker network)
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        user="postgres",
        password="postgres",
        database="postgres"
    )
    
    print("Generating data for robot table...")
    generate_robot_rows(conn)
    print("Generating data for teleop table...")
    generate_teleop_rows(conn)
    
    conn.close()
    print("Data generation complete.")

if __name__ == "__main__":
    main()
