-- postgres/init.sql

-- Enable PL/Python3
CREATE EXTENSION IF NOT EXISTS plpython3u;

-- Create tables
CREATE TABLE robot (
    UUID VARCHAR PRIMARY KEY,
    Robot_ID VARCHAR,
    Timestamp TIMESTAMP,
    Event_ID VARCHAR
);

CREATE TABLE teleop (
    UUID VARCHAR PRIMARY KEY,
    Robot_ID VARCHAR,
    Timestamp TIMESTAMP,
    Event_ID VARCHAR,
    Teleoperator_ID VARCHAR
);

-- Trigger function to send data to Kafka using PL/Python
CREATE OR REPLACE FUNCTION send_to_kafka() RETURNS trigger AS $$
import json
from kafka import KafkaProducer
# Connect to Kafka (note: 'kafka' is the hostname from docker-compose)
producer = KafkaProducer(bootstrap_servers='kafka:9092')
# Create a payload with operation and row data
payload = {'action': TG_OP, 'data': dict(NEW)}
producer.send('cdc_topic', json.dumps(payload).encode('utf-8'))
producer.flush()
return NEW
$$ LANGUAGE plpython3u;

-- Create triggers on insert for each table
CREATE TRIGGER robot_cdc_trigger
AFTER INSERT ON robot
FOR EACH ROW EXECUTE FUNCTION send_to_kafka();

CREATE TRIGGER teleop_cdc_trigger
AFTER INSERT ON teleop
FOR EACH ROW EXECUTE FUNCTION send_to_kafka();
