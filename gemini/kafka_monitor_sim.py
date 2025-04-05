# kafka_monitor_sim.py
import os
import time
import json
import random
import psutil # To get *some* host metrics (will reflect the container's view)
from datetime import datetime, timezone
from kafka import KafkaProducer

# --- Configuration ---
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'localhost:9092')
KAFKA_METRICS_TOPIC = 'kafka_metrics'
MONITOR_INTERVAL_SECONDS = int(os.environ.get('MONITOR_INTERVAL_SECONDS', 15))
SIMULATED_BROKER_ID = "kafka-broker-1" # Simulate metrics for this ID

print(f"--- Kafka Metrics Simulator Configuration ---")
print(f"Kafka Broker: {KAFKA_BROKER}")
print(f"Metrics Topic: {KAFKA_METRICS_TOPIC}")
print(f"Monitor Interval: {MONITOR_INTERVAL_SECONDS} seconds")
print("-------------------------------------------")

def get_kafka_producer(retries=5, delay=5):
    """Attempts to connect to Kafka with retries."""
    for i in range(retries):
        try:
            print(f"Attempting to connect to Kafka ({i+1}/{retries})...")
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='1' # Acks=1 is usually sufficient for metrics
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
    return None

def get_simulated_metrics():
    """Generates simulated Kafka broker metrics."""
    # Get actual CPU/Memory usage of the *container* running this script
    cpu_percent = psutil.cpu_percent(interval=None) # Use interval=None for non-blocking call
    memory_info = psutil.virtual_memory()
    memory_percent = memory_info.percent

    # Simulate Disk I/O (psutil can get real disk I/O, but might need root or specific setup)
    # For simplicity, we simulate these values.
    disk_read_mbps = round(random.uniform(1, 20), 2)
    disk_write_mbps = round(random.uniform(5, 50), 2)

    # Simulate Kafka-specific metrics (these would normally come from JMX)
    messages_in_per_sec = random.randint(50, 500)
    bytes_in_per_sec = messages_in_per_sec * random.randint(500, 1500)
    bytes_out_per_sec = messages_in_per_sec * random.randint(400, 1200) # Simulate slightly less out

    timestamp = datetime.now(timezone.utc).isoformat()

    metrics = {
        "broker_id": SIMULATED_BROKER_ID,
        "timestamp": timestamp,
        "cpu_percent": cpu_percent,
        "memory_percent": memory_percent,
        "disk_read_mbps": disk_read_mbps, # Simulated
        "disk_write_mbps": disk_write_mbps, # Simulated
        "messages_in_per_sec": messages_in_per_sec, # Simulated Kafka metric
        "bytes_in_per_sec": bytes_in_per_sec, # Simulated Kafka metric
        "bytes_out_per_sec": bytes_out_per_sec, # Simulated Kafka metric
    }
    return metrics

def main():
    producer = None
    try:
        producer = get_kafka_producer()
        print("Starting metrics simulation loop...")
        while True:
            metrics_data = get_simulated_metrics()
            try:
                print(f"Sending metrics: {metrics_data}")
                producer.send(KAFKA_METRICS_TOPIC, value=metrics_data)
                producer.flush() # Send immediately
            except Exception as e:
                print(f"Error sending metrics to Kafka: {e}")
                # Optional: try to reconnect producer if send fails
                try:
                    producer.close()
                except: pass
                producer = get_kafka_producer() # Attempt reconnect

            time.sleep(MONITOR_INTERVAL_SECONDS)

    except (Exception, KeyboardInterrupt) as e:
        print(f"\nShutting down metrics simulator due to: {e}")
    finally:
        if producer:
            print("Closing Kafka producer...")
            producer.close()
        print("Metrics simulator stopped.")

if __name__ == "__main__":
    main()

