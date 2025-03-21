import json
import random
import time
from datetime import datetime, timezone, timedelta

from kafka import KafkaProducer

KAFKA_TOPIC = "traffic_light_signals"
KAFKA_BROKERS = ["broker-1:9092", "broker-2:9093"]

def generate_traffic_light_data():
    traffic_light_id = random.randint(1, 5)

    primary_signal = random.randint(1, 3)
    secondary_signal = random.randint(1, 3)

    timestamp = (datetime.now(timezone.utc) + timedelta(seconds=random.randint(0, 30))).isoformat()

    return {
        "traffic_light_id": str(traffic_light_id),
        "primary_signal": primary_signal,
        "secondary_signal": secondary_signal,
        "location": "Dammtor/Theodor-Heuss-Platz",
        "event_timestamp": timestamp
    }

def produce_kafka_messages():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("Producing traffic light signals to Kafka...")
    while True:
        data = generate_traffic_light_data()
        producer.send(KAFKA_TOPIC, data)
        print(f"Sent: {data}")
        time.sleep(random.uniform(0.5, 2.0))

if __name__ == "__main__":
    produce_kafka_messages()
