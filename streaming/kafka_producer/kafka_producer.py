import json
from datetime import datetime, timezone
from time import sleep
from kafka import KafkaProducer

KAFKA_TOPIC = "traffic_light_signals"
KAFKA_BROKER = "broker:9092"
id_counter = 3

def generate_traffic_light_data():
    global id_counter  # Access the global counter
    # Get current timestamp in ISO 8601 format (UTC)
    timestamp = datetime.now(timezone.utc).isoformat()
    id_counter += 1
    # Simulated data
    return {
        "traffic_light_id": str(id_counter),
        "primary_signal": 3,  # Green
        "secondary_signal": 1,  # Red
        "location": "Dammtor/Theodor-Heuss-Platz",
        "event_timestamp": timestamp
            #"2024-11-05T12:30:00Z"

    }


def produce_kafka_messages():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("Producing traffic light signals to Kafka...")
    while True:
        data = generate_traffic_light_data()
        producer.send(KAFKA_TOPIC, data)
        print(f"Sent: {data}")
        sleep(1)


if __name__ == "__main__":
    produce_kafka_messages()
