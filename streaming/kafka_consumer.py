import json
import logging
import warnings
import time

warnings.simplefilter("ignore")

from dateutil import parser

import pandas as pd
from feast import FeatureStore
from kafka import KafkaConsumer

logger = logging.getLogger('kafka_consumer')
TRAFFIC_LIGHT_TOPIC = "traffic_light_signals"
BENCHMARK_TOPIC = "benchmark_entity_topic"
KAFKA_BROKERS = ["broker-1:9092"]

# Initialize the Feast feature store
store = FeatureStore(repo_path="./")

def process_traffic_light_message(message, message_retrieval_time):
    """
    Persist Kafka-transformed message to Feast online store and batch source.
    """
    # Deserialize the Kafka message
    data = json.loads(message.value.decode("utf-8"))
    entity_rows = [
        {"traffic_light_id": "1"}, {"traffic_light_id": "2"}, {"traffic_light_id": "3"},
        {"traffic_light_id": "4"}, {"traffic_light_id": "5"}
    ]
    online_features = store.get_online_features(
        features=[
            "traffic_light_windowed_features:avg_signal_duration_minutes",
            "traffic_light_windowed_features:primary_signal_count",
            "traffic_light_windowed_features:secondary_signal_count",
            "traffic_light_windowed_features:total_windowed_primary_signal_duration",
            "traffic_light_windowed_features:total_windowed_secondary_signal_duration",
        ],
        entity_rows=entity_rows
    ).to_dict()
    print("traffic_light_windowed_features:", online_features)
    data_retrieval_time = time.time()
    print("data_retrieval_time:", data_retrieval_time)

def create_benchmark_counter():
    """
    Creates a closure for counting processed benchmark messages.
    """
    count = 0  # Initialize counter at 0

    def count_up():
        nonlocal count
        count += 1
        return count

    return count_up
benchmark_counter = create_benchmark_counter()

def process_benchmark_message(message):
    data = json.loads(message.value.decode("utf-8"))
    entity_id = benchmark_counter()  # Increment and get the current ID

    start_time = time.time()
    print(f"Processing benchmark data for entity ID: {entity_id}")
    online_features = store.get_online_features(
        features=[
            "feature_sum:sum",
        ],
        entity_rows=[{"benchmark_entity": entity_id}]
    ).to_dict()
    end_time = time.time()

    print(f"benchmark online_features : {online_features}")
    print(f"benchmark data Retrieval took {end_time - start_time:.2f} seconds for entity ID {entity_id}")

def consume_kafka_messages():
    """
    Consumes messages from Kafka and processes them based on the topic.
    """
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BROKERS,
        auto_offset_reset='earliest',
        group_id="feast-persist-consumer"
    )
    consumer.subscribe([TRAFFIC_LIGHT_TOPIC, BENCHMARK_TOPIC])
    print("Consuming messages from Kafka...")
    for message in consumer:
        print("message in topic: ", message.topic)
        message_retrieval_time = time.time()
        if message.topic == TRAFFIC_LIGHT_TOPIC:
            process_traffic_light_message(message, message_retrieval_time)
        elif message.topic == BENCHMARK_TOPIC:
            process_benchmark_message(message)


if __name__ == "__main__":
    consume_kafka_messages()
