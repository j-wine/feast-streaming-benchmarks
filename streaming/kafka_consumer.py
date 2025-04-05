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
KAFKA_TOPIC = "traffic_light_signals"
KAFKA_BROKERS = ["broker-1:9092", "broker-2:9093"]

# Initialize the Feast feature store
store = FeatureStore(repo_path="./")

# Cache for last signal timestamps
last_signal_timestamps = {}


def calculate_signal_duration(traffic_light_id, current_timestamp):
    """
    Calculate the signal duration based on the last signal timestamp.
    """
    last_timestamp = last_signal_timestamps.get(traffic_light_id)
    if last_timestamp is None:
        signal_duration = 0  # Initialize with 0 if no previous timestamp exists
    else:
        signal_duration = (current_timestamp - last_timestamp).total_seconds()
    # Update the cache with the current timestamp
    last_signal_timestamps[traffic_light_id] = current_timestamp
    return signal_duration


def persist_to_feast_and_batch(message, message_retrieval_time):
    """
    Persist Kafka-transformed message to Feast online store and batch source.
    """
    # Deserialize the Kafka message
    data = json.loads(message.value.decode("utf-8"))
    entity_rows = [
        {"traffic_light_id": "1"}, {"traffic_light_id": "2"}, {"traffic_light_id": "3"},
        {"traffic_light_id": "4"}, {"traffic_light_id": "5"}
    ]
    online_df = store.get_online_features(
        features=[
            "traffic_light_transformed_features:signal_duration_minutes"
        ], entity_rows=entity_rows).to_df()
    print("traffic_light_features_stream:signal_duration_minutes:\n", online_df)
    data_retrieval_time = time.time()
    #print(data_retrieval_time - message_retrieval_time)
    print("data_retrieval_time:", data_retrieval_time)
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
    print("post push traffic_light_windowed_features:", online_features)



def consume_kafka_messages():
    """
    Consumes messages from Kafka and persists them to Feast online store and batch source(=offline store)
    """
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKERS,
        group_id="feast-persist-consumer",
    )
    print("Consuming messages from Kafka...")
    for message in consumer:
        message_retrieval_time = time.time()
        persist_to_feast_and_batch(message, message_retrieval_time)


if __name__ == "__main__":
    consume_kafka_messages()
