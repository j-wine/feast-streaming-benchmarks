import json
import logging

import pandas as pd
from feast import FeatureStore
from kafka import KafkaConsumer
logger = logging.getLogger('kafka_consumer')

KAFKA_TOPIC = "processed_traffic_light_signals"
KAFKA_BROKER = "broker:9092"
BATCH_SOURCE_PATH = "../offline_data/traffic_light_data.parquet"

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


def persist_to_feast_and_batch(message):
    """
    Persist Kafka-transformed message to Feast online store and batch source.
    """
    # Deserialize the Kafka message
    data = json.loads(message.value.decode("utf-8"))

    # Use Feast's event timestamp for processing
    try:
        current_timestamp = pd.to_datetime(data["timestamp"], utc=True)
    except Exception as e:
        print(f"Invalid timestamp in message: {data['timestamp']}, Error: {e}")
        return

    # Calculate signal duration
    traffic_light_id = data["traffic_light_id"]
    signal_duration = calculate_signal_duration(traffic_light_id, current_timestamp)

    # Enrich the data with the computed feature
    data["signal_duration"] = signal_duration
    data["event_timestamp"] = current_timestamp.isoformat()

    # Convert the data into a DataFrame
    df = pd.DataFrame([{
        "traffic_light_id": traffic_light_id,
        "primary_signal": data["primary_signal"],
        "secondary_signal": data.get("secondary_signal"),
        "location": data.get("location"),
        "signal_duration": signal_duration,
        "event_timestamp": data["event_timestamp"],
    }])

    # Persist the DataFrame to the Feast online store
    # For debugging Shows all registered feature views
    logger.log(level=20, msg=store.list_feature_views())
    # @BA !doesnt trigger the feature views registered transformations !
    # feature_view_name uses the name of the feature view as string
    # for decorator tag @stream_feature_view the name is the method name
    store.write_to_online_store(feature_view_name="traffic_light_features_stream", df=df)
    print(f"Persisted data to Feast:\n{df}")

    # Append the data to the batch source for historical storage
    df.to_parquet(BATCH_SOURCE_PATH, mode="a", index=False)
    # @BA @TODO test in comparison to write_to_offline_store
    # store.write_to_offline_store(feature_view_name="traffic_light_features_stream", df=df)
    print(f"Written to batch source:\n{df}")


def consume_kafka_messages():
    """
    Consumes messages from Kafka and persists them to Feast online store and batch source(=offline store)
    """
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id="feast-persist-consumer",
    )
    print("Consuming messages from Kafka...")
    for message in consumer:
        persist_to_feast_and_batch(message)


if __name__ == "__main__":
    consume_kafka_messages()
