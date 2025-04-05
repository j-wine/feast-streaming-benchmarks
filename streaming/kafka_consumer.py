import json
import logging
import warnings

warnings.simplefilter("ignore")

from dateutil import parser

import pandas as pd
from feast import FeatureStore
from kafka import KafkaConsumer

logger = logging.getLogger('kafka_consumer')
KAFKA_TOPIC = "traffic_light_signals"
KAFKA_BROKERS = ["broker-1:9092", "broker-2:9093"]

store = FeatureStore(repo_path="./")

def process_kafka_message(message):

    # Deserialize the Kafka message
    data = json.loads(message.value.decode("utf-8"))
    traffic_light_id = int(data["traffic_light_id"])  # Convert to integer for calculations
    entity_rows = [
        {"traffic_light_id": "1"}, {"traffic_light_id": "2"}, {"traffic_light_id": "3"},
        {"traffic_light_id": "4"}, {"traffic_light_id": "5"}
    ]
    # Convert the data into a DataFrame in format of traffic_light_stream_source KafkaSource
    df = pd.DataFrame([{
        "traffic_light_id": traffic_light_id,
        "primary_signal": data["primary_signal"],
        "secondary_signal": data.get("secondary_signal"),
        "location": data.get("location"),
        "event_timestamp": data["event_timestamp"],

    }])
    store.write_to_online_store(feature_view_name="traffic_light_features_stream", df=df)
    print("store.write_to_online_store(feature_view_name=""traffic_light_features_stream"", df=, )", df)
    # doenst work, probably due to path error when accessing parquet file
    # store.write_to_offline_store(feature_view_name="traffic_light_stats", df=df)
    # print("store.write_to_offline_store(feature_view_name=""traffic_light_stats"", df=, )", df)

    online_df = store.get_online_features(
        features=["traffic_light_features_stream:signal_sum"],
        entity_rows=entity_rows
    ).to_df()
    print("post write,pre push traffic_light_features_stream:signal_sum:\n", online_df)

    store.push("push_source",df)


    # @BA !doesnt trigger the feature views registered transformations !
    online_df = store.get_online_features(
        features=[
            "traffic_light_features_stream:signal_sum"

        ], entity_rows=entity_rows).to_df()
    print("post push online_df traffic_light_features_stream:signal_sum \n", online_df)


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

        process_kafka_message(message)


if __name__ == "__main__":
    consume_kafka_messages()
