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
    traffic_light_id = int(data["traffic_light_id"])  # Convert to integer for calculations
    entity_rows = [
        {"traffic_light_id": "1"}, {"traffic_light_id": "2"}, {"traffic_light_id": "3"},
        {"traffic_light_id": "4"}, {"traffic_light_id": "5"}
    ]

    try:
        # Ensure timestamp is correctly parsed with timezone
        current_timestamp = parser.isoparse(data["event_timestamp"]).astimezone()
    except Exception as e:
        print(f"❌ Invalid timestamp: {data['event_timestamp']}, Error: {e}")
        return

    # Calculate signal duration
    # @BA  transformation uses a variable keep track of the last signals timestamp
    # therefore transformation is attractive to do in consumer instead of registering in on-demand/stream feature view
    # @BA show an alternative way to do using feast
    signal_duration = calculate_signal_duration(traffic_light_id, current_timestamp)

    # Enrich the data with the computed feature
    data["signal_duration"] = signal_duration
    data["event_timestamp"] = current_timestamp.isoformat()

    # Convert the data into a DataFrame in format of traffic_light_stream_source KafkaSource
    df = pd.DataFrame([{
        "traffic_light_id": traffic_light_id,
        "primary_signal": data["primary_signal"],
        "secondary_signal": data.get("secondary_signal"),
        "location": data.get("location"),
        "signal_duration": signal_duration,
        "event_timestamp": data["event_timestamp"],

    }])
    store.write_to_online_store(feature_view_name="traffic_light_stats", df=df)
    print("store.write_to_online_store(feature_view_name=""traffic_light_stats"", df=, )", df)

    store.write_to_offline_store(feature_view_name="traffic_light_stats", df=df)
    print("store.write_to_offline_store(feature_view_name=""traffic_light_stats"", df=, )", df)

    online_df = store.get_online_features(
        features=[
            "traffic_light_stats:primary_signal",
            "traffic_light_stats:secondary_signal",
            "traffic_light_stats:location",
            "traffic_light_stats:signal_duration",
        ],   entity_rows=entity_rows   ).to_df()
    print("traffic_light_stats after write_to_online_store,before push:\n", online_df)
    # @BA use on-demand feature view to do transformation on read
    on_demand_entity_rows = online_df.rename(columns={"signal_duration": "signal_duration"}).to_dict(orient="records")

    online_df = store.get_online_features(
        features=[
            "on_demand_read_time_transformed_features:signal_duration_minutes"
        ],   entity_rows=on_demand_entity_rows   ).to_df()
    #@BA
    print("before write on_demand_read_time_transformed_features:signal_duration_minutes \n", online_df)

    online_df = store.get_online_features(
        features=["traffic_light_features_stream:signal_duration_minutes"],
        entity_rows=entity_rows
    ).to_df()
    print("pre push traffic_light_features_stream:signal_duration_minutes:\n", online_df)

    online_df = store.get_online_features(
        features=[
            "traffic_light_transformed_features:signal_duration_minutes"
        ], entity_rows=entity_rows).to_df()
    print("pre push traffic_light_transformed_features:signal_duration_minutes \n", online_df)

    # @BA use on-demand feature view to do transformation on write
    # @BA same as with the stream feature view we need to use push source or build the transformed feature here
    # as registered transfomation of feature view is not triggered by write_to_online_store
    # the store.write_to_online_store(feature_view_name="traffic_light_transformed_features", df=df)
    # like in tutorial, we use the source of the on demand feature view to push into
    # Example 4: On Demand Transformation on Write Using Pandas Mode tutorial https://docs.feast.dev/reference/beta-on-demand-feature-view
    # @BA does NOT work yet, the push() method needs push source
    # even if the feature view itself has push source
    # DOES NOT WORK: store.push("traffic_light_pushed_features", df)


    # @BA alternative: compute transformation 'inline' and then retrieve the transformed field by streamfeatureview. does this work?
    # it "works", the problem is that the transformation in the traffic_light_features_stream view is not done
    # note as alterantive usage in case ingestion with spark processor is unapplicable
    # df["signal_duration_minutes"] = df["signal_duration"] / 60
    # store.write_to_online_store(feature_view_name="traffic_light_features_stream", df=df)


    store.push("push_source",df)
    print("pushed to push_source")
    online_df = store.get_online_features(
        features=["traffic_light_features_stream:signal_duration_minutes"],
        entity_rows=entity_rows
    ).to_df()
    print("post push traffic_light_features_stream:signal_duration_minutes:\n", online_df)

    # Fetch online features for a specific entity
    entity_rows = [{"traffic_light_id": "1"}]
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

    online_df = store.get_online_features(
        features=[
            "traffic_light_transformed_features:signal_duration_minutes"
        ],   entity_rows=entity_rows   ).to_df()
    print("post push traffic_light_transformed_features:signal_duration_minutes \n", online_df)


    # @BA !doesnt trigger the feature views registered transformations !
    online_df = store.get_online_features(
        features=[
            "traffic_light_features_stream:signal_duration_minutes"

        ], entity_rows=entity_rows).to_df()
    print("post push online_df traffic_light_features_stream:signal_duration_minutes \n", online_df)


    # need different feature view as offline store doesnt know
    # about the only feature in the feature view which is signal_duration_minutes
    # training_df = store.get_historical_features(
    #     entity_df=entity_df,
    #     features=[
    #         "traffic_light_features_stream:signal_duration_minutes"
    #     ],
    # ).to_df()
    # print("training_df:\n", training_df)

    # causes error because of the path mapping from container to local fs -> fixed that error by additional volume mount in compose
    # @BA !cant write to offline store as batch source does not define feature signal_duration_minutes!
    # @BA aso does NOT trigger registered transformation in stream feature view!
    # store.write_to_offline_store(feature_view_name="traffic_light_features_stream", df=df)
    # print(f"Written to batch source:\n{df}")



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

        persist_to_feast_and_batch(message)


if __name__ == "__main__":
    consume_kafka_messages()
