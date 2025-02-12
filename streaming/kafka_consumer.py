import json
import logging
from dateutil import parser

import pandas as pd
from feast import FeatureStore
from kafka import KafkaConsumer

logger = logging.getLogger('kafka_consumer')
KAFKA_TOPIC = "traffic_light_signals"
KAFKA_BROKER = "broker:9092"

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
    traffic_light_id = data["traffic_light_id"]

    # entity id's to fetch
    entity_rows = [
        {"traffic_light_id": "320"}, {"traffic_light_id": "321"}, {"traffic_light_id": "333"},
                   {"traffic_light_id": "370"}, {"traffic_light_id": traffic_light_id}
    ]
    # Use Feast's event timestamp for processing

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
    print("store.write_to_online_store(feature_view_name=""traffic_light_stats"", df=df)")

    online_df = store.get_online_features(
        features=[
            "traffic_light_stats:primary_signal",
            "traffic_light_stats:secondary_signal",
            "traffic_light_stats:location",
            "traffic_light_stats:signal_duration",
        ],   entity_rows=entity_rows   ).to_df()
    print("traffic_light_stats after write_to_online_store,before push:\n", online_df)
    # @BA use on-demand feature view to do transformation on read
    online_df = store.get_online_features(
        features=[
            "on_demand_read_time_transformed_features:signal_duration_minutes"
        ],   entity_rows=entity_rows   ).to_df()
    #@BA
    print("on demand on read transf:before write on_demand_read_time_transformed_features:signal_duration_minutes \n", online_df)

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




    # @BA alternative: compute transformation here. does this work???
    # it seems to work, the problem is that the transformation in the traffic_light_features_stream view is not done
    # @BA use ingestion processor instead
    # df["signal_duration_minutes"] = df["signal_duration"] / 60
    # store.write_to_online_store(feature_view_name="traffic_light_features_stream", df=df)

    # @BA push without transformation and then retrieve the transformed feature using the stream feature view
    # @BA does the feature views transf get triggered?
    store.push("push_source",df)
    print("pushed to push_source")
    online_df = store.get_online_features(
        features=["traffic_light_features_stream:signal_duration_minutes"],
        entity_rows=entity_rows
    ).to_df()
    print("post push traffic_light_features_stream:signal_duration_minutes:\n", online_df)

    online_df = store.get_online_features(
        features=[
            "traffic_light_transformed_features:signal_duration_minutes"
        ],   entity_rows=entity_rows   ).to_df()
    print("post push traffic_light_transformed_features:signal_duration_minutes \n", online_df)


    # @BA !doesnt trigger the feature views registered transformations !
    # only store is flexible, so the new feature signal_duration_minutes can be set!

    print(f"Persisted data to Feast:\n{df}")



    # Persist the DataFrame to the Feast online store
    # For debugging Shows all registered feature views
    logger.log(level=20, msg=store.list_feature_views())
    # @BA !doesnt trigger the feature views registered transformations !
    # feature_view_name uses the name of the feature view as string
    # for decorator tag @stream_feature_view the name is the method name



    online_df = store.get_online_features(
        features=[
            "traffic_light_features_stream:signal_duration_minutes"

        ], entity_rows=entity_rows).to_df()
    print("online_df traffic_light_features_stream:signal_duration_minutes \n", online_df)


    # need different feature view as offline store doesnt know
    # about the only feature in the feature viewmwhich is signal_duration_minutes
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
        bootstrap_servers=KAFKA_BROKER,
        group_id="feast-persist-consumer",
    )
    print("Consuming messages from Kafka...")
    for message in consumer:

        persist_to_feast_and_batch(message)


if __name__ == "__main__":
    consume_kafka_messages()
