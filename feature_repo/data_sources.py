from datetime import timedelta
from feast import KafkaSource, FileSource, PushSource, RequestSource, Field
from feast.data_format import JsonFormat
from feast.types import String, Int64, Float64

# Batch source for historical feature retrieval
traffic_light_batch_source = FileSource(
    name="traffic_light_batch_source",
    path="offline_data/traffic_light_data.parquet",
    timestamp_field="event_timestamp",
)

push_source = PushSource(
    name="push_source",
    batch_source=traffic_light_batch_source
)

# Define a request data source for request-time features
# @BA Example 1: On Demand Transformation on Read Using Pandas Mode
# https://docs.feast.dev/reference/beta-on-demand-feature-view#example-1-on-demand-transformation-on-read-using-pandas-mode
traffic_lights_request_source = RequestSource(
    name="traffic lights",
    schema=[
        Field(name="traffic_light_id", dtype=String),
        Field(name="primary_signal", dtype=Int64),
        Field(name="secondary_signal", dtype=Int64),
        Field(name="location", dtype=String),
        Field(name="signal_duration", dtype=Float64),
    ]
)


# Kafka source for streaming data
traffic_light_stream_source = KafkaSource(
    name="traffic_light_stream_source",
    kafka_bootstrap_servers="broker:9092",
    topic="traffic_light_signals",
    timestamp_field="event_timestamp",
    batch_source=traffic_light_batch_source,
    message_format=JsonFormat(
        schema_json="traffic_light_id string, primary_signal int, secondary_signal int, location string, signal_duration double, event_timestamp timestamp"
    ),
    watermark_delay_threshold=timedelta(minutes=5),
)
