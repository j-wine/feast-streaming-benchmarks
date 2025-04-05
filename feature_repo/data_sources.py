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

# Kafka source for streaming data
traffic_light_stream_source = KafkaSource(
    name="traffic_light_stream_source",
    kafka_bootstrap_servers="broker-1:9092, broker-2:9092",
    topic="traffic_light_signals",
    timestamp_field="event_timestamp",
    batch_source=traffic_light_batch_source,
    message_format=JsonFormat(
        schema_json="traffic_light_id string, primary_signal int, secondary_signal int, location string, event_timestamp timestamp"
    ),
    watermark_delay_threshold=timedelta(minutes=5),
)
