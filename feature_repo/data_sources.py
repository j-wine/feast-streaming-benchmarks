from datetime import timedelta
from feast import KafkaSource, FileSource, PushSource, RequestSource, Field
from feast.data_format import JsonFormat
from feast.types import String, Int64, Float64

TRAFFIC_LIGHT_TOPIC="traffic_light_signals"
BENCHMARK_TOPIC = "benchmark_signals"

# Batch source for historical feature retrieval
traffic_light_batch_source = FileSource(
    name="traffic_light_batch_source",
    path="offline_data/generated_data.parquet",
    timestamp_field="event_timestamp",
)

# Kafka source for streaming data
traffic_light_stream_source = KafkaSource(
    name="traffic_light_stream_source",
    kafka_bootstrap_servers="broker-1:9092, broker-2:9092",
    topic=TRAFFIC_LIGHT_TOPIC,
    timestamp_field="event_timestamp",
    batch_source=traffic_light_batch_source,
    message_format=JsonFormat(
        schema_json="traffic_light_id string, primary_signal int, secondary_signal int, location string, event_timestamp timestamp"
    ),
    watermark_delay_threshold=timedelta(minutes=5),
)

# Batch source for historical feature retrieval
benchmark_batch_source = FileSource(
    name="benchmark_batch_source",
    path="offline_data/benchmark_data.parquet",
    timestamp_field="event_timestamp",
)

benchmark_stream_source = KafkaSource(
    name="benchmark_stream_source",
    kafka_bootstrap_servers="broker-1:9092, broker-2:9092",
    topic=BENCHMARK_TOPIC,
    timestamp_field="event_timestamp",
    batch_source=benchmark_batch_source,
    message_format=JsonFormat(
        schema_json="benchmark_entity int, event_timestamp timestamp, feature_0 int, feature_1 int, feature_2 int, feature_3 int, feature_4 int, feature_5 int, feature_6 int, feature_7 int, feature_8 int, feature_9 int"
    ),
    watermark_delay_threshold=timedelta(minutes=5),
)