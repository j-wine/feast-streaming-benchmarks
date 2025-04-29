from datetime import timedelta

from feast import KafkaSource, FileSource, PushSource
from feast.data_format import JsonFormat

BENCHMARK_TOPIC = "benchmark_entity_topic"
def generate_schema_json(num_features: int) -> str:
    base_fields = [
        "benchmark_entity int",
        "event_timestamp timestamp"
    ]
    feature_fields = [f"feature_{i} int" for i in range(num_features)]
    return ", ".join(base_fields + feature_fields)


benchmark_batch_source = FileSource(
    name="benchmark_batch_source",
    path="offline_data/generated_data.parquet",
    timestamp_field="event_timestamp",
)
benchmark_push_source = PushSource(
    name="benchmark_push_source",
    batch_source=benchmark_batch_source
)
benchmark_stream_source = KafkaSource(
    name="benchmark_stream_source",
    kafka_bootstrap_servers="broker-1:9092",
    topic=BENCHMARK_TOPIC,
    timestamp_field="event_timestamp",
    batch_source=benchmark_batch_source,
    message_format=JsonFormat(
        schema_json="benchmark_entity int, event_timestamp timestamp, feature_0 int, feature_1 int, feature_2 int, feature_3 int, feature_4 int, feature_5 int, feature_6 int, feature_7 int, feature_8 int, feature_9 int"
    ),
    watermark_delay_threshold=timedelta(minutes=5),
)

hundred_features_benchmark_stream_source = KafkaSource(
    name="hundred_features_benchmark_stream_source",
    kafka_bootstrap_servers="broker-1:9092",
    topic=BENCHMARK_TOPIC,
    timestamp_field="event_timestamp",
    batch_source=benchmark_batch_source,
    message_format=JsonFormat(schema_json=generate_schema_json(100)),
    watermark_delay_threshold=timedelta(minutes=5),
)
