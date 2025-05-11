from datetime import timedelta

from feast import KafkaSource, FileSource
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

benchmark_stream_source_10_features = KafkaSource(
    name="benchmark_stream_source_10_features",
    kafka_bootstrap_servers="broker-1:9092",
    topic=BENCHMARK_TOPIC,
    timestamp_field="event_timestamp",
    batch_source=benchmark_batch_source,
    message_format=JsonFormat(schema_json=generate_schema_json(10)),
    watermark_delay_threshold=timedelta(minutes=5),
)
benchmark_stream_source_50_features = KafkaSource(
    name="benchmark_stream_source_50_features",
    kafka_bootstrap_servers="broker-1:9092",
    topic=BENCHMARK_TOPIC,
    timestamp_field="event_timestamp",
    batch_source=benchmark_batch_source,
    message_format=JsonFormat(schema_json=generate_schema_json(50)),
    watermark_delay_threshold=timedelta(minutes=5),
)

benchmark_stream_source_100_features = KafkaSource(
    name="benchmark_stream_source_100_features",
    kafka_bootstrap_servers="broker-1:9092",
    topic=BENCHMARK_TOPIC,
    timestamp_field="event_timestamp",
    batch_source=benchmark_batch_source,
    message_format=JsonFormat(schema_json=generate_schema_json(100)),
    watermark_delay_threshold=timedelta(minutes=5),
)

benchmark_stream_source_250_features = KafkaSource(
    name="benchmark_stream_source_250_features",
    kafka_bootstrap_servers="broker-1:9092",
    topic=BENCHMARK_TOPIC,
    timestamp_field="event_timestamp",
    batch_source=benchmark_batch_source,
    message_format=JsonFormat(schema_json=generate_schema_json(250)),
    watermark_delay_threshold=timedelta(minutes=5),
)