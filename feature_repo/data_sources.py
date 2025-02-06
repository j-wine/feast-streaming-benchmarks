from datetime import timedelta
from feast import KafkaSource, FileSource
from feast.data_format import JsonFormat

# Batch source for historical feature retrieval
traffic_light_batch_source = FileSource(
    name="traffic_light_batch_source",
    path="offline_data/traffic_light_data.parquet",
    timestamp_field="event_timestamp",
)

# Kafka source for streaming data
traffic_light_stream_source = KafkaSource(
    name="traffic_light_stream_source",
    kafka_bootstrap_servers="broker:9092",
    topic="processed_traffic_light_signals",
    timestamp_field="event_timestamp",
    batch_source=traffic_light_batch_source,
    message_format=JsonFormat(
        schema_json="""
        {
            "type": "record",
            "name": "TrafficLightSignal",
            "fields": [
                {"name": "traffic_light_id", "type": "int"},
                {"name": "primary_signal", "type": "int"},
                {"name": "secondary_signal", "type": "int"},
                {"name": "location", "type": "string"},
                {"name": "signal_duration", "type": "double"},
                {"name": "event_timestamp", "type": "string"}
            ]
        }
        """
    ),
    watermark_delay_threshold=timedelta(minutes=5),
)
