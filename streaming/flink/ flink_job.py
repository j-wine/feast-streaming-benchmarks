from pyflink.common import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaProducer, FlinkKafkaConsumer
import json
from datetime import datetime

# Define Flink environment
env = StreamExecutionEnvironment.get_execution_environment()

# Kafka consumer
kafka_consumer = FlinkKafkaConsumer(
    topics="traffic_light_signals",
    deserialization_schema=SimpleStringSchema(),
    properties={"bootstrap.servers": "broker:9092", "group.id": "traffic-light-group"},
)

# Kafka producer for processed features
kafka_producer = FlinkKafkaProducer(
    topic="processed_traffic_light_signals",
    serialization_schema=SimpleStringSchema(),
    producer_config={"bootstrap.servers": "broker:9092"},
)

# Cache to hold the last signal timestamp per traffic light
last_signal_timestamps = {}


def transform_signal(data):
    """
    Transforms incoming Kafka messages with additional features like `signal_duration`.
    """
    data = json.loads(data)
    traffic_light_id = data["traffic_light_id"]
    current_timestamp = datetime.fromisoformat(data["timestamp"]).timestamp()

    # Calculate signal duration
    last_timestamp = last_signal_timestamps.get(traffic_light_id, current_timestamp)
    signal_duration = current_timestamp - last_timestamp
    last_signal_timestamps[traffic_light_id] = current_timestamp

    # Add computed feature
    data["signal_duration"] = signal_duration
    return json.dumps(data)


# Transform Kafka stream
kafka_stream = env.add_source(kafka_consumer).map(transform_signal)
kafka_stream.add_sink(kafka_producer)

env.execute("Traffic Light Flink Job")
