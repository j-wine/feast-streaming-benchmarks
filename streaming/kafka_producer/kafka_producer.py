import json
import time
import datetime
from pathlib import Path
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import UnknownTopicOrPartitionError, KafkaError
import os
import timing_helper

# --- Benchmark Parameters ---
BENCHMARK_FEATURES_IN = int(os.getenv("FEATURES"))
BENCHMARK_ROWS = int(os.getenv("ROWS"))
ENTITY_PER_SECOND = int(os.getenv("ENTITY_PER_SECOND"))
# second of the minute for producer to start sending.
PROCESSING_START=int(os.getenv("PROCESSING_START",30))

BENCHMARK_TOPIC = "benchmark_entity_topic"
KAFKA_BROKERS = ["broker-1:9092"]

def wait_for_kafka_topic(producer, topic, retries=150, delay=0.1):
    dummy_message = {"check": "ping"}
    for i in range(retries):
        try:
            print(f"⏳ Checking if topic '{topic}' is ready... Attempt {i+1}/{retries}")
            future = producer.send(topic, value=dummy_message)
            future.get(timeout=5)
            print(f"✅ Kafka topic '{topic}' is ready.")
            return
        except UnknownTopicOrPartitionError:
            print("❌ Topic not found yet. Waiting...")
            time.sleep(delay)
        except KafkaError as e:
            print(f" Kafka error: {e}. Retrying...")
            time.sleep(delay)
    raise RuntimeError(f"❌ Kafka topic '{topic}' not available after {retries} attempts.")


def read_benchmark_data():
    parquet_file = Path(__file__).parent / "offline_data/generated_data.parquet"
    df = pd.read_parquet(parquet_file)
    df = df.sort_values("benchmark_entity")

    feature_cols = [f"feature_{i}" for i in range(BENCHMARK_FEATURES_IN)]
    selected_cols = ["benchmark_entity", "event_timestamp"] + feature_cols
    df = df[selected_cols].copy()
    return df.head(BENCHMARK_ROWS)

def produce_kafka_messages():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all')
    wait_for_kafka_topic(producer, BENCHMARK_TOPIC)

    benchmark_df = read_benchmark_data()
    feature_columns = [f"feature_{i}" for i in range(BENCHMARK_FEATURES_IN)]

    print("Writing benchmark data to Kafka...")
    timing_helper.wait_until_second(PROCESSING_START)

    for row in benchmark_df.itertuples(index=False):
        benchmark_entity = {
            "benchmark_entity": getattr(row, "benchmark_entity"),
            "event_timestamp": datetime.datetime.utcnow().isoformat()
        }

        # Include only the configured number of features
        for feature in feature_columns:
            benchmark_entity[feature] = getattr(row, feature)

        producer.send(BENCHMARK_TOPIC, benchmark_entity)
        print(f"Sent Benchmark Data: {benchmark_entity}")

        time.sleep(1 / ENTITY_PER_SECOND)

    print("All benchmark entities have been sent.")

if __name__ == "__main__":
    produce_kafka_messages()
