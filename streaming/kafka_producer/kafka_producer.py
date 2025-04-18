import json
import time
import datetime
from pathlib import Path
import pandas as pd
from kafka import KafkaProducer

import timing_helper

# --- Parameters ---
BENCHMARK_ROWS = 10_000 # Number of datapoints send
ENTITY_PER_SECOND = 500
BENCHMARK_FEATURES = 100  # Number of features to include per entity

BENCHMARK_TOPIC = "benchmark_entity_topic"
KAFKA_BROKERS = ["broker-1:9092"]
PROCESSING_START = 30 # second of the minute to start sending

def read_benchmark_data():
    parquet_file = Path(__file__).parent / "offline_data/generated_data.parquet"
    df = pd.read_parquet(parquet_file)
    df = df.sort_values("benchmark_entity")

    feature_cols = [f"feature_{i}" for i in range(BENCHMARK_FEATURES)]
    selected_cols = ["benchmark_entity", "event_timestamp"] + feature_cols
    df = df[selected_cols].copy()
    return df.head(BENCHMARK_ROWS)

def produce_kafka_messages():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all')
    benchmark_df = read_benchmark_data()
    feature_columns = [f"feature_{i}" for i in range(BENCHMARK_FEATURES)]

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
