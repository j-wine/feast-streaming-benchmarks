import json
import threading
import time
import csv
import os
from queue import Queue
from feast import FeatureStore
from kafka import KafkaConsumer
BENCHMARK_ROWS = 10_000
# Configuration
ENTITY_PER_SECOND = 20
PROCESSING_INTERVAL = 1  # seconds
GROUP_SIZE = ENTITY_PER_SECOND * PROCESSING_INTERVAL
BENCHMARK_TOPIC = "benchmark_entity_topic"
KAFKA_BROKERS = ["broker-1:9092"]
CSV_PATH = "kafka_latency_log.csv"

store = FeatureStore()
current_group = []
group_times = []
group_lock = threading.Lock()
result_queue = Queue()


def schedule_polling(entities, receive_times):
    """Polling thread: polls until all entities have data, puts results into queue."""
    entity_rows = [{"benchmark_entity": eid} for eid in entities]
    retrieved = {eid: False for eid in entities}

    while not all(retrieved.values()):
        updated = store.get_online_features(
            features=["feature_sum:sum"],
            entity_rows=entity_rows
        ).to_dict()

        ids = updated["benchmark_entity"]
        values = updated["sum"]

        for i, entity_id in enumerate(ids):
            if values[i] is not None and not retrieved[entity_id]:
                retrieve_time = time.time()
                latency = retrieve_time - receive_times[i]
                result_queue.put({
                    "entity_id": entity_id,
                    "receive_timestamp": round(receive_times[i], 6),
                    "retrieval_timestamp": round(retrieve_time, 6),
                    "consumer_latency": round(latency, 4)
                })
                retrieved[entity_id] = True

        time.sleep(0.1)


def write_results_from_queue():
    """Write all queued benchmark results to CSV at once, only on STOP."""
    all_results = []

    while True:
        result = result_queue.get()
        if result == "STOP":
            break
        all_results.append(result)
    all_results.sort(key=lambda r: r["receive_timestamp"])
    # Write all to CSV at once
    with open(CSV_PATH, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "entity_id", "receive_timestamp", "retrieval_timestamp", "consumer_latency"
        ])
        writer.writerows(all_results)
        print(f"📝 Wrote {len(all_results)} entries to {CSV_PATH}")




def consume_kafka_messages():
    consumer = KafkaConsumer(
        BENCHMARK_TOPIC,
        bootstrap_servers=KAFKA_BROKERS,
        auto_offset_reset='earliest',
        group_id="feast-persist-consumer"
    )

    print("🚀 Consuming Kafka messages...")
    for message in consumer:
        receive_time = time.time()
        data = json.loads(message.value.decode("utf-8"))
        entity_id = data["benchmark_entity"]
        with group_lock:
            current_group.append(entity_id)
            group_times.append(receive_time)

            if len(current_group) >= GROUP_SIZE:
                entities = current_group.copy()
                times = group_times.copy()
                current_group.clear()
                group_times.clear()

                threading.Thread(
                    target=schedule_polling,
                    args=(entities, times),
                    daemon=True
                ).start()
        if entity_id == BENCHMARK_ROWS:
            print(f"🛑 Reached last expected entity: {entity_id}")
            break


if __name__ == "__main__":
    # Create CSV file if missing
    if not os.path.exists(CSV_PATH):
        with open(CSV_PATH, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "entity_id", "receive_timestamp", "retrieval_timestamp", "consumer_latency"
            ])
            writer.writeheader()

    # Start logging thread
    logger_thread = threading.Thread(target=write_results_from_queue)
    logger_thread.start()

    try:
        consume_kafka_messages()
    except KeyboardInterrupt:
        print("Stopping...")

    # Clean shutdown
    result_queue.put("STOP")
    logger_thread.join()
    print("🛑 Logging completed.")
