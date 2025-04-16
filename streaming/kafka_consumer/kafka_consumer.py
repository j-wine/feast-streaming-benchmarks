import datetime
import json
import threading
import time
import csv
import os
from queue import Queue
from feast import FeatureStore
from kafka import KafkaConsumer
BENCHMARK_ROWS = 10_000
ENTITY_PER_SECOND = 100
PROCESSING_INTERVAL = 1  # seconds

BENCHMARK_TOPIC = "benchmark_entity_topic"
KAFKA_BROKERS = ["broker-1:9092"]

RUN_ID = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
GROUP_ID  = f"feast-consumer-{RUN_ID}"
CSV_PATH = f"/app/logs/kafka_latency_log.csv"

GROUP_SIZE = ENTITY_PER_SECOND * PROCESSING_INTERVAL
polling_threads = []
store = FeatureStore()
current_group = []
group_times = []
group_lock = threading.Lock()
result_queue = Queue()

# def schedule_polling(entities, receive_times):
#     """Polling thread: polls until all entities have data, puts results into queue."""
#     entity_rows = [{"benchmark_entity": eid} for eid in entities]
#     retrieved = {eid: False for eid in entities}
#
#     while not all(retrieved.values()):
#         updated = store.get_online_features(
#             features=["feature_sum:sum"],
#             entity_rows=entity_rows
#         ).to_dict()
#
#         ids = updated["benchmark_entity"]
#         values = updated["sum"]
#
#         for i, entity_id in enumerate(ids):
#             if values[i] is not None and not retrieved[entity_id]:
#                 retrieve_time = time.time()
#                 result_queue.put({
#                     "entity_id": entity_id,
#                     "receive_timestamp": round(receive_times[i], 6),
#                     "retrieval_timestamp": round(retrieve_time, 6),
#
#                 })
#                 retrieved[entity_id] = True
#
#         time.sleep(0.1)


def write_results_from_queue():
    """Write all queued benchmark results to CSV at once, only on STOP."""
    all_results = []

    while True:
        result = result_queue.get()
        if result == "STOP":
            break
        all_results.append(result)
        print(f"result from queue: {result}")
    all_results.sort(key=lambda r: r["receive_timestamp"])
    # Write all to CSV at once
    with open(CSV_PATH, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=
        [
            "entity_id", "produce_timestamp","receive_timestamp", "retrieval_timestamp"
        ],delimiter=";")
        writer.writerows(all_results)
        print(f"📝 Wrote {len(all_results)} entries to {CSV_PATH}")

from datetime import datetime
def consume_kafka_messages_individual_polling():
    consumer = KafkaConsumer(
        BENCHMARK_TOPIC,
        bootstrap_servers=KAFKA_BROKERS,
        auto_offset_reset='latest',
        group_id=GROUP_ID
    )
    for partition in consumer.assignment():
        position = consumer.position(partition)
        print(f"Consumer position for {partition}: {position}")
    print("🚀 Consuming Kafka messages (individual polling)...")
    seen_entity_ids = set()
    duplicate_entity_ids = set()

    for message in consumer:
        data = json.loads(message.value.decode("utf-8"))
        entity_id = data["benchmark_entity"]
        receive_time = time.time()
        try:
            dt = datetime.fromisoformat(data["event_timestamp"])
            produce_time = dt.timestamp()
        except (ValueError, TypeError) as e:
            print(f"⚠️ Invalid event_timestamp: {data.get('event_timestamp')} — {e}")
            produce_time = time.time()  # fallback

        if entity_id in seen_entity_ids:
            if entity_id in duplicate_entity_ids:
                print(f"⚠️ 2nd time Duplicate entity_id encountered: {entity_id}")
                continue
            else:
                duplicate_entity_ids.add(entity_id)
                print(f"⚠️ Duplicate entity_id encountered: {entity_id}")
                continue
        seen_entity_ids.add(entity_id)
        print(f"produce_time b4 poll {produce_time}: {entity_id}")
        # Create and start a polling thread per entity
        t = threading.Thread(
            target=poll_single_entity,
            args=(entity_id, receive_time,produce_time),
            daemon=True
        )
        polling_threads.append(t)
        t.start()

        if len(seen_entity_ids) >= BENCHMARK_ROWS:
            print(f"🛑 Reached {BENCHMARK_ROWS} unique entity IDs")
            break

    if duplicate_entity_ids:
        duplicates_csv = f"/app/logs/duplicates_{RUN_ID}.csv"
        with open(duplicates_csv, "w") as f:
            f.write("entity_id\n")
            for dup in sorted(duplicate_entity_ids):
                f.write(f"{dup}\n")
        print(f"📁 Duplicates written to {duplicates_csv}")

def poll_single_entity(entity_id, receive_time, produce_time):
    entity_row = [{"benchmark_entity": entity_id}]
    while True:
        updated = store.get_online_features(
            features=["feature_sum:sum"],
            entity_rows=entity_row
        ).to_dict()
        feature_val = updated["sum"][0]
        if feature_val is not None:
            retrieve_time = time.time()

            result_queue.put({
                "entity_id": entity_id,
                "produce_timestamp": round(produce_time, 6), # time of entity created by producer
                "receive_timestamp": round(receive_time, 6), # time of entity received by producer
                "retrieval_timestamp": round(retrieve_time, 6), # time of successful poll
            })
            break
        time.sleep(0.1)
# def consume_kafka_messages_grouped():
#     consumer = KafkaConsumer(
#         BENCHMARK_TOPIC,
#         bootstrap_servers=KAFKA_BROKERS,
#         auto_offset_reset='latest',
#         group_id=GROUP_ID
#     )
#
#     print("🚀 Consuming Kafka messages...")
#     seen_entity_ids = set()
#     duplicate_entity_ids = set()
#     for message in consumer:
#         receive_time = time.time()
#         data = json.loads(message.value.decode("utf-8"))
#         entity_id = data["benchmark_entity"]
#
#         if len(seen_entity_ids) >= BENCHMARK_ROWS:
#             print(f"🛑 Reached {BENCHMARK_ROWS} unique entity IDs")
#             break
#         if entity_id in seen_entity_ids:
#             duplicate_entity_ids.add(entity_id)
#             print(f"⚠️ Duplicate entity_id encountered: {entity_id}")
#             continue
#         seen_entity_ids.add(entity_id)
#
#         with group_lock:
#             current_group.append(entity_id)
#             group_times.append(receive_time)
#
#             if len(current_group) >= GROUP_SIZE:
#                 entities = current_group.copy()
#                 times = group_times.copy()
#                 current_group.clear()
#                 group_times.clear()
#
#                 threading.Thread(
#                     target=schedule_polling,
#                     args=(entities, times),
#                     daemon=True
#                 ).start()
#
#     if duplicate_entity_ids:
#         duplicates_csv = f"/app/logs/duplicates_{RUN_ID}.csv"
#         with open(duplicates_csv, "w") as f:
#             f.write("entity_id\n")
#             for dup in sorted(duplicate_entity_ids):
#                 f.write(f"{dup}\n")
#         print(f"📁 Duplicates written to {duplicates_csv}")
#     else:
#         print("✅ No duplicate entity_ids encountered.")

if __name__ == "__main__":
    # Create CSV file if missing
    if not os.path.exists(CSV_PATH):
        with open(CSV_PATH, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "entity_id", "produce_timestamp","receive_timestamp", "retrieval_timestamp",
            ],delimiter=";")
            writer.writeheader()

    # Start logging thread
    logger_thread = threading.Thread(target=write_results_from_queue)
    logger_thread.start()

    try:
        consume_kafka_messages_individual_polling()
    except KeyboardInterrupt:
        print("Stopping...")

    for t in polling_threads:
        t.join()

    # Clean shutdown
    result_queue.put("STOP")
    logger_thread.join()
    print("🛑 Logging completed.")
