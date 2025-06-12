import datetime
import json
import threading
import time
import csv
import os
from queue import Queue
from feast import FeatureStore
from kafka import KafkaConsumer
import timing_helper
from collections import defaultdict

# ---------- BENCHMARK PARAMS ----------
STREAM_FEATURE_VIEW = os.getenv("FEATURE_VIEW_NAME")
BENCHMARK_ROWS = int(os.getenv("ROWS"))
ENTITY_PER_SECOND = int(os.getenv("ENTITY_PER_SECOND"))
PROCESSING_INTERVAL = int(os.getenv("PROCESSING_INTERVAL")) # millliseconds
PROCESSING_START=int(os.getenv("PROCESSING_START",30)) # second of the minute for producer to start sending

GROUP_SIZE =int(( ENTITY_PER_SECOND * PROCESSING_INTERVAL)/1000) # convert ms to second

# official benchmark uses params: https://github.com/feast-dev/feast-benchmarks/blob/main/python/full-report-redis.log
# Entity rows: 50; Features: 50; Concurrency: 5; RPS: 10
# Entity rows is our group size
# features has to switch out feature view
BENCHMARK_TOPIC = "benchmark_entity_topic"
KAFKA_BROKERS = ["broker-1:9092"]

RUN_ID = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
GROUP_ID  = f"feast-consumer-{RUN_ID}"
CSV_PATH = f"/app/logs/kafka_latency_log.csv"
THREAD_STATS_PATH = f"/app/logs/thread_request_stats.csv"

polling_threads = []
store = FeatureStore()
# trigger the caching of registry by making dummmy get request
store.get_online_features(features=["stream_view_10in_10out:sum"],entity_rows=[{"benchmark_entity": 1337}])
current_group = []
group_times = []
group_lock = threading.Lock()
result_queue = Queue()
group_produce_times = []
request_stats = defaultdict(int)
active_threads_count = []

def track_request(timestamp):
    second = int(timestamp)
    request_stats[second] += 1


def schedule_polling(entities, receive_times, produce_times, timeout_factor = 20):
    try:
        start_time = time.time()
        timeout = (PROCESSING_INTERVAL * timeout_factor) / 1000 # convert to seconds
        """Poll a group of entities and log each result individually into result_queue."""
        # print("schedule_polling")

        # Create dictionaries to map entity_id → times
        receive_map = dict(zip(entities, receive_times))
        produce_map = dict(zip(entities, produce_times))
        retrieved = {eid: False for eid in entities}
        retry_count = defaultdict(int)

        while not all(retrieved.values()):
            elapsed = time.time() - start_time
            if elapsed > timeout:
                not_retrieved = [eid for eid, ok in retrieved.items() if not ok]
                print(f"⏱️ Polling timed out after {elapsed:.2f}s. Missing: {not_retrieved}")
                break

            entity_rows = [{"benchmark_entity": eid} for eid in entities if not retrieved[eid]]
            pre_get_time = time.time()
            track_request(pre_get_time)
            updated = store.get_online_features(
                features=[f"{STREAM_FEATURE_VIEW}:sum"],
                entity_rows=entity_rows
            ).to_dict()
            post_get_time = time.time()
            # print(f"updated: {updated}")
            ids = updated.get("benchmark_entity", [])
            values = updated.get("sum", [])
            if not ids or not values:
                print(f"Unexpected Feast response: {updated}")
                time.sleep(1)
                continue
            for entity_id, val in zip(ids,values):
                retry_count[entity_id] += 1
                if val is not None and not retrieved[entity_id]:
                    # print(f"put entity_id in result_queue: {entity_id}")
                    retrieve_time = time.time()
                    result_queue.put({
                        "entity_id": entity_id,
                        "produce_timestamp": round(produce_map[entity_id], 6),
                        "receive_timestamp": round(receive_map[entity_id], 6),
                        "retrieval_timestamp": round(retrieve_time, 6),
                        "get_time": round(post_get_time - pre_get_time, 6),
                        "get_batch_size": len(entity_rows),
                        "retry_attempts": retry_count[entity_id]
                    })
                    retrieved[entity_id] = True
            time.sleep(0.1)

    except Exception as e:
        print(f"💥 Exception in polling thread: {e}")

def write_results_from_queue():
    """Write all queued benchmark results to CSV at once, only on STOP."""
    all_results = []

    while True:
        result = result_queue.get()
        if result == "STOP":
            break
        all_results.append(result)
        # print(f"result from queue: {result}")
    all_results.sort(key=lambda r: r["receive_timestamp"])
    # Write all to CSV at once
    with open(CSV_PATH, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=
        [
            "entity_id", "produce_timestamp","receive_timestamp", "retrieval_timestamp","get_time","get_batch_size","retry_attempts"
        ],delimiter=";")
        writer.writerows(all_results)
        print(f"Wrote {len(all_results)} entries to {CSV_PATH}")

from datetime import datetime
def consume_kafka_messages_individual_polling():
    consumer = KafkaConsumer(
        BENCHMARK_TOPIC,
        bootstrap_servers=KAFKA_BROKERS,
        auto_offset_reset='earliest',
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
        # print(f"len seen_entity_ids: {len(seen_entity_ids)}")


    if duplicate_entity_ids:
        duplicates_csv = f"/app/logs/duplicates.csv"
        with open(duplicates_csv, "w") as f:
            f.write("entity_id\n")
            for dup in sorted(duplicate_entity_ids):
                f.write(f"{dup}\n")
        # print(f" Duplicates written to {duplicates_csv}")

def poll_single_entity(entity_id, receive_time, produce_time):
    entity_row = [{"benchmark_entity": entity_id}]
    while True:
        pre_get_time = time.time()
        track_request(pre_get_time)
        updated = store.get_online_features(
            features=[f"{STREAM_FEATURE_VIEW}:sum"],
            entity_rows=entity_row
        ).to_dict()
        post_get_time = time.time()
        feature_val = updated["sum"][0]
        if feature_val is not None:
            retrieve_time = time.time()
            # print(f"sum: {feature_val} for entity {entity_id}")
            result_queue.put({
                "entity_id": entity_id,
                "produce_timestamp": round(produce_time, 6), # time of entity created by producer
                "receive_timestamp": round(receive_time, 6), # time of entity received by producer
                "retrieval_timestamp": round(retrieve_time, 6), # time of successful poll
                "get_time": round(post_get_time - pre_get_time, 6),
                "get_batch_size": len(entity_row)
            })
            break
        time.sleep(0.1)
def consume_kafka_messages_grouped():
    consumer = KafkaConsumer(
        BENCHMARK_TOPIC,
        bootstrap_servers=KAFKA_BROKERS,
        auto_offset_reset='latest',
        group_id=GROUP_ID,
        consumer_timeout_ms=1000  # Short timeout to allow periodic checks
    )

    print("🚀 Consuming Kafka messages (grouped polling)...")
    seen_entity_ids = set()
    duplicate_entity_ids = set()

    last_message_time = time.time()

    while True:
        # print(f"time.time():{time.time()}, last message time: {last_message_time}")
        try:
            message = next(consumer)
            last_message_time = time.time()
        except StopIteration:
            # print(f"last message time: {last_message_time}")
            if time.time() - last_message_time > 10:
                print("⏱️ No new messages received for over 10 seconds — stopping.")
                break
            continue  # Loop again to check timeout

        data = json.loads(message.value.decode("utf-8"))
        entity_id = data["benchmark_entity"]
        receive_time = time.time()

        try:
            dt = datetime.fromisoformat(data["event_timestamp"])
            produce_time = dt.timestamp()
        except (ValueError, TypeError) as e:
            print(f"⚠️ Invalid event_timestamp: {data.get('event_timestamp')} — {e}")
            produce_time = time.time()

        if entity_id in seen_entity_ids:
            if entity_id in duplicate_entity_ids:
                print(f"⚠️ 2nd time Duplicate entity_id encountered: {entity_id}")
                continue
            else:
                duplicate_entity_ids.add(entity_id)
                print(f"⚠️ Duplicate entity_id encountered: {entity_id}")
                continue
        seen_entity_ids.add(entity_id)
        # print(f"added seen entity_id: {entity_id}")
        acquired = group_lock.acquire(timeout=1)
        if not acquired:
            print("⚠️ Could not acquire group_lock — skipping this message.")
            continue
        try:
            current_group.append(entity_id)
            group_times.append(receive_time)
            group_produce_times.append(produce_time)

            if len(current_group) >= GROUP_SIZE:
                entities = current_group.copy()
                receive_times = group_times.copy()
                produce_times = group_produce_times.copy()

                current_group.clear()
                group_times.clear()
                group_produce_times.clear()

                t = threading.Thread(
                    target=schedule_polling,
                    args=(entities, receive_times, produce_times),
                    daemon=True
                )
                polling_threads.append(t)
                t.start()
        finally:
            group_lock.release()

        print(f"len(seen_entity_ids): {len(seen_entity_ids)}")
        if len(seen_entity_ids) >= BENCHMARK_ROWS:
            print(f"Reached {BENCHMARK_ROWS} unique entity IDs")
            break

    if duplicate_entity_ids:
        duplicates_csv = f"/app/logs/duplicates_{RUN_ID}.csv"
        with open(duplicates_csv, "w") as f:
            f.write("entity_id\n")
            for dup in sorted(duplicate_entity_ids):
                f.write(f"{dup}\n")
        print(f"Duplicates written to {duplicates_csv}")
    else:
        print("✅ No duplicate entity_ids encountered.")

if __name__ == "__main__":
    # Create CSV file if missing
    if not os.path.exists(CSV_PATH):
        with open(CSV_PATH, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "entity_id", "produce_timestamp","receive_timestamp", "retrieval_timestamp","get_time","get_batch_size", "retry_attempts"
            ],delimiter=";")
            writer.writeheader()

    # Start logging thread
    logger_thread = threading.Thread(target=write_results_from_queue)
    logger_thread.start()
    print(f"starting to wait at {time.time()}")
    timing_helper.wait_until_second(PROCESSING_START-5)
    try:
        consume_kafka_messages_grouped()
    except KeyboardInterrupt:
        print("Stopping...")

    for t in polling_threads:
        t.join()

    # Clean shutdown
    result_queue.put("STOP")
    logger_thread.join()
    print("🛑 Logging thread completed.")
    # Save per-second request statistics
    with open(THREAD_STATS_PATH, "w") as f:
        f.write("second;requests\n")
        for second, count in sorted(request_stats.items()):
            f.write(f"{second};{count}\n")
    print(f"thread stats written to {THREAD_STATS_PATH}")
