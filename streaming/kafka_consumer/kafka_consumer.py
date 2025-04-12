import json
import logging
import threading
import time
import warnings

from feast import FeatureStore
from kafka import KafkaConsumer

from timing_helper import wait_until

ENTITY_PER_SECOND = 10 # entities per second produced by kafka producer
PROCESSING_INTERVAL=5 # processing time of spark ingestor
PROCESSING_START=30 # start at second 30
warnings.simplefilter("ignore")


logger = logging.getLogger('kafka_consumer')
BENCHMARK_TOPIC = "benchmark_entity_topic"
KAFKA_BROKERS = ["broker-1:9092"]

# Initialize the Feast feature store
store = FeatureStore()

def process_benchmark_message(message, message_retrieval_time):
    data = json.loads(message.value.decode("utf-8"))
    # entity_id = benchmark_counter()  # Increment and get the current ID
    entity_id = data['benchmark_entity']  # Directly use the entity ID from the Kafka message


    print(f"Processing benchmark data for entity ID: {entity_id}")
    online_features = store.get_online_features(
        features=[
            "feature_sum:sum",
        ],
        entity_rows=[{"benchmark_entity": entity_id}],

    ).to_dict()
    print("🔍 Online features result:", online_features)

    if online_features['sum'][0] is None:
        def poll_features():
            wait_until(PROCESSING_START+PROCESSING_INTERVAL)

            while True:
                updated_features = store.get_online_features(
                    features=["feature_sum:sum"],
                    entity_rows=[{"benchmark_entity": entity_id}],
                    full_feature_names=True
                ).to_dict()
                if updated_features['feature_sum__sum'][0] is not None:
                    end_time = time.time()  # Stop timing when value is retrieved
                    # print(f"Updated online_features for entity ID {entity_id}: {updated_features}")
                    print(f"[luck]benchmark data retrieval took {end_time - message_retrieval_time:.10f} seconds for entity ID{entity_id} at end_time:{end_time}")
                    break
                time.sleep(0.1)

        # Start a thread to poll the features
        polling_thread = threading.Thread(target=poll_features)
        polling_thread.start()
    else:
        # print(f"Initial online_features: {online_features}")
        end_time = time.time()
        print(f"Benchmark data retrieval took {end_time - message_retrieval_time:.10f} seconds for entity ID {entity_id} at end_time:{end_time}")

# Note: `benchmark_counter` and `store.get_online_features` must be defined in your actual code.

def consume_kafka_messages():
    """
    Consumes messages from Kafka and processes them based on the topic.
    """
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BROKERS,
        auto_offset_reset='earliest',
        group_id="feast-persist-consumer"
    )
    consumer.subscribe([BENCHMARK_TOPIC])
    first_cycle=True
    for message in consumer:
        if(first_cycle):
            wait_until(PROCESSING_START + PROCESSING_INTERVAL)
            first_cycle=False
        message_retrieval_time = time.time()
        process_benchmark_message(message, message_retrieval_time)


if __name__ == "__main__":
    consume_kafka_messages()
