import os
import pandas as pd
from feast import FeatureStore
from feast.data_source import PushMode
from feast.infra.contrib.spark_kafka_processor import SparkProcessorConfig
from feast.infra.contrib.stream_processor import get_stream_processor_object
from pyspark.sql import SparkSession

# Use environment variables set by Docker
JAVA_HOME = os.getenv("JAVA_HOME")
SPARK_HOME = os.getenv("SPARK_HOME")


STREAM_FEATURE_VIEW = os.getenv("FEATURE_VIEW_NAME") # sfv to ingest
PROCESSING_INTERVAL = int(os.getenv("PROCESSING_INTERVAL")) # int in seconds SparkProcessorConfig.processing_time

# @BA document the producer topic initiation requirement
# second of the minute for producer to start sending. is needed no longer  when we send one meaningless  message to init topic
# PROCESSING_START=int(os.getenv("PROCESSING_START",30))

# Ensure PySpark is properly configured with Kafka support
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-shell"

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaTrafficLightProcessor") \
    .config("spark.sql.shuffle.partitions", 5) \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()

# Verify Spark setup
print(f"Using Spark Version: {spark.version}")

# Initialize Feature Store
store = FeatureStore()

def preprocess_fn(rows: pd.DataFrame):
    import time
    entity_ids = rows['benchmark_entity'].tolist()
    feast_ingestion_time = time.time()
    print(f"Spark -> Feast ingestion timestamp: {feast_ingestion_time:.6f} for entity ids: {entity_ids}")
    return rows

# Configure Spark ingestion job
ingestion_config = SparkProcessorConfig(
    mode="spark",
    source="kafka",
    spark_session=spark,
    processing_time=f"{PROCESSING_INTERVAL} seconds",
    query_timeout=15
)

# Fetch stream feature view
traffic_light_windowed_features = store.get_stream_feature_view(STREAM_FEATURE_VIEW)

# Initialize stream processor
processor = get_stream_processor_object(
    config=ingestion_config,
    fs=store,
    sfv=traffic_light_windowed_features,
    preprocess_fn=preprocess_fn
)
query = processor.ingest_stream_feature_view(to=PushMode.ONLINE)

query.awaitTermination()