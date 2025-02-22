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

# Ensure PySpark is properly configured with Kafka support
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 pyspark-shell"

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaTrafficLightProcessor") \
    .config("spark.sql.shuffle.partitions", 5) \
    .getOrCreate()

# Verify Spark setup
print(f"Using Spark Version: {spark.version}")

# Initialize Feature Store
store = FeatureStore()

def preprocess_fn(rows: pd.DataFrame):
    """Preprocess function to log Spark DataFrame details."""
    print(f"df columns: {rows.columns}")
    print(f"df size: {rows.shape[0]}")
    print(f"df preview:\n{rows.head()}")
    return rows

# Configure Spark ingestion job
ingestion_config = SparkProcessorConfig(
    mode="spark",
    source="kafka",
    spark_session=spark,
    processing_time="30 seconds",
    query_timeout=15
)

# Fetch stream feature view
traffic_light_windowed_features = store.get_stream_feature_view("traffic_light_windowed_features")

# Initialize stream processor
processor = get_stream_processor_object(
    config=ingestion_config,
    fs=store,
    sfv=traffic_light_windowed_features,
    preprocess_fn=preprocess_fn
)

# Start ingestion job (process stream data every 30 seconds)
query = processor.ingest_stream_feature_view(to=PushMode.ONLINE)

# Wait for stream termination (optional)
processor.await_termination()

# Stop query safely when done
query.stop()

# Fetch online features for a specific entity
entity_rows = [{"traffic_light_id": "320"}]
online_features = store.get_online_features(
    features=[
        "traffic_light_windowed_features:avg_signal_duration_minutes",
        "traffic_light_windowed_features:primary_signal_count",
        "traffic_light_windowed_features:secondary_signal_count",
        "traffic_light_windowed_features:total_windowed_primary_signal_duration",
        "traffic_light_windowed_features:total_windowed_secondary_signal_duration",
    ],
    entity_rows=entity_rows
).to_dict()
print("traffic_light_windowed_features:", online_features)
