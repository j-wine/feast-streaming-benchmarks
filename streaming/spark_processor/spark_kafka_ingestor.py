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


SFV_10_FEATURES_SUM = "feature_sum"
SFV_100_FEATURES_SUM = "hundred_features_sum"
SFV_100_FEATURES_SUM_100 = "hundred_features_all_sum"
# sfv to ingest
STREAM_FEATURE_VIEW = SFV_100_FEATURES_SUM_100
# second of the minute for producer to start sending. not longer needed when we send one meaningless  message to init topic
PROCESSING_START=30

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
    processing_time="1 seconds",
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