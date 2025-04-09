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
    """Preprocess function to log Spark DataFrame details before writing to Feast."""
    import time
    feast_ingestion_time = time.time()  # @BA Timestamp when Spark pushes data to Feast
    # i.e. after feature views method body, when the spark df is transformed to pandas df by feast.
    # as that step is mandatory, we can measure time between call of preprocess and retrieval of features
    print(f"Spark -> Feast ingestion timestamp: {feast_ingestion_time:.6f}")
    # # Define the columns to display in the log
    # columns_to_display = [
    #     "avg_signal_duration_minutes",
    #     "primary_signal_count",
    #     "secondary_signal_count",
    #     "total_windowed_primary_signal_duration",
    #     "total_windowed_secondary_signal_duration"
    # ]
    #
    # # Check if the DataFrame contains all the necessary columns
    # if not all(col in rows.columns for col in columns_to_display):
    #     missing_cols = [col for col in columns_to_display if col not in rows.columns]
    #     print(f"Missing columns in DataFrame: {missing_cols}")
    # else:
    #     print(rows[columns_to_display])

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
traffic_light_windowed_features = store.get_stream_feature_view("feature_sum")

# Initialize stream processor
processor = get_stream_processor_object(
    config=ingestion_config,
    fs=store,
    sfv=traffic_light_windowed_features,
    preprocess_fn=preprocess_fn
)
query = processor.ingest_stream_feature_view(to=PushMode.ONLINE)

query.awaitTermination()