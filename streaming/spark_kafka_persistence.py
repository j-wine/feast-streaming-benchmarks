"""
Alternative method of persisting from a stream feature view to the way shown in
kafka_consumer.py
This method only works in mode Spark on a KafkaSource.
The preprocess_fn is an optional function that can be provided to the stream processor.
It serves as an initial step to preprocess the raw data before any transformations
defined in the stream_feature_view are applied. This function is particularly useful for
tasks such as data cleaning, parsing, or preliminary transformations that prepare the data for subsequent processing.

The data flow is:
kafak_producer writes raw data
every 30 seconds (processing_time) spark processor triggers:
    I. preprocess_fn triggers to do pre-transformations
    II. stream_feature_view
ingest_stream_feature_view
See docs of @get_stream_processor_object
    Returns a stream processor object based on the config.

    The returned object will be capable of launching an ingestion job that reads data from the
    given stream feature view's stream source, transforms it if the stream feature view has a
    transformation, and then writes it to the online store. It will also preprocess the data
    if a preprocessor method is defined.

    raises ValueError("other processors besides spark-kafka not supported")
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from feast import FeatureStore
from feast.infra.contrib.spark_kafka_processor import SparkProcessorConfig
from feast.infra.contrib.stream_processor import get_stream_processor_object
from feast.data_source import PushMode

KAFKA_BROKER = "broker:9092"
KAFKA_TOPIC = "processed_traffic_light_signals"
BATCH_SOURCE_PATH = "../offline_data/traffic_light_data.parquet"

spark = SparkSession.builder \
    .appName("KafkaTrafficLightProcessor") \
    .getOrCreate()

schema = StructType([
    StructField("traffic_light_id", StringType(), True),
    StructField("primary_signal", IntegerType(), True),
    StructField("secondary_signal", IntegerType(), True),
    StructField("location", StringType(), True),
    StructField("timestamp", StringType(), True)  # Timestamp as string
])


def preprocess_fn(rows):
    """Preprocess function for Spark processor."""
    print(f"df columns: {rows.columns}")
    print(f"df size: {rows.size}")
    print(f"df preview:\n{rows.head()}")
    return rows


store = FeatureStore(repo_path="../feature_repo")

# Define ingestion configuration using SparkProcessorConfig
ingestion_config = SparkProcessorConfig(
    mode="spark",
    source="kafka",
    spark_session=spark,
    processing_time="30 seconds",
    query_timeout=15
)

time_feature_view = store.get_stream_feature_view("traffic_light_features_stream")

# from feature_repo.traffic_light_features import traffic_light_features_stream

processor = get_stream_processor_object(
    config=ingestion_config,
    fs=store,
    sfv=time_feature_view,
    preprocess_fn=preprocess_fn
)
# @BA triggers feature view transformations on write
query = processor.ingest_stream_feature_view(push_mode=PushMode.ONLINE_AND_OFFLINE)

processor.await_termination()

query.stop()
