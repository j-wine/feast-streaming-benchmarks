import logging
from datetime import timedelta

import pandas as pd
from feast import Field, FeatureView
from feast.on_demand_feature_view import on_demand_feature_view
from feast.stream_feature_view import stream_feature_view
from feast.types import Int64, String, Float32, Float64
from pyspark.sql import DataFrame

from data_sources import traffic_light_stream_source, traffic_light_batch_source, push_source
from entities import traffic_light
logger = logging.getLogger('traffic_light_features')



traffic_light_stats = FeatureView(
    name="traffic_light_stats",
    description="traffic light features",
    entities=[traffic_light],
    ttl=timedelta(days=14),
    schema=[
        Field(name="traffic_light_id", dtype=String),
        Field(name="primary_signal", dtype=Int64),
        Field(name="secondary_signal", dtype=Int64),
        Field(name="location", dtype=String),
        Field(name="signal_duration", dtype=Float64),
    ],
    online=True,
    source=traffic_light_batch_source,
    tags={"production": "True"},
    owner="test1@gmail.com",
)


traffic_light_pushed_features = FeatureView(
    name="traffic_light_pushed_features",
    entities=[traffic_light],
    ttl=timedelta(days=14),
    schema=[
        Field(name="traffic_light_id", dtype=String),
        Field(name="primary_signal", dtype=Int64),
        Field(name="secondary_signal", dtype=Int64),
        Field(name="location", dtype=String),
        Field(name="signal_duration", dtype=Float64),
    ],
    online=True,
    source=push_source,
    tags={"production": "True"},
    owner="test1@gmail.com",
)



@on_demand_feature_view(
    sources=[traffic_light_pushed_features],
    schema=[
        Field(name="signal_duration_minutes", dtype=Float64),
    ],
    mode="pandas",
    write_to_online_store=True,  # @BA Transformation applied at write-time
)
def traffic_light_transformed_features(features_df: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["signal_duration_minutes"] = features_df["signal_duration"] / 60

    print("on demand transformation signal_duration_minutes", df["signal_duration_minutes"])
    return df

@stream_feature_view(
    entities=[traffic_light],
    ttl=timedelta(days=140),
    mode="spark",  # apparently spark is currently the only support "mode"
    schema=[
        Field(name="signal_duration_minutes", dtype=Float64),
    ],
    timestamp_field="event_timestamp",
    online=True,
    source=traffic_light_stream_source,
)
def traffic_light_features_stream(df: DataFrame):
    from pyspark.sql.functions import col
    """
    Placeholder transformation function for external stream processing.
    The actual processing is handled by Flink/Kafka jobs.

    ! imports have to be inside the function to serialize it !
    """
    logger.log(level=logging.INFO, msg=f"in transformation of traffic_light_features_stream")
    return df.withColumn("signal_duration_minutes", col("signal_duration") / 60)
