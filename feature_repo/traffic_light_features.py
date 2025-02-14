import logging
from datetime import timedelta

import pandas as pd
from feast import Field, FeatureView
from feast.on_demand_feature_view import on_demand_feature_view
from feast.stream_feature_view import stream_feature_view
from feast.types import Int64, String, Float64
from pyspark.sql import DataFrame

from data_sources import traffic_light_stream_source, traffic_light_batch_source, push_source, \
    traffic_lights_request_source
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
)

@on_demand_feature_view(
    sources=[traffic_lights_request_source],
    # singleton is only applicable when mode="python"
    singleton=False,
    description="traffic light features for transformation on read time",
    mode="pandas",
    write_to_online_store=True,
    entities=[traffic_light],
    schema = [Field(name="signal_duration_minutes", dtype=Float64)]
)
def on_demand_read_time_transformed_features(features_df: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["signal_duration_minutes"] = features_df["signal_duration"] / 60
    print("read time on demand transformation signal_duration_minutes", df["signal_duration_minutes"])
    return df


@on_demand_feature_view(
    sources=[traffic_light_pushed_features],
    entities=[traffic_light],
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
    # !@BA imports have to be inside the function to serialize it !    """

    from pyspark.sql.functions import col
    """
    The transformation in method body is called when writing to the store via the view.
    The input pyspark.sql.dataframe can be transformed with spark.
    @BA More complex transformations should be done with an ingestion config using spark.

    logger.log(level=logging.INFO, msg=f"in transformation of traffic_light_features_stream")
    """
    # logs or prints here somehow arent visible in container log but the transformation does get triggered on store.push
    print("in transformation of traffic_light_features_stream")
    return df.withColumn("signal_duration_minutes", col("signal_duration") / 60)
