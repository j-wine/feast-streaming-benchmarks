import logging
from datetime import timedelta
from pyspark.sql import DataFrame

import pandas as pd
from feast import Field, FeatureView
from feast.on_demand_feature_view import on_demand_feature_view
from feast.stream_feature_view import stream_feature_view
from feast.types import Int64, String, Float64



from entities import traffic_light
from feature_repo.data_sources import traffic_light_stream_source

logger = logging.getLogger('traffic_light_features')

@stream_feature_view(
    entities=[traffic_light],
    ttl=timedelta(days=140),
    mode="spark",  # apparently spark is currently the only support "mode"
    schema=[
        Field(name="signal_sum", dtype=Int64),
    ],
    timestamp_field="event_timestamp",
    online=True,
    source=traffic_light_stream_source,
)
def benchmark_stream_feature_view(df: DataFrame):
    # !@BA imports have to be inside the function to serialize it !    """

    """
    The transformation in method body is called when writing to the store via the view.
    The input pyspark.sql.dataframe can be transformed with spark.
    @BA More complex windowed transformations should be done with an ingestion config using spark

    logger.log(level=logging.INFO, msg=f"in transformation of traffic_light_features_stream")
    """
    from pyspark.sql.functions import col
    print("in traffic_light_features_stream df schema:")
    df.printSchema()

    result_df = df.withColumn("signal_sum", col("primary_signal") + col("secondary_signal"))
    result_df.show()
    return result_df


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

    return df.withColumn("signal_duration_minutes", col("primary_signal") + col("secondary_signal"))

@stream_feature_view(
    entities=[traffic_light],
    ttl=timedelta(days=14),
    mode="spark",
    schema=[
        Field(name="avg_signal_duration_minutes", dtype=Float64),
        Field(name="primary_signal_count", dtype=Int64),
        Field(name="secondary_signal_count", dtype=Int64),
        Field(name="total_windowed_primary_signal_duration", dtype=Float64),
        Field(name="total_windowed_secondary_signal_duration", dtype=Float64),
    ],
    timestamp_field="event_timestamp",
    online=True,
    source=traffic_light_stream_source,
)
def traffic_light_windowed_features(df: DataFrame):
    """
    Compute windowed statistics:
    - Average signal duration in minutes.
    - Count occurrences of each primary and secondary signal.
    - Total accumulated signal durations for primary and secondary signals.
    """
    from pyspark.sql.functions import col, window, avg, count, sum
    from pyspark.sql.types import TimestampType
    print("in windowed_features df schema:")
    df.printSchema()
    print("df in windowed_features:")
    print(df)
    # Convert signal duration to minutes
    df = df.withColumn("event_timestamp",
                       col("event_timestamp").cast(TimestampType()))

    df = df.withColumn("signal_duration_minutes", col("primary_signal") + col("secondary_signal"))

    # Aggregate over a 10-minute window
    windowed_df = df.groupBy(window(col("event_timestamp"), "10 minutes"), col("traffic_light_id")) \
        .agg(
            avg("signal_duration_minutes").alias("avg_signal_duration_minutes"),
            count("primary_signal").alias("primary_signal_count"),
            count("secondary_signal").alias("secondary_signal_count"),
            sum("signal_duration_minutes").alias("total_windowed_primary_signal_duration"),
            sum("signal_duration_minutes").alias("total_windowed_secondary_signal_duration")
        )
    print("before return in windowed_features:")
    # Ensure event timestamp exists for Feast
    return windowed_df.withColumn("event_timestamp", col("window.start"))