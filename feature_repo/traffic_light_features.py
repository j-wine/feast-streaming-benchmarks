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
    ttl=timedelta(days=14),
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
    # logs or prints here somehow aren't visible in container log but the transformation does get triggered on store.push
    print("in transformation of traffic_light_features_stream, signal duration: ", col("signal_duration"))
    print("in transformation of traffic_light_features_stream, df: ", df)
    from pyspark.sql.functions import col, unix_timestamp, lag, when, lit
    from pyspark.sql.types import TimestampType
    from pyspark.sql.window import Window

    print("🚦 Debug: Processing traffic_light_features_stream. Input DF:\n")
    df.printSchema()

    # Ensure event_timestamp is in the correct format
    df = df.withColumn("event_timestamp", col("event_timestamp").cast(TimestampType()))

    # Apply watermarking to handle late-arriving data
    df = df.withWatermark("event_timestamp", "2 minutes")

    # Define window for tracking event order (grouped per traffic light)
    window_spec = Window.partitionBy("traffic_light_id").orderBy("event_timestamp")

    df = df.withColumn("prev_timestamp", lag("event_timestamp").over(window_spec))

    # Compute signal duration
    df = df.withColumn("signal_duration",
                       when(col("prev_timestamp").isNotNull(),
                            unix_timestamp(col("event_timestamp")) - unix_timestamp(col("prev_timestamp")))
                       .otherwise(None))

    # Convert to minutes
    df = df.withColumn("signal_duration_minutes", col("signal_duration") / lit(60))

    print("✅ Debug: Returning traffic_light_features_stream", df)
    return df



@stream_feature_view(
    entities=[traffic_light],
    ttl=timedelta(days=14),
    mode="spark",
    schema=[
        Field(name="avg_primary_red_duration", dtype=Float64),
        Field(name="avg_primary_green_duration", dtype=Float64),
        Field(name="avg_primary_amber_duration", dtype=Float64),
        Field(name="avg_secondary_red_duration", dtype=Float64),
        Field(name="avg_switch_time_green_to_red", dtype=Float64),
        Field(name="avg_switch_time_red_to_green", dtype=Float64),
    ],
    timestamp_field="event_timestamp",
    online=True,
    source=traffic_light_stream_source,
)
def traffic_light_windowed_features(df: DataFrame):
    from pyspark.sql.functions import col, unix_timestamp, window, avg, lit, when
    from pyspark.sql.types import TimestampType, StructType, StructField, IntegerType, DoubleType
    from pyspark.sql.streaming.state import GroupState, GroupStateTimeout

    # Ensure event_timestamp is in correct format
    df = df.withColumn("event_timestamp", col("event_timestamp").cast(TimestampType()))

    # Apply watermarking to handle late-arriving data
    df = df.withWatermark("event_timestamp", "2 minutes")

    # Define state structure for tracking previous signal and timestamp
    state_schema = StructType([
        StructField("prev_primary_signal", IntegerType(), True),
        StructField("prev_secondary_signal", IntegerType(), True),
        StructField("prev_timestamp", TimestampType(), True)
    ])

    from pyspark.sql.types import StringType

    def update_state(key, data, state: GroupState):
        """
        Tracks previous signal state and computes signal durations.
        Returns an iterable of Pandas DataFrames instead of a single DataFrame.
        """
        prev_primary_signal = None
        prev_secondary_signal = None
        prev_timestamp = None

        # Correct way to get state: Use state.get ONLY
        if state.exists:
            previous_state = state.get
            prev_primary_signal = previous_state[0]
            prev_secondary_signal = previous_state[1]
            prev_timestamp = previous_state[2]

        results = []

        # Iterate over the generator and process each Pandas DataFrame
        for batch in data:
            for row in batch.itertuples(index=False):
                current_primary_signal = row.primary_signal
                current_secondary_signal = row.secondary_signal
                current_timestamp = row.event_timestamp  # This is a Python datetime object

                # Compute signal duration using native Python timestamp conversion
                signal_duration = None
                if prev_primary_signal is not None and prev_timestamp is not None:
                    signal_duration = (
                                                  current_timestamp.timestamp() - prev_timestamp.timestamp()) / 60  # Convert to minutes

                # Detect switches
                switch_to_green = current_timestamp if prev_primary_signal != 3 and current_primary_signal == 3 else None
                switch_to_red = current_timestamp if prev_primary_signal == 3 and current_primary_signal == 1 else None

                # Compute switch durations using Python timestamps
                switch_duration_green_to_red = None
                switch_duration_red_to_green = None
                if switch_to_red is not None and switch_to_green is not None:
                    switch_duration_green_to_red = (switch_to_red.timestamp() - switch_to_green.timestamp()) / 60
                if switch_to_green is not None and switch_to_red is not None:
                    switch_duration_red_to_green = (switch_to_green.timestamp() - switch_to_red.timestamp()) / 60

                results.append((str(key[0]), current_timestamp, current_primary_signal, current_secondary_signal,
                                signal_duration, switch_duration_green_to_red, switch_duration_red_to_green))

                # Correct way to update state: Use a tuple
                state.update((current_primary_signal, current_secondary_signal, current_timestamp))

        # Convert results to a Pandas DataFrame
        yield pd.DataFrame(results,
                           columns=["traffic_light_id", "event_timestamp", "primary_signal", "secondary_signal",
                                    "signal_duration", "switch_duration_green_to_red", "switch_duration_red_to_green"])

    # Update the schema to use StringType() for traffic_light_id
    output_schema = StructType([
        StructField("traffic_light_id", StringType(), False),  # <-- Changed from IntegerType() to StringType()
        StructField("event_timestamp", TimestampType(), False),
        StructField("primary_signal", IntegerType(), False),
        StructField("secondary_signal", IntegerType(), False),
        StructField("signal_duration", DoubleType(), True),
        StructField("switch_duration_green_to_red", DoubleType(), True),
        StructField("switch_duration_red_to_green", DoubleType(), True)
    ])

    # Step 1: Apply stateful processing to track signal changes per traffic light
    stateful_df = df.groupBy("traffic_light_id").applyInPandasWithState(
        update_state,
        outputStructType=output_schema,
        stateStructType=state_schema,
        outputMode="Append",  # <-- Change from "Update" to "Append" since aggregations aren't supported in Update mode
        timeoutConf=GroupStateTimeout.ProcessingTimeTimeout
    )

    # Step 2: Perform windowed aggregation on the stateful DataFrame
    windowed_df = stateful_df.withWatermark("event_timestamp", "30 minutes").groupBy(
        window(col("event_timestamp"), "30 minutes", "30 seconds"), col("traffic_light_id")
    ).agg(
        avg(when(col("primary_signal") == 1, col("signal_duration"))).alias("avg_primary_red_duration"),
        avg(when(col("primary_signal") == 3, col("signal_duration"))).alias("avg_primary_green_duration"),
        avg(when(col("primary_signal") == 2, col("signal_duration"))).alias("avg_primary_amber_duration"),
        avg(when(col("secondary_signal") == 1, col("signal_duration"))).alias("avg_secondary_red_duration"),
        avg(when(col("switch_duration_green_to_red").isNotNull(), col("switch_duration_green_to_red"))).alias(
            "avg_switch_time_green_to_red"),
        avg(when(col("switch_duration_red_to_green").isNotNull(), col("switch_duration_red_to_green"))).alias(
            "avg_switch_time_red_to_green")
    )

    print("Debug: Returning windowed features in a separate query.")
    return windowed_df.withColumn("event_timestamp", col("window.start"))