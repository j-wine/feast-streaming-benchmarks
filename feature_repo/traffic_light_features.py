import logging
from datetime import timedelta

from feast import Field
from feast.stream_feature_view import stream_feature_view
from feast.types import Int64
from pyspark.sql import DataFrame

from data_sources import traffic_light_stream_source
from entities import traffic_light

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
def traffic_light_features_stream(df: DataFrame):
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
