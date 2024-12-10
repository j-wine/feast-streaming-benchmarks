from datetime import timedelta
from feast import Field
from feast.stream_feature_view import stream_feature_view
from feast.types import Int64, String, Float32
from feature_repo.data_sources import traffic_light_stream_source
from feature_repo.entities import traffic_light


@stream_feature_view(
    entities=[traffic_light],
    ttl=timedelta(days=1),
    mode="spark",           # apparently spark is currently the only support "mode"
    schema=[
        Field(name="primary_signal", dtype=Int64),
        Field(name="secondary_signal", dtype=Int64),
        Field(name="location", dtype=String),
        Field(name="signal_duration", dtype=Float32),
    ],
    timestamp_field="timestamp",
    online=True,
    source=traffic_light_stream_source,
)
def traffic_light_features_stream(df):
    """
    Placeholder transformation function for external stream processing.
    The actual processing is handled by Flink/Kafka jobs.

    ! imports have to be inside the function to serialize it !
    """
    # from pyspark.sql.functions import col
    # return df.withColumn("signal_duration_minutes", col("signal_duration") / 60)
    return df
