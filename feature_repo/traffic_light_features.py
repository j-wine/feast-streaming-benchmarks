from datetime import timedelta

from feast import Field
from feast.stream_feature_view import stream_feature_view
from feast.types import Int64
from pyspark.sql import DataFrame

from entities import benchmark_entity
from feature_repo.data_sources import benchmark_stream_source


@stream_feature_view(
    entities=[benchmark_entity],
    ttl=timedelta(days=140),
    mode="spark",  # apparently spark is currently the only support "mode"
    schema=[
        Field(name="sum", dtype=Int64),
    ],
    timestamp_field="event_timestamp",
    online=True,
    source=benchmark_stream_source,
)
def feature_sum(df: DataFrame):
    from pyspark.sql.functions import col
    from pyspark.sql.types import LongType
    df = df.withColumn("sum", (col("feature_0") + col("feature_1")).cast(LongType()))
    return df.select("benchmark_entity","event_timestamp", "sum")