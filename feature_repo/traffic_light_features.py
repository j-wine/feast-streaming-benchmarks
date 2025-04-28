from datetime import timedelta

from feast import Field, FeatureView
from feast.stream_feature_view import stream_feature_view
from feast.types import Int64, UnixTimestamp
from pyspark.sql import DataFrame

from entities import benchmark_entity
from data_sources import benchmark_stream_source, hundred_features_benchmark_stream_source, benchmark_batch_source, \
    benchmark_push_source

benchmark_sum_push_fv= FeatureView(name="benchmark_push_stats",
                            entities=[benchmark_entity],
                             ttl=timedelta(days=140),
                             online=True,
                             source=benchmark_push_source,
                             schema=[
                                 Field(name="sum", dtype=Int64), Field(name="benchmark_entity", dtype=Int64),
                                 Field(
                                     name="event_timestamp",
                                     dtype=UnixTimestamp,
                                     description="Event timestamp of the batch",
                                 )
                             ],
                             )

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

# def generate_feature_schema(num_features: int):
#     return [Field(name=f"feature_{i}", dtype=Int64) for i in range(num_features)]

# @stream_feature_view(
#     entities=[benchmark_entity],
#     ttl=timedelta(days=140),
#     mode="spark",
#     schema=[Field(name="sum", dtype=Int64)],
#     timestamp_field="event_timestamp",
#     online=True,
#     source=hundred_features_benchmark_stream_source,
# )
# def hundred_features_sum(df: DataFrame):
#     from pyspark.sql.functions import col
#     from pyspark.sql.types import LongType
#     df = df.withColumn("sum", (col("feature_0") + col("feature_99")).cast(LongType()))
#     return df.select("benchmark_entity", "event_timestamp", "sum")
#
# @stream_feature_view(
#     entities=[benchmark_entity],
#     ttl=timedelta(days=140),
#     mode="spark",
#     schema=[Field(name="sum", dtype=Int64)] + generate_feature_schema(100),
#     timestamp_field="event_timestamp",
#     online=True,
#     source=hundred_features_benchmark_stream_source,
# )
# def hundred_features_all_sum(df: DataFrame):
#     from pyspark.sql.functions import col
#     from pyspark.sql.types import LongType
#     df = df.withColumn("sum", (col("feature_0") + col("feature_99")).cast(LongType()))
#     return df