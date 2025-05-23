from datetime import timedelta

from feast import Field
from feast.stream_feature_view import stream_feature_view
from feast.types import Int64
from pyspark.sql import DataFrame

from entities import benchmark_entity
from data_sources import benchmark_stream_source_10_features, benchmark_stream_source_100_features, \
    benchmark_stream_source_250_features, benchmark_stream_source_50_features, benchmark_stream_source_1_features


def generate_feature_schema(num_features: int):
    return [Field(name=f"feature_{i}", dtype=Int64) for i in range(num_features)]

@stream_feature_view(
    entities=[benchmark_entity],
    ttl=timedelta(days=140),
    mode="spark",
    schema=[
        Field(name="sum", dtype=Int64),
    ],
    timestamp_field="event_timestamp",
    online=True,
    source=benchmark_stream_source_1_features,
)
def stream_view_1in_1out(df: DataFrame):
    from pyspark.sql.functions import col
    from pyspark.sql.types import LongType
    df = df.withColumn("sum", (col("feature_0")).cast(LongType()))
    return df.select("benchmark_entity","event_timestamp", "sum")

@stream_feature_view(
    entities=[benchmark_entity],
    ttl=timedelta(days=140),
    mode="spark",  # apparently spark is currently the only support "mode"
    schema=[
        Field(name="sum", dtype=Int64),
    ],
    timestamp_field="event_timestamp",
    online=True,
    source=benchmark_stream_source_10_features,
)
def stream_view_10in_1out(df: DataFrame):
    from pyspark.sql.functions import col
    from pyspark.sql.types import LongType
    df = df.withColumn("sum", (col("feature_0") + col("feature_1")).cast(LongType()))
    return df.select("benchmark_entity","event_timestamp", "sum")

# technically 11 features out, but we need the sum field to indicate successfull processing by spark
@stream_feature_view(
    entities=[benchmark_entity],
    ttl=timedelta(days=140),
    mode="spark",  # apparently spark is currently the only support "mode"
    schema=[Field(name="sum", dtype=Int64)] + generate_feature_schema(10),
    timestamp_field="event_timestamp",
    online=True,
    source=benchmark_stream_source_10_features,
)
def stream_view_10in_10out(df: DataFrame):
    from pyspark.sql.functions import col
    from pyspark.sql.types import LongType
    df = df.withColumn("sum", (col("feature_0") + col("feature_1")).cast(LongType()))
    return df

@stream_feature_view(
    entities=[benchmark_entity],
    ttl=timedelta(days=140),
    mode="spark",
    schema=[Field(name="sum", dtype=Int64)] + generate_feature_schema(50),
    timestamp_field="event_timestamp",
    online=True,
    source=benchmark_stream_source_50_features,
)
def stream_view_50in_1out(df: DataFrame):
    from pyspark.sql.functions import col
    from pyspark.sql.types import LongType
    df = df.withColumn("sum", (col("feature_0") + col("feature_49")).cast(LongType()))
    return df.select("benchmark_entity", "event_timestamp", "sum")

@stream_feature_view(
    entities=[benchmark_entity],
    ttl=timedelta(days=140),
    mode="spark",
    schema=[Field(name="sum", dtype=Int64)] + generate_feature_schema(50),
    timestamp_field="event_timestamp",
    online=True,
    source=benchmark_stream_source_50_features,
)
def stream_view_50in_50out(df: DataFrame):
    from pyspark.sql.functions import col
    from pyspark.sql.types import LongType
    df = df.withColumn("sum", (col("feature_0") + col("feature_49")).cast(LongType()))
    return df


@stream_feature_view(
    entities=[benchmark_entity],
    ttl=timedelta(days=140),
    mode="spark",
    schema=[Field(name="sum", dtype=Int64)],
    timestamp_field="event_timestamp",
    online=True,
    source=benchmark_stream_source_100_features,
)
def stream_view_100in_1out(df: DataFrame):
    from pyspark.sql.functions import col
    from pyspark.sql.types import LongType
    df = df.withColumn("sum", (col("feature_0") + col("feature_99")).cast(LongType()))
    return df.select("benchmark_entity", "event_timestamp", "sum")

@stream_feature_view(
    entities=[benchmark_entity],
    ttl=timedelta(days=140),
    mode="spark",
    schema=[Field(name="sum", dtype=Int64)] + generate_feature_schema(100),
    timestamp_field="event_timestamp",
    online=True,
    source=benchmark_stream_source_100_features,
)
def stream_view_100in_100out(df: DataFrame):
    from pyspark.sql.functions import col
    from pyspark.sql.types import LongType
    df = df.withColumn("sum", (col("feature_0") + col("feature_99")).cast(LongType()))
    return df

@stream_feature_view(
    entities=[benchmark_entity],
    ttl=timedelta(days=140),
    mode="spark",
    schema=[Field(name="sum", dtype=Int64)],
    timestamp_field="event_timestamp",
    online=True,
    source=benchmark_stream_source_250_features,
)
def stream_view_250in_1out(df: DataFrame):
    from pyspark.sql.functions import col
    from pyspark.sql.types import LongType
    df = df.withColumn("sum", (col("feature_0") + col("feature_249")).cast(LongType()))
    return df

@stream_feature_view(
    entities=[benchmark_entity],
    ttl=timedelta(days=140),
    mode="spark",
    schema=[Field(name="sum", dtype=Int64)] + generate_feature_schema(250),
    timestamp_field="event_timestamp",
    online=True,
    source=benchmark_stream_source_250_features,
)
def stream_view_250in_250out(df: DataFrame):
    from pyspark.sql.functions import col
    from pyspark.sql.types import LongType
    df = df.withColumn("sum", (col("feature_0") + col("feature_249")).cast(LongType()))
    return df