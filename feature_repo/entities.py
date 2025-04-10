from feast import Entity, ValueType

benchmark_entity = Entity(
    name="benchmark_entity",
    join_keys=["benchmark_entity"],
    value_type=ValueType.INT64,
)
