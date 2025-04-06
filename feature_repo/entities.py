from feast import Entity, ValueType
from feast.types import Int64

# Define the traffic light entity
traffic_light = Entity(
    name="traffic_light_id",
    join_keys=["traffic_light_id"],
    description="Unique identifier for each traffic light",
    value_type=ValueType.STRING
)

benchmark_entity = Entity(
    name="benchmark_entity",
    join_keys=["benchmark_entity"],
    value_type=ValueType.INT64,
)
