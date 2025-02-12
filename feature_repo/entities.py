from feast import Entity, ValueType
from feast.types import Int64

# Define the traffic light entity
traffic_light = Entity(
    name="traffic_light_id",
    join_keys=["traffic_light_id"],
    description="Unique identifier for each traffic light",
    value_type=ValueType.STRING
)

     # value_type=Int64.to_value_type()  # Using the PrimitiveFeastType for INT64