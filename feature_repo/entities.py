from feast import Entity

# Define the traffic light entity
traffic_light = Entity(
    name="traffic_light_id",
    join_keys=["traffic_light_id"],
    description="Unique identifier for each traffic light",
)
