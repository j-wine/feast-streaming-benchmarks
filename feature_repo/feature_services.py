from feast import FeatureService

from feature_repo.traffic_light_features import traffic_light_features

# Define a FeatureService for the traffic light model
traffic_light_service = FeatureService(
    name="traffic_light_service",
    features=[traffic_light_features],
    description="Service to provide traffic light features"
)
