from feast import FeatureService

from feature_repo import traffic_light_features
from traffic_light_features import traffic_light_features_stream, traffic_light_stats, \
    traffic_light_transformed_features, traffic_light_pushed_features

# Define a FeatureService for the traffic light model
traffic_light_service = FeatureService(
    name="traffic_light_service",
    features=[traffic_light_features_stream, traffic_light_stats, traffic_light_transformed_features, traffic_light_pushed_features],
    description="Service to provide traffic light features"
)
