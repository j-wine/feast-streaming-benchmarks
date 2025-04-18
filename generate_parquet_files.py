import datetime
import pandas as pd
import numpy as np
from pathlib import Path

def generate_data(num_entities: int, num_features: int) -> pd.DataFrame:
    features = [f"feature_{i}" for i in range(num_features)]
    columns = ["benchmark_entity", "event_timestamp"] + features

    # One row per entity, each entity appears exactly once
    df = pd.DataFrame(index=np.arange(num_entities), columns=columns)
    df["benchmark_entity"] = list(range(num_entities))
    df["event_timestamp"] = datetime.datetime.utcnow()

    feature_data = np.random.randint(1, num_entities, size=(num_entities, num_features))
    for i, feat in enumerate(features):
        df[feat] = feature_data[:, i]
    return df

if __name__ == "__main__":
    output_path = Path(__file__).parent / "feature_repo/offline_data/generated_data.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df = generate_data(num_entities=100_000, num_features=250)
    df.to_parquet(output_path, index=False)
    print(f"✅ Generated {output_path}")
