import re
import pandas as pd
from datetime import datetime


def parse_spark_ingestor_log(input_filename= "spark_ingestor.log", output_filename= "parsed_spark_ingestion_log.csv"):

    # Load your spark log (adjust path as needed)
    with open(input_filename, "r") as f:
        spark_log = f.read()

    # Regex to extract timestamp and entity ids
    pattern = r"Spark -> Feast ingestion timestamp: (\d+\.\d+) for entity ids: \[([^\]]+)\]"

    parsed_data = []
    matches = re.findall(pattern, spark_log)

    for timestamp_str, entity_ids_str in matches:
        timestamp = float(timestamp_str)
        entity_ids = [int(e.strip()) for e in entity_ids_str.split(",") if e.strip()]
        for entity_id in entity_ids:
            parsed_data.append({
                "entity_id": entity_id,
                "spark_ingestion_time": timestamp
            })

    df_spark_log = pd.DataFrame(parsed_data)

    # Custom time format with full precision (up to nanoseconds or more)
    def format_high_precision(ts):
        dt = datetime.fromtimestamp(ts)
        fractional = f"{ts:.15f}".split(".")[1]
        return dt.strftime("%H:%M:%S") + "." + fractional

    df_spark_log["spark_ingestion_hms"] = df_spark_log["spark_ingestion_time"].apply(format_high_precision)

    # Save to CSV (optional)
    df_spark_log.to_csv(output_filename, index=False)
