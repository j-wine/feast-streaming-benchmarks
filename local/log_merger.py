import pandas as pd
import os
import sys
from datetime import datetime
import pandas as pd
import sys
from datetime import datetime

def format_hms_milliseconds(ts):
    try:
        dt = datetime.fromtimestamp(float(ts) / 1_000_000)  # convert from micro/nanoseconds if needed
        return dt.strftime("%H:%M:%S") + f":{int(dt.microsecond / 1000):03d}"
    except Exception as e:
        print(f"Failed to parse timestamp {ts}: {e}")
        return ""

def merge_and_compute_latencies(spark_csv_path, kafka_csv_path, output_csv="merged_with_latency.csv"):
    # Load CSVs
    spark_df = pd.read_csv(spark_csv_path)
    kafka_df = pd.read_csv(kafka_csv_path, sep=";")

    # Ensure consistent types
    spark_df["entity_id"] = spark_df["entity_id"].astype(int)
    kafka_df["entity_id"] = kafka_df["entity_id"].astype(int)

    # Merge on entity_id (inner join to skip mismatches)
    merged_df = pd.merge(spark_df, kafka_df, on="entity_id", how="inner")

    # Calculate latency metrics
    merged_df["preprocess_until_poll"] = merged_df["retrieval_timestamp"] - merged_df["spark_ingestion_time"]
    merged_df["preprocess_latency"] = merged_df["spark_ingestion_time"] - merged_df["receive_timestamp"]
    merged_df["diff_preprocess"] = merged_df["preprocess_until_poll"] - merged_df["preprocess_latency"]

    # Apply time formatting to all relevant columns
    for col in [
        "spark_ingestion_time",
        "receive_timestamp",
        "retrieval_timestamp",
        "preprocess_until_poll",
        "preprocess_latency",
        "diff_preprocess"
    ]:
        merged_df[col + "_hms"] = merged_df[col].apply(format_hms_milliseconds)

    # Save to CSV
    merged_df.to_csv(output_csv, index=False)
    print(f"✅ Merged file saved as '{output_csv}'")

consumer_csv_path = "../logs/kafka_latency_log.csv"
spark_csv_path = "parsed_spark_ingestion_log.csv"
output_path = "merged_log.csv"
merge_and_compute_latencies(spark_csv_path, consumer_csv_path,output_path)
