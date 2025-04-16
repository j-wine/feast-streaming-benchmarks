from datetime import datetime
from datetime import timedelta

import pandas as pd


def format_timestamp_hms_milliseconds(ts):
    try:
        ts = float(ts)
        dt = datetime.fromtimestamp(ts)
        milliseconds = int((ts % 1) * 1000)
        return dt.strftime("%H:%M:%S") + f":{milliseconds:03d}"
    except Exception as e:
        print(f"⚠️ Failed to parse timestamp {ts}: {e}")
        return ""

def format_duration(ts):
    """Format duration (seconds) to HH:MM:SS:MS, no timezone bias."""
    try:
        ts = float(ts)
        td = timedelta(seconds=ts)
        total_seconds = int(td.total_seconds())
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        milliseconds = int((ts % 1) * 1000)
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}:{milliseconds:03d}"
    except Exception as e:
        print(f"⚠️ Failed to format duration {ts}: {e}")
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
    # add human hour:minute:second form of timestamps
    for col in [
        "spark_ingestion_time",
        "receive_timestamp",
        "retrieval_timestamp",
        "produce_timestamp",
    ]:
        merged_df[col + "_hms"] = merged_df[col].apply(format_timestamp_hms_milliseconds)
    # Calculate latency metrics (durations)
    # durations are already formatted in seconds
    merged_df["preprocess_until_poll"] = merged_df["retrieval_timestamp"] - merged_df["spark_ingestion_time"]
    # the kafka consumer receive timestamp is sometimes later than spark ingest
    # therefore is sometimes negative value, causing parsing error
    # merged_df["receive_to_ingestor"] = merged_df["spark_ingestion_time"] - merged_df["receive_timestamp"]
    merged_df["produce_to_ingest"] = merged_df["spark_ingestion_time"] - merged_df["produce_timestamp"]
    merged_df["produce_to_receive"] = merged_df["receive_timestamp"] - merged_df["produce_timestamp"]
    merged_df["produce_to_retrieve"] = merged_df["retrieval_timestamp"] - merged_df["produce_timestamp"]

    merged_df.to_csv(output_csv, index=False)
    print(f"✅ Merged file saved as '{output_csv}'")

consumer_csv_path = "../logs/kafka_latency_log.csv"
spark_csv_path = "parsed_spark_ingestion_log.csv"
output_path = "merged_log.csv"
merge_and_compute_latencies(spark_csv_path, consumer_csv_path,output_path)
