from datetime import datetime
from datetime import timedelta

import pandas as pd

import re


def parse_spark_ingestor_log(input_filename= "spark_log.csv", output_filename= "parsed_spark_ingestion_log.csv"):

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


    # Reorder columns
    ordered_columns = (
        ["entity_id"] +
        ["preprocess_until_poll", "produce_to_ingest", "produce_to_receive", "produce_to_retrieve", ] +
        ["produce_timestamp_hms", "receive_timestamp_hms", "spark_ingestion_time_hms", "retrieval_timestamp_hms"] +
        ["produce_timestamp", "receive_timestamp", "spark_ingestion_time", "retrieval_timestamp"]
    )
    merged_df = merged_df[ordered_columns]

    # Convert all float values from dot to comma format for Excel compatibility
    for col in merged_df.columns:
        if merged_df[col].dtype == float:
            merged_df[col] = merged_df[col].map(lambda x: f"{x:.6f}".replace('.', ','))

    # Save to CSV with semicolon delimiter
    merged_df.to_csv(output_csv, sep=';', index=False)
    print(f"✅ Merged file saved as '{output_csv}' with ';' separator and comma decimal formatting.")


consumer_csv_path = "../logs/kafka_latency_log.csv"
spark_csv_path = "parsed_spark_ingestion_log.csv"
output_path = "merged_log.csv"
parse_spark_ingestor_log()
merge_and_compute_latencies(spark_csv_path, consumer_csv_path,output_path)
