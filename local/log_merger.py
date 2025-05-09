from datetime import datetime
from datetime import timedelta

import pandas as pd


import re
from datetime import datetime


def parse_spark_ingestor_log(input_filename="logs/spark_log", output_filename="local/parsed_spark_ingestion_log.csv"):
    with open(input_filename, "r") as f:
        spark_log = f.read()

    pattern = r"Spark -> Feast ingestion timestamp: (\d+\.\d+) for entity ids: \[([^\]]*)\]"

    parsed_data = []
    matches = re.finditer(pattern, spark_log)

    for match in matches:
        print(f"matches: {match}")
        timestamp_str = match.group(1)
        entity_ids_str = match.group(2)

        if not entity_ids_str.strip():
            # Empty list: []
            continue

        try:
            timestamp = float(timestamp_str)
            entity_ids = []
            for e in entity_ids_str.split(","):
                e = e.strip()
                if e.isdigit():
                    entity_ids.append(int(e))
            for entity_id in entity_ids:
                parsed_data.append({
                    "entity_id": entity_id,
                    "spark_ingestion_time": timestamp
                })
        except Exception as e:
            print(f"⚠️ Skipping malformed line: {match.group(0)} — {e}")
            continue

    df_spark_log = pd.DataFrame(parsed_data)

    def format_high_precision(ts):
        dt = datetime.fromtimestamp(ts)
        fractional = f"{ts:.15f}".split(".")[1]
        return dt.strftime("%H:%M:%S") + "." + fractional

    if not df_spark_log.empty:
        df_spark_log["spark_ingestion_hms"] = df_spark_log["spark_ingestion_time"].apply(format_high_precision)
    else:
        print("⚠️ No valid entity IDs found in spark log.")

    df_spark_log.to_csv(output_filename, index=False)
    print(f"✅ Parsed {len(df_spark_log)} rows into {output_filename}")


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
        "produce_timestamp"
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

if __name__ == "__main__":
    consumer_csv_path = "logs/kafka_latency_log.csv"
    spark_csv_path = "local/parsed_spark_ingestion_log.csv"
    output_path = "local/merged_log.csv"
    parse_spark_ingestor_log()
    merge_and_compute_latencies(spark_csv_path, consumer_csv_path,output_path)
