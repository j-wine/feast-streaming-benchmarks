import os
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
    if not os.path.exists(spark_csv_path):
        print(f"❌ File not found: {spark_csv_path}")
        return
    if not os.path.exists(kafka_csv_path):
        print(f"❌ File not found: {kafka_csv_path}")
        return

    try:
        spark_df = pd.read_csv(spark_csv_path)
        if spark_df.empty:
            print(f"⚠️ Empty Spark DataFrame from: {spark_csv_path}")
            return
    except Exception as e:
        print(f"❌ Failed to read Spark CSV: {e}")
        return


    # Load CSVs
    spark_df = pd.read_csv(spark_csv_path)
    try:
        kafka_df = pd.read_csv(kafka_csv_path, sep=";")
        if kafka_df.empty:
            print(f"⚠️ Empty Kafka DataFrame from: {kafka_csv_path}")
            return
    except Exception as e:
        print(f"❌ Failed to read Kafka CSV: {e}")
        return

    try:
        spark_df["entity_id"] = spark_df["entity_id"].astype(int)
        kafka_df["entity_id"] = kafka_df["entity_id"].astype(int)

        merged_df = pd.merge(spark_df, kafka_df, on="entity_id", how="inner")
        if merged_df.empty:
            print("⚠️ No matching rows found after merge.")
            return

        for col in ["spark_ingestion_time", "receive_timestamp", "retrieval_timestamp", "produce_timestamp"]:
            merged_df[col + "_hms"] = merged_df[col].apply(format_timestamp_hms_milliseconds)


        # If spark_ingestion_time < retrieval_timestamp (the normal case), then we take it.
        # If spark_ingestion_time > retrieval_timestamp (error case with batch congestion), then we know:
        # retrieval_timestamp - produce_timestamp is always correct (worst case total latency)
        merged_df["preprocess_until_poll"] = merged_df.apply(
            lambda row: row["retrieval_timestamp"] - row["spark_ingestion_time"]
            if row["retrieval_timestamp"] >= row["spark_ingestion_time"]
            else row["retrieval_timestamp"] - row["receive_timestamp"],  # fallback: total latency between message receival in the consumer and sucessfull read
            axis=1
        )
        merged_df["latency_fallback_used"] = merged_df.apply(
            lambda row: row["retrieval_timestamp"] < row["spark_ingestion_time"],
            axis=1
        )
        merged_df["produce_to_ingest"] = merged_df["spark_ingestion_time"] - merged_df["produce_timestamp"]
        merged_df["produce_to_receive"] = merged_df["receive_timestamp"] - merged_df["produce_timestamp"]
        merged_df["produce_to_retrieve"] = merged_df["retrieval_timestamp"] - merged_df["produce_timestamp"]

        ordered_columns = (
            ["entity_id"] +
            ["preprocess_until_poll", "produce_to_ingest", "produce_to_receive", "produce_to_retrieve"] +
            ["produce_timestamp_hms", "receive_timestamp_hms", "spark_ingestion_time_hms", "retrieval_timestamp_hms"] +
            ["produce_timestamp", "receive_timestamp", "spark_ingestion_time", "retrieval_timestamp"]
        )
        merged_df = merged_df[ordered_columns]

        for col in merged_df.columns:
            if merged_df[col].dtype == float:
                merged_df[col] = merged_df[col].map(lambda x: f"{x:.6f}".replace('.', ','))

        merged_df.to_csv(output_csv, sep=';', index=False)
        print(f"✅ Merged data written to {output_csv}")
    except Exception as e:
        print(f"❌ Failed during merge or processing: {e}")

if __name__ == "__main__":
    consumer_csv_path = "logs/kafka_latency_log.csv"
    spark_csv_path = "local/parsed_spark_ingestion_log.csv"
    output_path = "local/merged_log.csv"
    parse_spark_ingestor_log()
    merge_and_compute_latencies(spark_csv_path, consumer_csv_path,output_path)
