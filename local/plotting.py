import os
from datetime import datetime

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def analyze_thread_stats_vs_latency(thread_csv, latency_csv, is_grouped=True, eps=2500, interval=1,
                                     rows=100_000, input_features=100, output_features=100,
                                     online_store="redis", operating_system="linux", output_prefix="plots/analysis"):
    # Load thread stats
    df_threads = pd.read_csv(thread_csv, sep=";")
    df_threads.rename(columns={"second": "timestamp_second", "requests": "requests_per_second"}, inplace=True)

    # Convert float timestamps in latency log to int seconds
    df_latency = pd.read_csv(latency_csv, sep=";")
    df_latency["retrieval_sec"] = df_latency["retrieval_timestamp"].astype(str).str.replace(",", ".").astype(float).astype(int)

    # Join on rounded retrieval second
    df_merged = pd.merge(
        df_latency,
        df_threads,
        left_on="retrieval_sec",
        right_on="timestamp_second",
        how="left"
    )

    if df_merged.empty:
        print("⚠️ No matches between retrieval timestamps and thread log seconds.")
        return

    # Convert latency to float
    df_merged["preprocess_until_poll"] = df_merged["preprocess_until_poll"].astype(str).str.replace(",", ".").astype(float)
    df_merged["produce_to_retrieve"] = df_merged["produce_to_retrieve"].astype(str).str.replace(",", ".").astype(float)

    # Determine mode
    poll_mode = "Grouped Poll" if is_grouped else "Single Poll"
    title_info = f"{poll_mode} — {eps} EPS, {interval}s Interval, {rows} Rows, {input_features} In, {output_features} Out"
    mode = "grouped" if is_grouped else "single"

    # === Plot: Latency vs. Requests Per Second ===
    plt.figure(figsize=(10, 6))
    plt.scatter(df_merged["requests_per_second"], df_merged["preprocess_until_poll"], alpha=0.4)
    plt.xlabel("Requests per Second")
    plt.ylabel("Preprocess → Poll Latency (s)")
    plt.title(f"Latency vs. Request Volume\n{title_info}")
    plt.grid(True)
    plt.tight_layout()
    filename1 = f"{output_prefix}_lat_vs_rps_{operating_system}_{online_store}_{mode}_{eps}eps_{interval}s_{rows}rows_{input_features}in_{output_features}out.png"
    plt.savefig(filename1)
    print(f"📊 Saved: {filename1}")

    # === Plot: Average Latency Over Time ===
    avg_latency_by_second = df_merged.groupby("timestamp_second")["preprocess_until_poll"].mean()
    plt.figure(figsize=(10, 6))
    avg_latency_by_second.plot(marker='o')
    plt.xlabel("Unix Timestamp (second)")
    plt.ylabel("Average Latency (s)")
    plt.title(f"Avg Latency Over Time\n{title_info}")
    plt.grid(True)
    plt.tight_layout()
    filename2 = f"{output_prefix}_avg_latency_time_{operating_system}_{online_store}_{mode}_{eps}eps_{interval}s_{rows}rows_{input_features}in_{output_features}out.png"
    plt.savefig(filename2)
    print(f"📈 Saved: {filename2}")

def compute_latency_stats(csv_path, column):
    df = pd.read_csv(csv_path, sep=";")

    # Clean data: replace comma with dot for float conversion, drop negatives
    df[column] = df[column].astype(str).str.replace(",", ".").astype(float)
    df = df[df[column] >= 0]

    latencies = df[column]

    stats = {
        "min": latencies.min(),
        "mean": latencies.mean(),
        "p50": latencies.median(),
        "p90": latencies.quantile(0.9),
        "p95": latencies.quantile(0.95),
        "p99": latencies.quantile(0.99),
        "max": latencies.max(),
    }
    return stats, df


def plot_latency_stats(stats, is_grouped, eps, interval, rows, input_features, output_features, online_store="redis",operating_system="linux",output_file=None):
    labels = list(stats.keys())
    values = list(stats.values())

    x = np.arange(len(labels))

    plt.figure(figsize=(10, 6))
    bars = plt.bar(x, values, width=0.5)

    plt.xticks(x, labels)
    plt.ylabel("Latency (seconds)")
    poll_mode = "Grouped Poll" if is_grouped else "Single Poll"
    title = f"{poll_mode} — {eps} EPS, {interval}s Interval, {rows} Rows, {input_features} Input Features, {output_features} Output Features"
    plt.title(title)
    print(title)

    for i, bar in enumerate(bars):
        yval = bar.get_height()
        plt.text(bar.get_x() + bar.get_width() / 2, yval + 0.01, f"{yval:.3f}", ha='center', va='bottom')

    plt.tight_layout()
    plt.grid(axis='y', linestyle='--', alpha=0.7)

    if output_file is None:
        mode = "grouped" if is_grouped else "single"
        output_file = f"lat_{operating_system}_{online_store}_{mode}_{eps}eps_{interval}s_{rows}rows_{input_features}in_{output_features}out.png"
    full_path = os.path.join("plots", output_file)
    plt.savefig(full_path)
    plt.show()


def plot_latency_over_time(df, is_grouped, eps, interval, rows, input_features, output_features, online_store="redis",operating_system="linux" ,output_file=None):
    df = df[df["preprocess_until_poll"] >= 0].copy()
    df["spark_ingestion_time"] = df["spark_ingestion_time"].astype(str).str.replace(",", ".").astype(float)
    df["spark_ingestion_dt"] = df["spark_ingestion_time"].apply(datetime.fromtimestamp)

    plt.figure(figsize=(12, 6))
    plt.plot(df["spark_ingestion_dt"], df["preprocess_until_poll"], linestyle='-', marker='.', alpha=0.5)
    plt.xlabel("Spark Ingestion Time")
    plt.ylabel("Latency (seconds)")
    poll_mode = "Grouped Poll" if is_grouped else "Single Poll"
    title = f"{poll_mode} — Latency Over Time ({eps} EPS, {interval}s Interval)"
    plt.title(title)
    plt.grid(True)

    if output_file is None:
        mode = "grouped" if is_grouped else "single"
        output_file = f"time_{operating_system}_{online_store}_{mode}_{eps}eps_{interval}s_{rows}rows_{input_features}in_{output_features}out.png"

    plt.tight_layout()
    full_path = os.path.join("plots", output_file)
    plt.savefig(full_path)
    plt.show()

if __name__ == "__main__":
    # ==== MAIN ====
    operating_system = "linux"
    csv_path = "merged_log.csv"
    column = "preprocess_until_poll"
    eps = int(os.getenv("ENTITY_PER_SECOND"))
    interval = int(os.getenv("PROCESSING_INTERVAL"))
    rows = int(os.getenv("ROWS"))
    input_features = int(os.getenv("FEATURES"))
    output_features = input_features
    is_grouped = True
    online_store = os.getenv("ONLINE_STORE")
    latency_stats, df_filtered = compute_latency_stats(csv_path, column)
    #
    # plot_latency_stats(latency_stats, is_grouped, eps, interval, rows, input_features, output_features,online_store,operating_system)
    # plot_latency_over_time(df_filtered, is_grouped, eps, interval, rows, input_features, output_features,online_store,operating_system)
    analyze_thread_stats_vs_latency(
        thread_csv="../logs/thread_request_stats.csv",
        latency_csv=csv_path,
        is_grouped=is_grouped,
        eps=eps,
        interval=interval,
        rows=rows,
        input_features=input_features,
        output_features=output_features,
        online_store=online_store,
        operating_system=operating_system
    )