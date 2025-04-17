import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


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
    return stats


def plot_latency_stats(stats, title="Latency Stats (100 EPS, 1s Interval)", output_file="latency_stats.png"):
    labels = list(stats.keys())
    values = list(stats.values())

    x = np.arange(len(labels))

    plt.figure(figsize=(10, 6))
    bars = plt.bar(x, values, width=0.5)

    plt.xticks(x, labels)
    plt.ylabel("Latency (seconds)")
    plt.title(title)

    for i, bar in enumerate(bars):
        yval = bar.get_height()
        plt.text(bar.get_x() + bar.get_width() / 2, yval + 0.01, f"{yval:.3f}", ha='center', va='bottom')

    plt.tight_layout()
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.savefig(output_file)
    plt.show()


# === USAGE ===
csv_path = "merged_log.csv"  # Adjust if needed
column = "preprocess_until_poll"

latency_stats = compute_latency_stats(csv_path, column)
plot_latency_stats(latency_stats)