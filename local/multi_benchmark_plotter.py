import os
import re
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

BENCHMARK_ROOT = os.path.expanduser("~/benchmark_results")
OUTPUT_DIR = os.path.join(BENCHMARK_ROOT, "comparative_plots")
print(OUTPUT_DIR)
LATENCY_COLUMN = "preprocess_until_poll"

import seaborn as sns
def plot_latency_vs_eps_by_store(mode="same_duration"):
    """
    Plot latency metrics vs EPS for each online store.
    Modes:
        - "same_duration": groups by interval + duration + features
        - "same_rows":     groups by interval + rows + features
    """
    latest_benchmarks = load_latest_benchmarks(BENCHMARK_ROOT)
    from collections import defaultdict

    grouped = defaultdict(lambda: defaultdict(dict))  # key → eps → store → stats

    for key, run in latest_benchmarks.items():
        meta = run["meta"]
        interval = int(meta["interval"])
        eps = int(meta["eps"])
        rows = int(meta["rows"])
        features = int(meta["features"])
        store = meta["store"]

        if mode == "same_duration":
            duration = rows // eps
            group_key = f"{interval}s_{duration}s_{features}f"
        elif mode == "same_rows":
            group_key = f"{interval}s_{rows}rows_{features}f"
        else:
            raise ValueError(f"Unsupported mode: {mode}")

        merged_csv = os.path.join(run["path"], "merged_log.csv")
        if os.path.exists(merged_csv):
            stats = compute_latency_stats(merged_csv)
            grouped[group_key][eps][store] = stats

    for group_key, eps_dict in grouped.items():
        sorted_eps = sorted(eps_dict.keys())
        all_stores = sorted({store for eps_stats in eps_dict.values() for store in eps_stats})

        for metric in ["min", "mean", "p50", "p90", "p95", "p99", "max"]:
            plt.figure(figsize=(12, 6))

            for store in all_stores:
                y = [eps_dict[eps].get(store, {}).get(metric, np.nan) for eps in sorted_eps]
                plt.plot(sorted_eps, y, marker='o', label=store)

            plt.xlabel("EPS (Entities per Second)")
            plt.ylabel(f"Latency ({metric}) [ms]")
            mode_label = "Same Duration" if mode == "same_duration" else "Same Row Count"
            plt.title(f"Latency ({metric}) vs EPS — {mode_label} — {group_key}")
            plt.grid(True)
            plt.legend()
            plt.tight_layout()

            suffix = "duration" if mode == "same_duration" else "rows"
            output_file = os.path.join(
                OUTPUT_DIR,
                f"latency_metric_{metric}_{suffix}_{group_key}.png"
            )
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            plt.savefig(output_file)
            print(f"📊 Saved EPS-{suffix} comparison: {output_file}")
            plt.close()


def plot_violin_latency_by_store(benchmarks):
    violin_dir = os.path.join(OUTPUT_DIR, "violin_plots")
    os.makedirs(violin_dir, exist_ok=True)

    # Group by same EPS + ROWS + FEATURES
    grouped = {}
    for key, run in benchmarks.items():
        m = run["meta"]
        group_key = f'{m["eps"]}eps_{m["interval"]}s_{m["rows"]}rows_{m["features"]}f'
        grouped.setdefault(group_key, []).append(run)

    for name, group in grouped.items():
        if len(group) < 2:
            continue

        df_all = []
        for run in group:
            csv_path = os.path.join(run["path"], "merged_log.csv")
            if not os.path.exists(csv_path):
                continue

            df = pd.read_csv(csv_path, sep=";")
            if LATENCY_COLUMN not in df:
                continue

            df[LATENCY_COLUMN] = df[LATENCY_COLUMN].astype(str).str.replace(",", ".").astype(float)
            df = df[df[LATENCY_COLUMN] >= 0]
            df["latency_ms"] = df[LATENCY_COLUMN] * 1000
            df["store"] = run["meta"]["store"]
            df_all.append(df[["latency_ms", "store"]])

        if df_all:
            all_df = pd.concat(df_all)

            plt.figure(figsize=(10, 6))
            sns.violinplot(data=all_df, x="store", y="latency_ms", inner="quartile", palette="muted")
            plt.title(f"Latency Distribution by Online Store — {name}")
            plt.ylabel("Latency (ms)")
            plt.xlabel("Online Store")
            plt.grid(True)
            plt.tight_layout()

            filename = f"violin_latency_store_{name}.png"
            plt.savefig(os.path.join(violin_dir, filename))
            plt.close()
            print(f"🎻 Saved violin plot: {filename}")
def plot_latency_vs_features(benchmarks, metric="mean"):
    feature_dir = os.path.join(OUTPUT_DIR, "latency_vs_features")
    os.makedirs(feature_dir, exist_ok=True)

    # Group by store + eps + rows
    grouped = {}
    for key, run in benchmarks.items():
        m = run["meta"]
        group_key = f'{m["store"]}_{m["eps"]}eps_{m["interval"]}s_{m["rows"]}rows'
        grouped.setdefault(group_key, []).append(run)

    for name, group in grouped.items():
        features = []
        values = []
        for run in sorted(group, key=lambda r: int(r["meta"]["features"])):
            csv_path = os.path.join(run["path"], "merged_log.csv")
            if not os.path.exists(csv_path):
                continue

            stats = compute_latency_stats(csv_path)
            features.append(int(run["meta"]["features"]))
            values.append(stats.get(metric, None))

        if features and values:
            plt.figure(figsize=(8, 5))
            plt.plot(features, values, marker="o")
            plt.title(f"{metric.capitalize()} Latency vs. Feature Count — {name}")
            plt.xlabel("Number of Features")
            plt.ylabel("Latency (ms)")
            plt.grid(True)
            plt.tight_layout()

            filename = f"latency_vs_features_{metric}_{name}.png"
            plt.savefig(os.path.join(feature_dir, filename))
            plt.close()
            print(f"📈 Saved feature-latency plot: {filename}")

def plot_latency_distribution_per_benchmark(benchmarks):
    dist_dir = os.path.join(OUTPUT_DIR, "distributions")
    os.makedirs(dist_dir, exist_ok=True)

    for key, run in benchmarks.items():
        csv_path = os.path.join(run["path"], "merged_log.csv")
        if not os.path.exists(csv_path):
            continue

        df = pd.read_csv(csv_path, sep=";")
        if LATENCY_COLUMN not in df:
            continue

        df[LATENCY_COLUMN] = df[LATENCY_COLUMN].astype(str).str.replace(",", ".").astype(float)
        df = df[df[LATENCY_COLUMN] >= 0]
        latencies_ms = df[LATENCY_COLUMN] * 1000

        # Compute key percentiles
        stats = {
            "mean": latencies_ms.mean(),
            "p90": latencies_ms.quantile(0.9),
            "p99": latencies_ms.quantile(0.99)
        }

        plt.figure(figsize=(10, 6))
        sns.histplot(latencies_ms, bins=50, kde=True, color="steelblue", alpha=0.6)

        for label, val in stats.items():
            plt.axvline(val, linestyle="--", label=f"{label}: {val:.1f} ms")

        plt.xlabel("Latency (ms)")
        plt.ylabel("Frequency (Vorkommen)")
        meta = run["meta"]
        title = f"{meta['store']} — {meta['eps']} EPS, {meta['features']}F, {meta['rows']} rows"
        plt.title(title)
        plt.legend()
        plt.grid(True)
        plt.tight_layout()

        filename = f"latency_dist_{meta['store']}_{meta['eps']}eps_{meta['features']}f_{meta['rows']}rows.png"
        plt.savefig(os.path.join(dist_dir, filename))
        plt.close()
        print(f"📉 Distribution plotted: {filename}")

# compute latency in ms
def compute_latency_stats(csv_path, column="preprocess_until_poll"):
    df = pd.read_csv(csv_path, sep=";")
    df[column] = df[column].astype(str).str.replace(",", ".").astype(float)
    df = df[df[column] >= 0]
    latencies = df[column] * 1000
    return {
        "min": latencies.min(),
        "mean": latencies.mean(),
        "p50": latencies.median(),
        "p90": latencies.quantile(0.9),
        "p95": latencies.quantile(0.95),
        "p99": latencies.quantile(0.99),
        "max": latencies.max(),
    }

def parse_benchmark_folder_name(name):
    # Updated regex: makes the optional prefix (e.g., "localbranch") truly optional
    pattern = r"(?:(?P<prefix>[^_]+)_)?(?P<store>[^_]+)_(?P<eps>\d+)eps_(?P<interval>\d+)s_(?P<rows>\d+)rows_(?P<features>\d+)f_(?P<ts>\d{8}_\d{6})"
    match = re.match(pattern, name)
    if not match:
        return None
    info = match.groupdict()
    info["prefix"] = info["prefix"] or ""  # fallback to empty if no prefix
    info["timestamp"] = datetime.strptime(info["ts"], "%Y%m%d_%H%M%S")
    return info


def load_latest_benchmarks(root):
    benchmarks_by_key = {}
    for entry in os.listdir(root):
        full_path = os.path.join(root, entry)
        if not os.path.isdir(full_path):
            continue
        meta = parse_benchmark_folder_name(entry)
        if not meta:
            continue
        key = f'{meta["store"]}_{meta["eps"]}eps_{meta["interval"]}s_{meta["rows"]}rows_{meta["features"]}f'

        existing = benchmarks_by_key.get(key)
        if not existing or meta["timestamp"] > existing["meta"]["timestamp"]:
            benchmarks_by_key[key] = {"meta": meta, "path": full_path}
    return benchmarks_by_key


def group_for_comparison(latest_benchmarks):
    group_by_store = {}
    group_by_eps = {}

    for key, data in latest_benchmarks.items():
        m = data["meta"]
        base_key = f'{m["prefix"]}_{m["interval"]}s_{m["rows"]}rows_{m["features"]}f'
        store_variant_key = f'{base_key}_{m["eps"]}eps'
        eps_variant_key = f'{base_key}_{m["store"]}'

        group_by_store.setdefault(store_variant_key, []).append(data)
        group_by_eps.setdefault(eps_variant_key, []).append(data)

    return group_by_store, group_by_eps





def plot_comparison_bar(stats_by_label, title, output_path):
    plt.figure(figsize=(12, 6))
    metrics = list(next(iter(stats_by_label.values())).keys())
    x = np.arange(len(metrics))
    width = 0.15

    for i, (label, stats) in enumerate(stats_by_label.items()):
        vals = list(stats.values())
        plt.bar(x + i * width, vals, width, label=label)

    plt.xticks(x + width, metrics)
    plt.ylabel("Latency (milliseconds)")
    plt.title(title)
    plt.grid(True)
    plt.legend()
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    plt.tight_layout()
    plt.savefig(output_path)
    print(f"✅ Saved comparison plot: {output_path}")
    plt.close()


def export_stats_to_csv(stats_by_label, output_csv):
    df = pd.DataFrame.from_dict(stats_by_label, orient="index")
    df.index.name = "benchmark"
    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
    df.to_csv(output_csv, sep=";")
    print(f"📄 Exported latency stats to: {output_csv}")



def run_comparative_plotting():
    benchmarks = load_latest_benchmarks(BENCHMARK_ROOT)
    store_groups, eps_groups = group_for_comparison(benchmarks)

    for name, group in store_groups.items():
        if len(group) < 2:
            continue
        stats = {}
        for run in group:
            label = run["meta"]["store"]
            merged_csv = os.path.join(run["path"], "merged_log.csv")
            if os.path.exists(merged_csv):
                stats[label] = compute_latency_stats(merged_csv)
        if stats:
            plot_path = os.path.join(OUTPUT_DIR, f"latency_store_{name}.png")
            plot_comparison_bar(stats, f"Latency by Online Store — {name}", plot_path)

            csv_path = os.path.join(OUTPUT_DIR, f"latency_store_{name}.csv")
            export_stats_to_csv(stats, csv_path)

    for name, group in eps_groups.items():
        if len(group) < 2:
            continue
        stats = {}
        for run in group:
            label = f'{run["meta"]["eps"]} EPS'
            merged_csv = os.path.join(run["path"], "merged_log.csv")
            if os.path.exists(merged_csv):
                stats[label] = compute_latency_stats(merged_csv)
        if stats:
            print(OUTPUT_DIR)
            plot_path = os.path.join(OUTPUT_DIR, f"latency_eps_{name}.png")
            plot_comparison_bar(stats, f"Latency by EPS — {name}", plot_path)

            csv_path = os.path.join(OUTPUT_DIR, f"latency_eps_{name}.csv")
            export_stats_to_csv(stats, csv_path)

def plot_latency_vs_features_combined(benchmarks, metric="mean"):
    feature_dir = os.path.join(OUTPUT_DIR, "latency_vs_features_combined")
    os.makedirs(feature_dir, exist_ok=True)

    # Group by identical eps + interval + rows (excluding store + features)
    from collections import defaultdict
    config_groups = defaultdict(lambda: defaultdict(list))  # config_key → store → runs

    for key, run in benchmarks.items():
        m = run["meta"]
        config_key = f'{m["eps"]}eps_{m["interval"]}s_{m["rows"]}rows'
        store = m["store"]
        config_groups[config_key][store].append(run)

    for config_key, store_runs in config_groups.items():
        # Only include stores with sufficient data points
        valid_stores = {store: runs for store, runs in store_runs.items() if len(runs) >= 3}
        if len(valid_stores) < 2:
            continue  # skip if too few stores

        plt.figure(figsize=(10, 6))
        for store, runs in valid_stores.items():
            features = []
            values = []
            for run in sorted(runs, key=lambda r: int(r["meta"]["features"])):
                csv_path = os.path.join(run["path"], "merged_log.csv")
                if not os.path.exists(csv_path):
                    continue
                stats = compute_latency_stats(csv_path)
                feature_count = int(run["meta"]["features"])
                value = stats.get(metric)
                if value is not None:
                    features.append(feature_count)
                    values.append(value)

            if features and values:
                plt.plot(features, values, marker="o", label=store)

        plt.xlabel("Number of Features")
        plt.ylabel(f"Latency ({metric}) [ms]")
        plt.title(f"{metric.capitalize()} Latency vs Features — {config_key}")
        plt.grid(True)
        plt.legend()
        plt.tight_layout()

        filename = f"latency_vs_features_combined_{metric}_{config_key}.png"
        plt.savefig(os.path.join(feature_dir, filename))
        plt.close()
        print(f"📈 Combined plot saved: {filename}")

from matplotlib.dates import DateFormatter, AutoDateLocator

from matplotlib.dates import AutoDateLocator, DateFormatter
from matplotlib.ticker import MaxNLocator

def plot_throughput_vs_latency_all(benchmarks, output_dir="throughput_vs_latency"):
    os.makedirs(os.path.join(OUTPUT_DIR, output_dir), exist_ok=True)

    for key, run in benchmarks.items():
        merged_csv_path = os.path.join(run["path"], "merged_log.csv")
        if not os.path.exists(merged_csv_path):
            continue

        try:
            df = pd.read_csv(merged_csv_path, sep=";")
            if "preprocess_until_poll" not in df or "retrieval_timestamp" not in df:
                continue

            df["retrieval_timestamp"] = df["retrieval_timestamp"].astype(str).str.replace(",", ".").astype(float)
            df["preprocess_until_poll"] = df["preprocess_until_poll"].astype(str).str.replace(",", ".").astype(float)
            df = df[df["preprocess_until_poll"] >= 0]

            df["retrieval_dt"] = pd.to_datetime(df["retrieval_timestamp"], unit="s")
            df["timestamp_sec"] = df["retrieval_dt"].dt.floor("S")

            grouped = df.groupby("timestamp_sec").agg(
                avg_latency=("preprocess_until_poll", lambda x: x.mean() * 1000),
                throughput=("entity_id", "count")
            ).reset_index()

            if grouped.empty:
                continue

            fig, ax1 = plt.subplots(figsize=(12, 6))

            # Primary Y-axis (latency)
            ax1.plot(grouped["timestamp_sec"], grouped["avg_latency"], color="blue", label="Ø Latency (ms)")
            ax1.set_ylabel("Average Latency (ms)", color="blue")
            ax1.tick_params(axis="y", labelcolor="blue")
            ax1.grid(True)

            # X-axis formatting
            ax1.set_xlabel("Time (HH:MM:SS)")
            ax1.set_xlim(grouped["timestamp_sec"].min(), grouped["timestamp_sec"].max())
            ax1.xaxis.set_major_locator(MaxNLocator(nbins=10))  # ~10 ticks max
            ax1.xaxis.set_major_formatter(DateFormatter("%H:%M:%S"))
            fig.autofmt_xdate()

            # Secondary Y-axis (throughput)
            ax2 = ax1.twinx()
            bar_width = Timedelta(seconds=1)  # width matches one second
            ax2.bar(grouped["timestamp_sec"], grouped["throughput"], width=bar_width, align="center", color="gray",
                    alpha=0.3)
            ax2.set_ylabel("Throughput (entities/sec)", color="gray")
            ax2.tick_params(axis="y", labelcolor="gray")

            # Title and save
            plt.title(f"Throughput vs Avg Latency — {run['meta']['store']} — {run['meta']['eps']}eps, {run['meta']['features']}F")
            fig.tight_layout()

            filename = f"throughput_vs_latency_{run['meta']['store']}_{run['meta']['eps']}eps_{run['meta']['features']}f.png"
            plt.savefig(os.path.join(OUTPUT_DIR, output_dir, filename))
            plt.close()
            print(f"📈 Saved throughput-latency plot: {filename}")

        except Exception as e:
            print(f"⚠️ Error while processing {merged_csv_path}: {e}")


if __name__ == "__main__":
    run_comparative_plotting()
    benchmarks = load_latest_benchmarks(BENCHMARK_ROOT)
    plot_latency_distribution_per_benchmark(benchmarks)
    plot_violin_latency_by_store(benchmarks)
    plot_latency_vs_eps_by_store(mode="same_duration")
    plot_latency_vs_eps_by_store(mode="same_rows")
    for metric in ["min", "mean", "p50", "p90", "p95", "p99"]:
        plot_latency_vs_features(benchmarks, metric)