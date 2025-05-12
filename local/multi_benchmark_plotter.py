import os
import re
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np
from .plotting import compute_latency_stats

BENCHMARK_ROOT = os.path.expanduser("~/benchmark_results")
OUTPUT_DIR = os.path.join(BENCHMARK_ROOT, "comparative_plots")
print(OUTPUT_DIR)
LATENCY_COLUMN = "preprocess_until_poll"


def parse_benchmark_folder_name(name):
    # Format: localbranch_redis_2500eps_1s_25000rows_10f_20250509_233007
    pattern = r"(?P<prefix>.+?)_(?P<store>[^_]+)_(?P<eps>\d+)eps_(?P<interval>\d+)s_(?P<rows>\d+)rows_(?P<features>\d+)f_(?P<ts>\d{8}_\d{6})"
    match = re.match(pattern, name)
    if not match:
        return None
    info = match.groupdict()
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
        key = f'{meta["prefix"]}_{meta["store"]}_{meta["eps"]}eps_{meta["interval"]}s_{meta["rows"]}rows_{meta["features"]}f'
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


if __name__ == "__main__":
    run_comparative_plotting()
