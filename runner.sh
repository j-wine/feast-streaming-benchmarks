#!/bin/bash
set -e

# --- CONFIG ---
CREDENTIALS_PATH=~/application_default_credentials.json
RESULTS_ROOT=~/benchmark_results
REPO_BASE=feast-streaming-benchmarks
VENV_PATH=~/feastbench
DRY_RUN=${DRY_RUN:-false}
BRANCHES=(
  automated-bigtable-gcp
  automated-dragonfly
  automated-redis
  automated-mysql
  automated-postgres
)

BENCHMARK_CONFIGS=(
  "100 1 600 10"
  "100 1 1200 10"
  "100 1 1800 10"
  "100 1 600 100"
  "100 1 1200 100"
  "100 1 1800 100"
  "100 1 600 250"
  "100 1 1200 250"
  "100 1 1800 250"
  "500 1 3000 10"
  "500 1 3000 100"
  "500 1 3000 250"
  "1000 1 6000 10"
  "1000 1 6000 100"
  "1000 1 6000 250"
  "2500 1 15000 10"
  "2500 1 15000 100"
  "2500 1 15000 250"
)

wait_until_second() {
  target_second=$(( (60 + PROCESSING_START - 30) % 60 ))
  current_second=$(date +%S)
  current_second=$((10#$current_second))
  seconds_to_wait=$(( (60 + target_second - current_second) % 60 ))

  if (( seconds_to_wait < 5 )); then
    seconds_to_wait=$((seconds_to_wait + 60))
  fi

  echo "🕒 Waiting $seconds_to_wait seconds before starting benchmark containers..."
  sleep "$seconds_to_wait"
}

echo "[1] Activating Python virtualenv..."
source "$VENV_PATH/bin/activate"

mkdir -p "$RESULTS_ROOT"

for BRANCH in "${BRANCHES[@]}"; do
  REPO_DIR="${REPO_BASE}-${BRANCH}"
  echo "📦 Entering branch: $BRANCH"
  cd ~/"$REPO_DIR"

  set -o allexport
  source .env
  set +o allexport
  PROCESSING_START=${PROCESSING_START:-30}

  echo "[2] Generating parquet files..."
  python generate_parquet_files.py

  if [[ "$BRANCH" == *"bigtable"* && -f "$CREDENTIALS_PATH" ]]; then
    echo "[3] Copying GCP credentials..."
    for svc in streaming/kafka_consumer streaming/kafka_producer streaming/spark_processor; do
      cp "$CREDENTIALS_PATH" "$svc/application_default_credentials.json"
    done
  fi

  for config in "${BENCHMARK_CONFIGS[@]}"; do
    read EPS INTERVAL ROWS FEATURES <<< "$config"

    cat > .env <<EOF
OPERATING_SYSTEM=$OPERATING_SYSTEM
MACHINE=$MACHINE
ONLINE_STORE=$ONLINE_STORE
ENTITY_PER_SECOND=$EPS
PROCESSING_INTERVAL=$INTERVAL
ROWS=$ROWS
FEATURES=$FEATURES
PROCESSING_START=$PROCESSING_START
FEATURE_VIEW_NAME=stream_view_${FEATURES}in_${FEATURES}out
EOF

    echo "[RUN] $ONLINE_STORE — EPS=$EPS, INT=$INTERVAL, ROWS=$ROWS, FEAT=$FEATURES"

    if [[ "$DRY_RUN" == "true" ]]; then
      echo "🧪 [DRY RUN] Would run benchmark for $ONLINE_STORE — EPS=$EPS, FEAT=$FEATURES"
      continue
    fi

    set -o allexport
    source .env
    set +o allexport

    docker compose build
    docker compose up -d redis registry zookeeper broker-1 feature_server

    wait_until_second

    docker compose up -d kafka_producer kafka_consumer spark_ingestor
    docker wait kafka_consumer

    docker logs spark_ingestor >> logs/spark_log

    python local/log_merger.py
    python local/plotting.py

    timestamp=$(date +%Y%m%d_%H%M%S)
    results_dir="$RESULTS_ROOT/${ONLINE_STORE}_${EPS}eps_${INTERVAL}s_${ROWS}rows_${FEATURES}f_$timestamp"
    mkdir -p "$results_dir"
    cp logs/* "$results_dir/" || true
    cp local/merged_log.csv "$results_dir/" || true
    cp plots/* "$results_dir/" 2>/dev/null || true

    rm -f logs/*
    docker compose down --volumes
    echo "✅ Finished run: $results_dir"
    echo "------------------------------------------------------------"
  done

  echo "✅ All runs completed for branch: $BRANCH"
  echo ""
done

echo "🎉 All benchmarks finished across all branches."

echo ""
echo "📁 Result directories created:"
find "$RESULTS_ROOT" -type d -name "${ONLINE_STORE}_*" | sort
