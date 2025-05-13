#!/bin/bash
# project was cloned into ~
cd ~/feast-streaming-benchmarks
set -e
# --- Load repo-level .env ---
set -o allexport
source .env
set +o allexport

# --- CONFIG ---
CREDENTIALS_PATH=~/application_default_credentials.json
RESULTS_ROOT=~/benchmark_results

BENCHMARK_CONFIGS=(
  # EPS INTERVAL ROWS FEATURES
  "100 1 6000 10"
  "100 1 12000 10"
  "100 1 18000 10"
  "100 1 6000 100"
  "100 1 12000 100"
  "100 1 18000 100"
  "100 1 6000 250"
  "100 1 12000 250"
  "100 1 18000 250"

  "500 1 30000 10"
  "500 1 30000 100"
  "500 1 30000 250"

  "1000 1 60000 10"
  "1000 1 60000 100"
  "1000 1 60000 250"

  "2500 1 150000 10"
  "2500 1 150000 100"
  "2500 1 150000 250"
)

wait_until_second() {
  # give container up to 30 seconds startup time
  # If PROCESSING_START_SECOND=30, containers should start at :00

  target_second=$(( (60 + PROCESSING_START_SECOND - 30) % 60 ))
  now=$(date +%s)
  current_second=$(date +%S)
  seconds_to_wait=$(( (60 + target_second - current_second) % 60 ))

  # If we're too close, wait for the next cycle
  if (( seconds_to_wait < 5 )); then
    seconds_to_wait=$((seconds_to_wait + 60))
  fi

  echo "🕒 Waiting $seconds_to_wait seconds to align with container start time (target=$target_second, PROCESSING_START=$PROCESSING_START_SECOND)..."
  sleep "$seconds_to_wait"
}


# --- Ensure results root exists ---
mkdir -p "$RESULTS_ROOT"

# --- One-time parquet generation ---
echo "[1] Activating virtualenv and generating parquet files..."
#source redis/bin/activate
python generate_parquet_files.py

# --- Optional: GCP credentials injection ---
if [[ "$(git branch --show-current)" == *"bigtable"* ]]; then
  echo "[INFO] Copying GCP credentials..."
  for svc in streaming/kafka_consumer streaming/kafka_producer streaming/spark_processor; do
    cp "$CREDENTIALS_PATH" "$svc/application_default_credentials.json"
  done
fi

# --- Benchmark loop ---
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

  echo "[RUN] EPS=$EPS, INT=$INTERVAL, ROWS=$ROWS, FEAT=$FEATURES"

  set -o allexport
  source .env
  set +o allexport

  docker compose build

  echo "[INFO] Starting infrastructure services..."
  docker compose up -d registry mysql zookeeper broker-1 feature_server

  wait_until_second

  echo "[INFO] Starting benchmark containers at second $PROCESSING_START..."
  docker compose up -d kafka_producer kafka_consumer spark_ingestor

  docker wait kafka_consumer

  docker logs spark_ingestor >> logs/spark_log
  python local/log_merger.py
  python local/plotting.py

  timestamp=$(date +%Y%m%d_%H%M%S)
  results_dir="$RESULTS_ROOT/localbranch_${ONLINE_STORE}_${EPS}eps_${INTERVAL}s_${ROWS}rows_${FEATURES}f_$timestamp"
  mkdir -p "$results_dir"
  cp logs/* "$results_dir/" || true
  cp local/merged_log.csv "$results_dir/" || true
  cp plots/* "$results_dir/" 2>/dev/null || true

  rm -f logs/*
  docker compose down --volumes
  echo "✅ Completed: $results_dir"
  echo "------------------------------------------------------------"
done

echo "🎉 All benchmark configs completed for current branch."
