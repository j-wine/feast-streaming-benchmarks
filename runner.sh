#!/bin/bash
set -e

# --- CONFIG ---
CREDENTIALS_PATH=~/application_default_credentials.json
RESULTS_ROOT=~/benchmark_results
REPO_BASE=feast-streaming-benchmarks
VENV_PATH=~/feastbench
DRY_RUN=${DRY_RUN:-false}
BRANCHES=(
  automated-dragonfly
  )

BENCHMARK_CONFIGS=(

  # Leichte Last (100 EPS)
  "100 100 30000 10"      # Best-Case: geringe Feature-Anzahl, minimalste Latenz
  "100 250 30000 10"
  "100 500 30000 10"
  "100 750 30000 10"
  "100 100 30000 250"     # Feature-Komplexitätsgrenze
  "100 500 30000 250"     # Feature-Grenze + hoher processing_time

  # Mittlere Last (500 EPS)
  "500 250 150000 50"     # moderate Feature-Anzahl, realistische Last
  "500 500 150000 250"    # maximale Feature-Anzahl in mittlerem Setting

  # ️ Hohe Last (1000 EPS)
  "1000 300 300000 50"    # etwas kürzeres Intervall, mittlere Komplexität
  "1000 700 300000 100"   # realistische Feature-Komplexität, angepasste Zeit
  "1000 1000 300000 250"  # Feature- und Last-Grenzbereich

  # Zwischenstufe (1500 EPS) → neu
  "1500 1000 450000 100"  # neue Stufe für Übergang Hoch → Extrem

  # Extreme Last (2000 EPS)
  "2000 1000 600000 50"   # harte Last, einfache Features
  "2000 1500 600000 100"  # mittlere Features, langer Intervall
  "2000 2000 600000 250"  # maximal belastet

  # Referenzpunkt für Vergleichbarkeit
  "1000 1000 300000 50"    # bereits bekannter Lauf, Vergleich zu Altläufen

  # Stresstest (kurzes Intervall, hohe Last)
  "1000 200 300000 50"    # Tests Mini-Batch-Stau/Feast-Bottlenecks gezielt
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

mkdir -p "$RESULTS_ROOT" || true

for BRANCH in "${BRANCHES[@]}"; do
  REPO_DIR="${REPO_BASE}-${BRANCH}"
  echo "📦 Entering branch: $BRANCH"
  cd ~/"$REPO_DIR"
  # Safely shut down containers and clean up volumes/networks
  docker compose down --volumes --remove-orphans || true
  # Global cleanup (should be outside specific Compose context)
  docker container prune -f
  docker network prune -f

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
    # --- Start online store service ---
    case "$ONLINE_STORE" in
      redis|dragonfly)
        docker compose up -d redis
        ;;
      postgres)
        echo "[preclean] Deleting PostgreSQL bind mount..."
        sudo find ./feast-postgres-online -mindepth 1 -delete || true
        mkdir -p ./feast-postgres-online || true
        docker compose up -d postgres_online
        ;;
      mysql)
        echo "[preclean] Deleting MySQL bind mount..."
        sudo find ./mysql_data -mindepth 1 -delete || true
        mkdir -p ./mysql_data || true
        docker compose up -d mysql
        ;;
      bigtable)
        echo "ℹ️ Skipping online store container for Bigtable (external service)"
        echo "[preclean]Deleting Bigtable table: feast_demo_local.benchmark_entity"
        ;;
      *)
        echo "❌ Unknown ONLINE_STORE: $ONLINE_STORE"
        exit 1
        ;;
    esac

    docker compose up -d registry zookeeper broker-1 feature_server

    wait_until_second

    docker compose up -d kafka_producer kafka_consumer spark_ingestor
    docker wait kafka_consumer

    docker logs spark_ingestor >> logs/spark_log
#    mkdir -p local/plots
#    python local/log_merger.py
#    python local/plotting.py

    timestamp=$(date +%Y%m%d_%H%M%S)
    results_dir="$RESULTS_ROOT/${ONLINE_STORE}_${EPS}eps_${INTERVAL}s_${ROWS}rows_${FEATURES}f_$timestamp"
    mkdir -p "$results_dir" || true
    cp logs/* "$results_dir/" || true
#    cp local/merged_log.csv "$results_dir/" || true
#    cp plots/* "$results_dir/" 2>/dev/null || true

    rm -f logs/*
    docker compose down --volumes
    docker volume prune  -f
    docker network prune -f
    if [[ "$BRANCH" == *"bigtable"* ]]; then
      gcloud bigtable tables delete feast_demo_local.benchmark_entity --instance=feastbigtable --quiet || echo "[warn] Bigtable table not found or already deleted. Skipping."
    fi
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
