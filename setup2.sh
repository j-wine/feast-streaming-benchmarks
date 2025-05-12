# --- CONFIG ---
PYTHON_VERSION=3.10.13
REPO_URL=https://github.com/j-wine/feast-streaming-benchmarks.git
BRANCHES=(automated-bigtable-gcp automated-dragonfly automated-redis)
#  automated-mysql automated-postgres
VENV_NAME=feastbench
CURRENT_USER=$(whoami)
CREDENTIALS_PATH="/home/${CURRENT_USER}/application_default_credentials.json"

echo "[1] Installing Python $PYTHON_VERSION..."
cd /usr/src
sudo curl -O https://www.python.org/ftp/python/$PYTHON_VERSION/Python-$PYTHON_VERSION.tgz
sudo tar -xf Python-$PYTHON_VERSION.tgz
cd Python-$PYTHON_VERSION
sudo ./configure --enable-optimizations
sudo make -j$(nproc)
sudo make altinstall

# Create virtualenv
cd ~
echo "[2] Creating virtualenv..."
/usr/local/bin/python3.10 -m venv $VENV_NAME
source $VENV_NAME/bin/activate
which python
python --version
pip install --upgrade pip

# Clone and prep each repo
for BRANCH in "${BRANCHES[@]}"; do
  echo "[3] Cloning branch $BRANCH..."
  DIR="feast-streaming-benchmarks-$BRANCH"
  git clone --branch $BRANCH $REPO_URL "$DIR"
  cd "$DIR"

  echo "[5] Preparing folders..."
  mkdir -p logs local
  chmod a+rwx logs local



  if [[ "$BRANCH" == *"bigtable"* && -f "$CREDENTIALS_PATH" ]]; then
    echo "[6] Injecting GCP credentials for $BRANCH..."
    for svc in streaming/kafka_consumer feature_repo streaming/spark_processor; do
      cp "$CREDENTIALS_PATH" "$svc/application_default_credentials.json"
    done
  fi

  cd ..
done
# --- Install dependencies once from one repo ---
cd feast-streaming-benchmarks-automated-redis # or any branch folder
pip install -r local/requirements.txt
cd ~

echo "✅ Setup complete for all branches."
