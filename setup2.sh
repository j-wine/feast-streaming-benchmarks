# --- CONFIG ---
PYTHON_VERSION=3.10.13
VENV_NAME=gcp
REPO_URL=https://github.com/j-wine/feast-streaming-benchmarks.git
REPO_BRANCH=benchmark-v3-timings-100-features-linux-bigtable-gcp

# google username folder contains file upload of credentials
CURRENT_USER=$(whoami)
CREDENTIALS_PATH="/home/${CURRENT_USER}/application_default_credentials.json"
# --- PYTHON INSTALL ---
echo "[3/9] Installing Python $PYTHON_VERSION from source..."
cd /usr/src
sudo curl -O https://www.python.org/ftp/python/$PYTHON_VERSION/Python-$PYTHON_VERSION.tgz
sudo tar -xf Python-$PYTHON_VERSION.tgz
cd Python-$PYTHON_VERSION
sudo ./configure --enable-optimizations
sudo make -j$(nproc)
sudo make altinstall

# --- CLONE REPO ---
echo "[4/9] Cloning benchmark repo..."
cd ~
git clone --branch $REPO_BRANCH $REPO_URL
cd feast-streaming-benchmarks

# --- PYTHON VENV SETUP ---
echo "[5/9] Creating virtualenv ($VENV_NAME) with Python 3.10..."
/usr/local/bin/python3.10 -m venv $VENV_NAME
source $VENV_NAME/bin/activate
which python
python --version
# --- FIX LOG PERMISSIONS ---
echo "[6/9] Setting log, plots directory permissions..."
mkdir -p logs local
chmod a+rwx logs local

# --- INSTALL REQUIREMENTS ---
echo "[6/9] Installing Python dependencies..."
cd local
pip install --upgrade pip
pip install -r requirements.txt
cd ..

# --- VALIDATE CREDENTIALS FILE ---
echo "[7/9] Validating GCP credentials file path..."
if [ ! -f "$CREDENTIALS_PATH" ]; then
  echo "❌ ERROR: GCP credentials file not found at $CREDENTIALS_PATH"
  exit 1
fi

 # --- COPY GCP CREDENTIALS INTO CONTAINERS ---
  echo "[9] Copying GCP credentials into container contexts..."
  for svc in streaming/kafka_consumer feature_repo streaming/spark_processor; do
    cp "$CREDENTIALS_PATH" "$svc/application_default_credentials.json"
  done