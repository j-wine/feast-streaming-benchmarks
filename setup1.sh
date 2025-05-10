#!/bin/bash

set -e  # Exit on any error

# --- CONFIG ---
PYTHON_VERSION=3.10.13
VENV_NAME=$ONLINE_STORE
REPO_URL=https://github.com/j-wine/feast-streaming-benchmarks.git
REPO_BRANCH=benchmark-v3-timings-100-features-linux-bigtable-gcp

#!/bin/bash

set -e  # Exit on any error

# --- PREP ---
echo "[1/9] Updating system and installing dependencies..."
sudo apt-get update
sudo apt-get install -y \
  build-essential \
  zlib1g-dev libncurses5-dev libgdbm-dev libnss3-dev \
  libssl-dev libsqlite3-dev libreadline-dev libffi-dev \
  libbz2-dev liblzma-dev curl git docker.io docker-compose

# --- DOCKER SETUP ---
echo "[2/9] Configuring Docker service and permissions..."
sudo usermod -aG docker $USER
sudo systemctl enable docker.service
sudo systemctl start docker.service
newgrp docker
