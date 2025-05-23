#!/bin/bash
set -e  # Exit on any error

# --- PREP ---
echo "[1/9] Updating system and installing dependencies..."
sudo apt-get update
sudo apt-get install -y \
  build-essential \
  zlib1g-dev libncurses5-dev libgdbm-dev libnss3-dev \
  libssl-dev libsqlite3-dev libreadline-dev libffi-dev \
  libbz2-dev liblzma-dev curl git docker.io

# --- DOCKER SETUP ---
echo "[2/9] Installing Docker and Docker Compose v2..."

sudo usermod -aG docker $USER
sudo systemctl enable docker.service
sudo systemctl start docker.service

# Install Compose v2
mkdir -p ~/.docker/cli-plugins
curl -SL https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64 \
  -o ~/.docker/cli-plugins/docker-compose
chmod +x ~/.docker/cli-plugins/docker-compose

# Test version
docker compose version || echo "⚠️ Docker Compose v2 install failed"
