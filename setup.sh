#!/usr/bin/env bash
set -euo pipefail
sudo apt-get update
sudo apt-get install -y python3 python3-venv python3-pip make curl
if ! command -v docker >/dev/null || ! docker compose version >/dev/null 2>&1; then
  echo "Instale Docker + Compose: https://docs.docker.com/engine/install/debian/"
  echo "Depois: sudo usermod -aG docker \$USER  (e faça logout/login)"
fi
python3 -m venv .venv
.venv/bin/pip install -U pip
.venv/bin/pip install -r requirements.txt
echo "Pronto. Rode: make up"
