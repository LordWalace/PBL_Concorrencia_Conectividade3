#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

echo "Limpando execuções anteriores..."
docker compose -f client/docker-compose.yml down >/dev/null 2>&1 || true

echo "Iniciando painel de operação..."
docker compose -f client/docker-compose.yml run --rm client
