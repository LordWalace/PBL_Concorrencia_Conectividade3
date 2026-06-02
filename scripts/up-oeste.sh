#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
sector="Oeste"

if [[ ! -f .env ]]; then
  echo "Arquivo .env não encontrado. Crie o arquivo .env antes de rodar este script."
  exit 1
fi

docker compose -f gateway/docker-compose.yml -f beacon/docker-compose.yml -f device/docker-compose.yml up -d --build gateway-oeste beacon-oeste

echo "Mostrando logs de gateway-oeste e beacon-oeste..."
docker compose -f gateway/docker-compose.yml -f beacon/docker-compose.yml logs -f gateway-oeste beacon-oeste
