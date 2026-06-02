#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
sector="Norte"

if [[ ! -f .env ]]; then
  echo "Arquivo .env não encontrado. Crie o arquivo .env antes de rodar este script."
  exit 1
fi

docker compose -f gateway/docker-compose.yml -f beacon/docker-compose.yml -f device/docker-compose.yml up -d --build gateway-norte beacon-norte

echo "Mostrando logs de gateway-norte e beacon-norte..."
docker compose -f gateway/docker-compose.yml -f beacon/docker-compose.yml logs -f gateway-norte beacon-norte
