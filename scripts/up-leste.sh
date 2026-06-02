#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
sector="Leste"

if [[ ! -f .env ]]; then
  echo "Arquivo .env não encontrado. Crie o arquivo .env antes de rodar este script."
  exit 1
fi

docker compose -f gateway/docker-compose.yml -f beacon/docker-compose.yml -f device/docker-compose.yml up -d --build gateway-leste beacon-leste

echo "Mostrando logs de gateway-leste e beacon-leste..."
docker compose -f gateway/docker-compose.yml -f beacon/docker-compose.yml logs -f gateway-leste beacon-leste
