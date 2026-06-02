#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

sector=""
if [[ $# -ge 1 ]]; then
  sector="$1"
else
  if [[ -f .env ]]; then
    sector=$(grep -E '^(GATEWAY_ID|SETOR_ID)(_|=)' .env | head -n1 | cut -d'=' -f2 | tr -d '[:space:]')
  fi
fi

if [[ -z "$sector" ]]; then
  echo "Uso: $0 [Norte|Sul|Leste|Oeste]"
  echo "Ou deixe .env com GATEWAY_ID/SETOR_ID definido e rode sem argumentos."
  exit 1
fi

sector=$(echo "$sector" | tr '[:upper:]' '[:lower:]')
case "$sector" in
  norte)
    beacon=beacon-norte
    gateway_var=IP_NORTE
    sector_name=Norte
    ;;
  sul)
    beacon=beacon-sul
    gateway_var=IP_SUL
    sector_name=Sul
    ;;
  leste)
    beacon=beacon-leste
    gateway_var=IP_LESTE
    sector_name=Leste
    ;;
  oeste)
    beacon=beacon-oeste
    gateway_var=IP_OESTE
    sector_name=Oeste
    ;;
  *)
    echo "Setor inválido: $sector"
    exit 1
    ;;
esac

if [[ ! -f .env ]]; then
  if [[ ! -f .env.example ]]; then
    echo "Arquivo .env.example não encontrado. Não é possível criar .env para o beacon."
    exit 1
  fi
  echo "Arquivo .env não encontrado. Criando .env a partir de .env.example..."
  cp .env.example .env
fi

get_env_value() {
  local key="$1"
  grep -E "^${key}=" .env | head -n1 | cut -d'=' -f2- | tr -d '[:space:]'
}

set_env_value() {
  local key="$1"
  local value="$2"
  if grep -qE "^${key}=" .env; then
    sed -i -e "s|^${key}=.*|${key}=${value}|" .env
  else
    echo "${key}=${value}" >> .env
  fi
}

gateway_ip=$(get_env_value "$gateway_var")
if [[ -z "$gateway_ip" ]]; then
  echo "Não foi possível encontrar ${gateway_var} em .env. Verifique o arquivo de ambiente."  >&2
  exit 1
fi

set_env_value "SETOR_ID" "$sector_name"
set_env_value "GATEWAY_IP" "$gateway_ip"
set_env_value "GATEWAY_TCP_REG_PORT" "$(get_env_value GATEWAY_TCP_REG_PORT)"

if [[ -z "$(get_env_value GATEWAY_TCP_REG_PORT)" ]]; then
  echo "GATEWAY_TCP_REG_PORT não encontrado em .env. Adicione-o ou ajuste .env.example." >&2
  exit 1
fi

echo "Iniciando beacon para o setor ${sector_name}..."
docker compose -f beacon/docker-compose.yml up -d --build "$beacon"

echo "Exibindo logs do contêiner ${beacon}..."
docker logs -f "$beacon"