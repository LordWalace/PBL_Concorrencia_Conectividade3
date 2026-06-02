#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

container=""
if [[ $# -ge 1 ]]; then
  input="$1"
  normalized=$(echo "$input" | tr '[:upper:]' '[:lower:]')
  case "$normalized" in
    norte|sul|leste|oeste)
      container="gateway-$normalized"
      ;;
    gateway-norte|gateway-sul|gateway-leste|gateway-oeste)
      container="$normalized"
      ;;
    *)
      container="$input"
      ;;
  esac
else
  if [[ -f .env ]]; then
    sector=$(grep -E '^(SETOR_ID|GATEWAY_ID)(_|=)' .env | head -n1 | cut -d'=' -f2 | tr -d '[:space:]')
    if [[ -n "$sector" ]]; then
      sector=$(echo "$sector" | tr '[:upper:]' '[:lower:]')
      case "$sector" in
        norte|sul|leste|oeste)
          container="gateway-$sector"
          ;;
        *)
          container=""
          ;;
      esac
    fi
  fi

  if [[ -z "$container" ]]; then
    read -rp "Nome ou ID do contêiner do gateway a ser derrubado: " container
  fi
fi

if [[ -z "$container" ]]; then
  echo "Uso: $0 <container-name|container-id>"
  exit 1
fi

if ! docker ps -a --format '{{.Names}}' | grep -qx "$container" && ! docker ps -a --format '{{.ID}}' | grep -qx "$container"; then
  echo "Contêiner '$container' não foi encontrado."
  exit 1
fi

docker rm -f "$container"

echo "Contêiner de gateway '$container' removido com sucesso."