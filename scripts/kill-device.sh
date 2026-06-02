#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

device=""
if [[ $# -ge 1 ]]; then
  input="$1"
  normalized=$(echo "$input" | tr '[:upper:]' '[:lower:]')
  case "$normalized" in
    norte|sul|leste|oeste)
      device="drone-$normalized"
      ;;
    drone-norte|drone-sul|drone-leste|drone-oeste)
      device="$normalized"
      ;;
    *)
      device="$input"
      ;;
  esac
else
  if [[ -f .env ]]; then
    sector=$(grep -E '^(SETOR_ID|GATEWAY_ID)(_|=)' .env | head -n1 | cut -d'=' -f2 | tr -d '[:space:]')
    if [[ -n "$sector" ]]; then
      sector=$(echo "$sector" | tr '[:upper:]' '[:lower:]')
      case "$sector" in
        norte|sul|leste|oeste)
          device="drone-$sector"
          ;;
        *)
          device=""
          ;;
      esac
    fi
  fi

  if [[ -z "$device" ]]; then
    read -rp "Nome ou ID do contêiner do drone a ser derrubado: " device
  fi
fi

if [[ -z "$device" ]]; then
  echo "Uso: $0 <container-name|container-id>"
  exit 1
fi

if ! docker ps --format '{{.Names}}' | grep -qx "$device" && ! docker ps --format '{{.ID}}' | grep -qx "$device"; then
  echo "Contêiner '$device' não está em execução ou não foi encontrado."
  exit 1
fi

docker kill "$device"

echo "Contêiner de drone '$device' derrubado com sucesso."