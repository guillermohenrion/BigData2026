#!/usr/bin/env bash
# Baja los 3 brokers y Kafka UI y borra los volúmenes (topics y offsets desde cero).
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose down -v
