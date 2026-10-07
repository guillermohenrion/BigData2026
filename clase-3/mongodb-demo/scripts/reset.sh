#!/usr/bin/env bash
# Baja los 3 nodos y Mongo Express y borra los volúmenes (replica set y datos desde cero).
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose down -v
