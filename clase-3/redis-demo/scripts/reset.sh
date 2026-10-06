#!/usr/bin/env bash
# Baja Redis y RedisInsight y borra los volúmenes (datos y conexiones guardadas).
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose down -v
