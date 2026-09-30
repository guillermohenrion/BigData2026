#!/usr/bin/env bash
# Baja el cluster y borra los volúmenes (HDFS vacío la próxima vez).
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose --profile scale down -v
