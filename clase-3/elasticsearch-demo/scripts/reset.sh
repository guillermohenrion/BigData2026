#!/usr/bin/env bash
# Baja Elasticsearch y Kibana y borra el volumen (índices vacíos la próxima vez).
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose down -v
