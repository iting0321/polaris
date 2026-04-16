#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MARQUEZ_COMPOSE_FILE="${ROOT_DIR}/.tmp-marquez-compose.yml"
SPARK_COMPOSE_FILE="${ROOT_DIR}/.tmp-spark-compose.yml"

echo "Stopping Marquez stack"
if [[ -f "${MARQUEZ_COMPOSE_FILE}" ]]; then
  docker compose -f "${MARQUEZ_COMPOSE_FILE}" down -v || true
fi

echo "Stopping Spark stack"
if [[ -f "${SPARK_COMPOSE_FILE}" ]]; then
  docker compose -f "${SPARK_COMPOSE_FILE}" down -v || true
fi

echo "Removing generated compose files"
rm -f "${MARQUEZ_COMPOSE_FILE}" "${SPARK_COMPOSE_FILE}"

echo "Removing lingering launcher containers"
docker rm -f \
  polaris-marquez-web-1 \
  polaris-marquez-1 \
  polaris-postgres-1 \
  polaris-polaris-setup-1 \
  polaris-spark-sql-run-116d4ab03461 \
  2>/dev/null || true

echo "Stopping local Polaris process if still running"
killall java 2>/dev/null || true
pkill -f 'org.apache.polaris' 2>/dev/null || true
pkill -f 'gradlew run' 2>/dev/null || true

echo "Current listeners on launcher ports"
lsof -nP -iTCP:3000 -iTCP:5000 -iTCP:5001 -iTCP:8181 -iTCP:8182 -sTCP:LISTEN || true

echo "Remaining relevant containers"
docker ps -a --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}' | rg 'polaris|marquez|spark' || true
