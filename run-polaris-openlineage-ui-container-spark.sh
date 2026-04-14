#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -euo pipefail

ROOT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)

POLARIS_HOST="${POLARIS_HOST:-localhost}"
POLARIS_REALM="${POLARIS_REALM:-POLARIS}"
POLARIS_CLIENT_ID="${POLARIS_CLIENT_ID:-root}"
POLARIS_CLIENT_SECRET="${POLARIS_CLIENT_SECRET:-s3cr3t}"
POLARIS_PRODUCER_URI="${POLARIS_PRODUCER_URI:-http://localhost:8181}"
GRADLE_USER_HOME="${GRADLE_USER_HOME:-/tmp/gradle-home}"
CATALOG_NAME="${CATALOG_NAME:-manual_spark}"
KEEP_FAILED_STACK="${KEEP_FAILED_STACK:-1}"
MARQUEZ_PORT="${MARQUEZ_PORT:-5000}"

MARQUEZ_COMPOSE_FILE="${ROOT_DIR}/.tmp-marquez-compose.yml"
SPARK_COMPOSE_FILE="${ROOT_DIR}/.tmp-spark-compose.yml"
POLARIS_PID=""

docker_daemon_ready() {
  docker info >/dev/null 2>&1
}

require_docker_daemon() {
  if docker_daemon_ready; then
    return 0
  fi

  echo "Docker daemon is not reachable."
  echo "Current CLI context: $(docker context show 2>/dev/null || echo unknown)"
  echo "Expected socket: ${DOCKER_HOST:-unix:///Users/iting0321/.docker/run/docker.sock}"
  echo
  echo "Start Docker Desktop (or your Docker engine), wait for it to finish starting,"
  echo "then rerun this script."
  exit 1
}

cleanup() {
  local exit_code=$?

  if [[ ${exit_code} -ne 0 && "${KEEP_FAILED_STACK}" = "1" ]]; then
    if docker_daemon_ready && [[ -f "${MARQUEZ_COMPOSE_FILE}" ]]; then
      echo
      echo "Marquez startup failed. Logs:"
      docker compose -f "${MARQUEZ_COMPOSE_FILE}" logs --no-color || true
    fi
    if docker_daemon_ready; then
      echo
      echo "Failed stack preserved for debugging:"
      echo "  docker compose -f ${MARQUEZ_COMPOSE_FILE} ps"
      echo "  docker compose -f ${MARQUEZ_COMPOSE_FILE} logs --no-color"
      echo "  docker compose -f ${SPARK_COMPOSE_FILE} ps"
      echo "  docker compose -f ${SPARK_COMPOSE_FILE} logs --no-color"
    fi
    exit "${exit_code}"
  fi

  if [[ -f "${SPARK_COMPOSE_FILE}" ]]; then
    docker compose -f "${SPARK_COMPOSE_FILE}" down -v >/dev/null 2>&1 || true
    rm -f "${SPARK_COMPOSE_FILE}"
  fi

  if [[ -n "${POLARIS_PID}" ]] && kill -0 "${POLARIS_PID}" 2>/dev/null; then
    kill "${POLARIS_PID}" 2>/dev/null || true
    wait "${POLARIS_PID}" 2>/dev/null || true
  fi

  if [[ -f "${MARQUEZ_COMPOSE_FILE}" ]]; then
    docker compose -f "${MARQUEZ_COMPOSE_FILE}" down -v >/dev/null 2>&1 || true
    rm -f "${MARQUEZ_COMPOSE_FILE}"
  fi

  exit "${exit_code}"
}

trap cleanup EXIT INT TERM

require_docker_daemon

cat > "${MARQUEZ_COMPOSE_FILE}" <<'EOF'
services:
  postgres:
    image: postgres:14
    environment:
      POSTGRES_USER: marquez
      POSTGRES_PASSWORD: marquez
      POSTGRES_DB: marquez
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U marquez -d marquez"]
      interval: 5s
      timeout: 5s
      retries: 20

  marquez:
    image: marquezproject/marquez:0.51.1
    platform: linux/amd64
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      POSTGRES_HOST: postgres
      POSTGRES_PORT: 5432
      POSTGRES_DB: marquez
      POSTGRES_USER: marquez
      POSTGRES_PASSWORD: marquez
      SEARCH_ENABLED: "false"
      MARQUEZ_PORT: 5000
      MARQUEZ_ADMIN_PORT: 5001
    ports:
      - "5000:5000"
      - "5001:5001"
    healthcheck:
      test: ["CMD-SHELL", "wget -qO- http://localhost:5001/ping || exit 1"]
      interval: 5s
      timeout: 5s
      retries: 30

  marquez-web:
    image: marquezproject/marquez-web:0.51.1
    platform: linux/amd64
    depends_on:
      marquez:
        condition: service_healthy
    environment:
      MARQUEZ_HOST: marquez
      MARQUEZ_PORT: 5000
      WEB_PORT: 3000
    ports:
      - "3000:3000"
EOF

echo "Starting Marquez API and UI"
docker compose -f "${MARQUEZ_COMPOSE_FILE}" up -d

echo "Waiting for Marquez API on http://localhost:5000"
until curl --silent --fail "http://localhost:5001/ping" >/dev/null; do
  if ! docker compose -f "${MARQUEZ_COMPOSE_FILE}" ps marquez | grep -q "running"; then
    echo "Marquez container stopped before becoming healthy"
    exit 1
  fi
  sleep 2
done

echo "Starting Polaris locally with the HTTP OpenLineage event listener pointed at Marquez"
(
  cd "${ROOT_DIR}"
  env \
    GRADLE_USER_HOME="${GRADLE_USER_HOME}" \
    POLARIS_EVENT_LISTENER_TYPES="persistence-http-openlineage" \
    POLARIS_EVENT_LISTENER_PERSISTENCE_HTTP_OPENLINEAGE_ENDPOINT="http://127.0.0.1:5000/api/v1/lineage" \
    POLARIS_OPENLINEAGE_ENABLED="true" \
    POLARIS_OPENLINEAGE_PRODUCER="${POLARIS_PRODUCER_URI}" \
    ./gradlew run \
    -Dpolaris.bootstrap.credentials="${POLARIS_REALM},${POLARIS_CLIENT_ID},${POLARIS_CLIENT_SECRET}" \
    -Dpolaris.event-listener.types=persistence-http-openlineage \
    -Dpolaris.event-listener.persistence-http-openlineage.endpoint="http://127.0.0.1:5000/api/v1/lineage" \
    -Dpolaris.openlineage.enabled=true \
    -Dpolaris.openlineage.producer="${POLARIS_PRODUCER_URI}"
) &
POLARIS_PID=$!

echo "Waiting for Polaris health endpoint"
until curl --silent --fail "http://${POLARIS_HOST}:8182/q/health" >/dev/null; do
  sleep 2
done

export POLARIS_CLIENT_ID POLARIS_CLIENT_SECRET CATALOG_NAME

cat > "${SPARK_COMPOSE_FILE}" <<'EOF'
services:
  polaris-setup:
    image: alpine/curl:8.17.0
    extra_hosts:
      - "host.docker.internal:host-gateway"
    entrypoint: /bin/sh
    command:
      - -ec
      - |
        apk add --no-cache jq
        TOKEN=$$(curl -s http://host.docker.internal:8181/api/catalog/v1/oauth/tokens \
          --user ${POLARIS_CLIENT_ID}:${POLARIS_CLIENT_SECRET} \
          -d grant_type=client_credentials \
          -d scope=PRINCIPAL_ROLE:ALL | jq -r .access_token)
        curl --fail-with-body -s -X POST http://host.docker.internal:8181/api/management/v1/catalogs \
          -H "Authorization: Bearer $$TOKEN" \
          -H "Accept: application/json" \
          -H "Content-Type: application/json" \
          -d '{
                "catalog": {
                  "name": "${CATALOG_NAME}",
                  "type": "INTERNAL",
                  "readOnly": false,
                  "properties": {
                    "default-base-location": "file:///tmp/polaris/"
                  },
                  "storageConfigInfo": {
                    "storageType": "FILE",
                    "allowedLocations": [
                      "file:///tmp",
                      "file:///tmp/polaris/"
                    ]
                  }
                }
              }' || true
        curl --fail-with-body -s -X PUT http://host.docker.internal:8181/api/management/v1/catalogs/${CATALOG_NAME}/catalog-roles/catalog_admin/grants \
          -H "Authorization: Bearer $$TOKEN" \
          -H "Accept: application/json" \
          -H "Content-Type: application/json" \
          -d '{"type":"catalog","privilege":"TABLE_WRITE_DATA"}' >/dev/null
        curl --fail-with-body -s -X PUT http://host.docker.internal:8181/api/management/v1/principal-roles/service_admin/catalog-roles/${CATALOG_NAME} \
          -H "Authorization: Bearer $$TOKEN" \
          -H "Accept: application/json" \
          -H "Content-Type: application/json" \
          -d '{"name":"catalog_admin"}' >/dev/null

  spark-sql:
    image: apache/spark:3.5.8-java17-python3
    extra_hosts:
      - "host.docker.internal:host-gateway"
    depends_on:
      polaris-setup:
        condition: service_completed_successfully
    stdin_open: true
    tty: true
    ports:
      - "4040-4045:4040-4045"
    command:
      - /opt/spark/bin/spark-sql
      - --packages
      - org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1,org.apache.iceberg:iceberg-aws-bundle:1.10.1,org.apache.iceberg:iceberg-gcp-bundle:1.10.1,org.apache.iceberg:iceberg-azure-bundle:1.10.1
      - --conf
      - spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
      - --conf
      - spark.sql.catalog.polaris=org.apache.iceberg.spark.SparkCatalog
      - --conf
      - spark.sql.catalog.polaris.type=rest
      - --conf
      - spark.sql.catalog.polaris.warehouse=${CATALOG_NAME}
      - --conf
      - spark.sql.catalog.polaris.uri=http://host.docker.internal:8181/api/catalog
      - --conf
      - spark.sql.catalog.polaris.credential=${POLARIS_CLIENT_ID}:${POLARIS_CLIENT_SECRET}
      - --conf
      - spark.sql.catalog.polaris.scope=PRINCIPAL_ROLE:ALL
      - --conf
      - spark.sql.defaultCatalog=polaris
      - --conf
      - spark.sql.catalogImplementation=in-memory
      - --conf
      - spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp -Divy.home=/tmp
EOF

echo
echo "Ready:"
echo "  Polaris API: http://localhost:8181"
echo "  Marquez UI:  http://localhost:3000"
echo
echo "Starting containerized spark-sql"
echo "When spark-sql opens, run:"
echo "  CREATE NAMESPACE IF NOT EXISTS db1;"
echo "  CREATE TABLE db1.orders USING iceberg AS SELECT CAST(1 AS BIGINT) AS id, 'alice' AS customer, CAST(10.50 AS DECIMAL(10,2)) AS amount;"
echo "  INSERT INTO db1.orders VALUES (2, 'bob', CAST(20.00 AS DECIMAL(10,2)));"
echo "  ALTER TABLE db1.orders ADD COLUMN status STRING;"
echo
echo "Then refresh http://localhost:3000"
echo

docker compose -f "${SPARK_COMPOSE_FILE}" run --rm spark-sql
