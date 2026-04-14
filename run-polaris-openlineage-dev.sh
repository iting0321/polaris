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

OPENLINEAGE_HOST="${OPENLINEAGE_HOST:-127.0.0.1}"
OPENLINEAGE_PORT="${OPENLINEAGE_PORT:-5000}"
OPENLINEAGE_DIR="${OPENLINEAGE_DIR:-/tmp/polaris-openlineage}"
POLARIS_HOST="${POLARIS_HOST:-localhost}"
POLARIS_REALM="${POLARIS_REALM:-POLARIS}"
POLARIS_CLIENT_ID="${POLARIS_CLIENT_ID:-root}"
POLARIS_CLIENT_SECRET="${POLARIS_CLIENT_SECRET:-s3cr3t}"
POLARIS_PRODUCER_URI="${POLARIS_PRODUCER_URI:-http://localhost:8181}"
GRADLE_USER_HOME="${GRADLE_USER_HOME:-/tmp/gradle-home}"

RECEIVER_PID=""
POLARIS_PID=""

cleanup() {
  local exit_code=$?

  if [[ -n "${POLARIS_PID}" ]] && kill -0 "${POLARIS_PID}" 2>/dev/null; then
    kill "${POLARIS_PID}" 2>/dev/null || true
    wait "${POLARIS_PID}" 2>/dev/null || true
  fi

  if [[ -n "${RECEIVER_PID}" ]] && kill -0 "${RECEIVER_PID}" 2>/dev/null; then
    kill "${RECEIVER_PID}" 2>/dev/null || true
    wait "${RECEIVER_PID}" 2>/dev/null || true
  fi

  exit "${exit_code}"
}

trap cleanup EXIT INT TERM

mkdir -p "${OPENLINEAGE_DIR}"

echo "OpenLineage payloads will be written to ${OPENLINEAGE_DIR}"
echo "Starting local OpenLineage receiver on http://${OPENLINEAGE_HOST}:${OPENLINEAGE_PORT}/api/v1/lineage"

python3 - "${OPENLINEAGE_HOST}" "${OPENLINEAGE_PORT}" "${OPENLINEAGE_DIR}" <<'PY' &
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
import json
import sys

host = sys.argv[1]
port = int(sys.argv[2])
output_dir = Path(sys.argv[3])
output_dir.mkdir(parents=True, exist_ok=True)


class Handler(BaseHTTPRequestHandler):
    counter = 0

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        Handler.counter += 1
        path = output_dir / f"event-{Handler.counter:03d}.json"
        path.write_bytes(body)
        print(f"\n=== wrote {path} ===", flush=True)
        try:
            print(json.dumps(json.loads(body), indent=2), flush=True)
        except Exception:
            print(body.decode("utf-8", errors="replace"), flush=True)
        self.send_response(201)
        self.end_headers()
        self.wfile.write(b"created")

    def log_message(self, format, *args):
        pass


HTTPServer((host, port), Handler).serve_forever()
PY
RECEIVER_PID=$!

echo "Starting Polaris with the HTTP OpenLineage event listener enabled"

(
  cd "${ROOT_DIR}"
  env GRADLE_USER_HOME="${GRADLE_USER_HOME}" ./gradlew run \
    -Dpolaris.bootstrap.credentials="${POLARIS_REALM},${POLARIS_CLIENT_ID},${POLARIS_CLIENT_SECRET}" \
    -Dpolaris.event-listener.types=persistence-http-openlineage \
    -Dpolaris.event-listener.persistence-http-openlineage.endpoint="http://${OPENLINEAGE_HOST}:${OPENLINEAGE_PORT}/api/v1/lineage" \
    -Dpolaris.openlineage.enabled=true \
    -Dpolaris.openlineage.producer="${POLARIS_PRODUCER_URI}"
) &
POLARIS_PID=$!

echo "Waiting for Polaris health endpoint"
until curl --silent --fail "http://${POLARIS_HOST}:8182/q/health" >/dev/null; do
  sleep 2
done

echo "Polaris is ready at http://${POLARIS_HOST}:8181"
echo "Opening Spark SQL. Generated OpenLineage payloads will be printed above and saved under ${OPENLINEAGE_DIR}"
echo "Press Ctrl-C to stop Spark, Polaris, and the local OpenLineage receiver"

cd "${ROOT_DIR}"
env POLARIS_HOST="${POLARIS_HOST}" ./regtests/run_spark_sql.sh
