#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Wait until DS Prometheus endpoints respond, force API Micrometer registration,
# then poll OAP until cluster meters exist (no fixed-only sleep).

set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-test/e2e-v2/cases/dolphinscheduler/cluster/docker-compose.yml}"

resolve_compose_project() {
  if [[ -n "${COMPOSE_PROJECT_NAME:-}" ]]; then
    echo "${COMPOSE_PROJECT_NAME}"
    return
  fi
  local master_container
  master_container="$(docker ps --filter 'name=-ds-master-' --format '{{.Names}}' | head -1)"
  if [[ -n "${master_container}" ]]; then
    echo "${master_container%-ds-master-*}"
    return
  fi
  echo "skywalking_e2e"
}

COMPOSE_PROJECT_NAME="$(resolve_compose_project)"

dc() {
  docker compose -p "${COMPOSE_PROJECT_NAME}" -f "${COMPOSE_FILE}" "$@"
}

wait_prometheus() {
  local service="$1"
  local url="$2"
  local needle="${3:-}"
  echo "Waiting for Prometheus on ${service} (${url})${needle:+ [needle=${needle}]}..."
  for _ in $(seq 1 90); do
    if dc exec -T "${service}" curl -fsS "${url}" >/tmp/ds-prom.out 2>/dev/null; then
      if [[ -z "${needle}" ]] || grep -q "${needle}" /tmp/ds-prom.out; then
        echo "${service} Prometheus ready."
        return 0
      fi
    fi
    sleep 5
  done
  echo "Timed out waiting for ${service} Prometheus at ${url}" >&2
  dc logs --tail=80 "${service}" || true
  exit 1
}

resolve_oap_base() {
  if [[ -n "${oap_host:-}" && -n "${oap_12800:-}" ]]; then
    echo "http://${oap_host}:${oap_12800}"
    return
  fi
  local mapped
  mapped="$(dc port oap 12800 2>/dev/null | head -1 || true)"
  if [[ -n "${mapped}" ]]; then
    echo "http://${mapped}"
    return
  fi
  echo "http://127.0.0.1:12800"
}

wait_oap_metric() {
  local expression="$1"
  local service_name="$2"
  local instance_name="${3:-}"
  local base
  base="$(resolve_oap_base)"
  echo "Waiting for OAP meter ${expression} on ${service_name}${instance_name:+ / ${instance_name}} via ${base}..."
  local args=(--display yaml --base-url="${base}/graphql" metrics exec
    --expression="${expression}" --service-name="${service_name}")
  if [[ -n "${instance_name}" ]]; then
    args+=(--instance-name="${instance_name}")
  fi
  for _ in $(seq 1 90); do
    if out="$(swctl "${args[@]}" 2>/dev/null)" && echo "${out}" | grep -Eq 'value:[[:space:]]*["'\'']?[0-9]'; then
      echo "OAP meter ready: ${expression}"
      return 0
    fi
    sleep 5
  done
  echo "Timed out waiting for OAP meter ${expression}" >&2
  swctl "${args[@]}" || true
  exit 1
}

# Gauges / counters that register at process start.
wait_prometheus ds-master "http://127.0.0.1:5679/actuator/prometheus" "ds_workflow_instance_running"
wait_prometheus ds-worker "http://127.0.0.1:1235/actuator/prometheus" "ds_worker_cpu_usage"
wait_prometheus ds-alert "http://127.0.0.1:50053/actuator/prometheus" "ds_alert_pending"

# API ds_* meters are often lazy — wait for the endpoint first, then traffic.
wait_prometheus ds-api "http://127.0.0.1:12345/dolphinscheduler/actuator/prometheus"

echo "Generating API traffic to register ds_api_* meters..."
for _ in $(seq 1 30); do
  dc exec -T ds-api curl -fsS -o /dev/null -w "%{http_code}" \
    "http://127.0.0.1:12345/dolphinscheduler/login" || true
  dc exec -T ds-api curl -fsS -o /dev/null \
    "http://127.0.0.1:12345/dolphinscheduler/users/get-user-info" || true
  sleep 1
done

wait_prometheus ds-api "http://127.0.0.1:12345/dolphinscheduler/actuator/prometheus" "ds_api_request_count"

wait_oap_instances() {
  local service_name="$1"
  shift
  local base names
  base="$(resolve_oap_base)"
  names=("$@")
  echo "Waiting for OAP instances on ${service_name}: ${names[*]}..."
  for _ in $(seq 1 90); do
    local out missing=0
    out="$(swctl --display yaml --base-url="${base}/graphql" instance ls --service-name="${service_name}" 2>/dev/null || true)"
    for n in "${names[@]}"; do
      if ! echo "${out}" | grep -q "name:[[:space:]]*${n}$"; then
        missing=1
        break
      fi
    done
    if [[ "${missing}" -eq 0 ]]; then
      echo "OAP instances ready: ${names[*]}"
      return 0
    fi
    sleep 5
  done
  echo "Timed out waiting for OAP instances ${names[*]}" >&2
  swctl --display yaml --base-url="${base}/graphql" instance ls --service-name="${service_name}" || true
  exit 1
}

# Confirm MAL filter + scrape path: service meters and all four component instances.
wait_oap_metric "meter_dolphinscheduler_workflow_instance_running" "dolphinscheduler::ds-e2e-cluster"
# Master meters are increase counters; wait until consume_command has a series for ds-master-1.
wait_oap_metric "meter_dolphinscheduler_instance_master_consume_command_count" "dolphinscheduler::ds-e2e-cluster" "ds-master-1"
wait_oap_metric "meter_dolphinscheduler_instance_worker_cpu_usage" "dolphinscheduler::ds-e2e-cluster" "ds-worker-1"
wait_oap_metric "meter_dolphinscheduler_instance_alert_pending" "dolphinscheduler::ds-e2e-cluster" "ds-alert-1"
wait_oap_metric "meter_dolphinscheduler_instance_api_request_count" "dolphinscheduler::ds-e2e-cluster" "ds-api-1"
wait_oap_instances "dolphinscheduler::ds-e2e-cluster" ds-master-1 ds-worker-1 ds-api-1 ds-alert-1

echo "DolphinScheduler cluster setup finished."
