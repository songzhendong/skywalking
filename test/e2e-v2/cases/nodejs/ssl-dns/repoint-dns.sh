#!/usr/bin/env bash
#
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
#
# Repoint oap.test inside provider/consumer containers (DNS + TLS combo E2E).
set -euo pipefail
MODE="${1:?usage: repoint-dns.sh good|bad}"

provider_container() {
  docker ps --format '{{.Names}}' | grep -E 'provider' | head -1
}

consumer_container() {
  docker ps --format '{{.Names}}' | grep -E 'consumer' | head -1
}

repoint_container() {
  local cid="$1"
  local ip="$2"
  docker exec "${cid}" bash -c "grep -v '[[:space:]]oap\\.test' /etc/hosts > /tmp/h && echo '${ip} oap.test' >> /tmp/h && cat /tmp/h > /etc/hosts"
}

for cid in "$(provider_container)" "$(consumer_container)"; do
  [[ -z "${cid}" ]] && continue
  if [[ "${MODE}" == "bad" ]]; then
    repoint_container "${cid}" "127.0.0.1"
  elif [[ "${MODE}" == "good" ]]; then
    OIP="$(docker exec "${cid}" getent hosts oap | awk '{print $1; exit}')"
    if [[ -z "${OIP}" ]]; then
      echo "oap IP not found in ${cid}" >&2
      exit 1
    fi
    repoint_container "${cid}" "${OIP}"
  else
    echo "unknown mode: ${MODE}" >&2
    exit 1
  fi
done
