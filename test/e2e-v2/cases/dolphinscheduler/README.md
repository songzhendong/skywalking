# DolphinScheduler monitoring e2e tests

End-to-end tests for the `DOLPHINSCHEDULER` layer (Prometheus → OpenTelemetry Collector → OAP MAL).

```
dolphinscheduler/
├── mock/     # CI "DolphinScheduler" — OTLP JSON replay (full matrix)
├── cluster/  # CI "DolphinScheduler Cluster" — real Master/Worker/API/Alert scrape
└── README.md
```

MAL rules: [`otel-rules/dolphinscheduler/`](../../../../oap-server/server-starter/src/main/resources/otel-rules/dolphinscheduler/)
(**29** Prometheus metric families → **33** Service + Instance meters).

## Run locally (same as CI)

From the repository root, with [skywalking-infra-e2e](https://github.com/apache/skywalking-infra-e2e)
and [swctl](https://github.com/apache/skywalking-cli) installed:

```bash
# Mock — full meter contract via OTLP JSON replay
e2e run -c test/e2e-v2/cases/dolphinscheduler/mock/e2e.yaml

# Cluster — live Prometheus scrape of a real DS stack
e2e run -c test/e2e-v2/cases/dolphinscheduler/cluster/e2e.yaml
```

CI builds OAP with `make docker.all` then runs the same configs via
[`.github/workflows/skywalking.yaml`](../../../../.github/workflows/skywalking.yaml)
(matrix entries **DolphinScheduler** and **DolphinScheduler Cluster**).

## Mock suite (`mock/`)

Replays synthetic OTLP JSON — full **33**-meter contract (2 entity + 33 metrics).

| File | Role |
|------|------|
| [`e2e.yaml`](mock/e2e.yaml) | CI entry |
| [`dolphinscheduler-cases.yaml`](mock/dolphinscheduler-cases.yaml) | swctl checks |
| [`docker-compose.yml`](mock/docker-compose.yml) | OAP + BanyanDB + OTLP replay sidecar |
| [`mock-data/otel-dolphinscheduler-metrics.json`](mock/mock-data/otel-dolphinscheduler-metrics.json) | Payload |

## Cluster suite (`cluster/`)

Real Master / Worker / API / Alert (pinned `3.2.2`) + Postgres + ZooKeeper.
Collector scrapes `/actuator/prometheus` (API under `/dolphinscheduler/...`) and exports OTLP.

| File | Role |
|------|------|
| [`e2e.yaml`](cluster/e2e.yaml) | CI entry |
| [`docker-compose.yml`](cluster/docker-compose.yml) | DS stack + Collector + OAP |
| [`otel-collector-config.yaml`](cluster/otel-collector-config.yaml) | Prometheus scrape + `job_name` |
| [`setup.sh`](cluster/setup.sh) | Wait for Prometheus + light API traffic |
| [`dolphinscheduler-cases.yaml`](cluster/dolphinscheduler-cases.yaml) | Smoke checks (13) |

### Coverage split

- **Mock**: every MAL meter (contract).
- **Cluster**: entity registration + gauges that are stable with an idle/lightly loaded cluster
  (running workflows / prepared tasks / worker capacity / alert pending). Outcome and dispatch
  counters that need workflow load stay in mock only.

## Setup documentation

- [DolphinScheduler monitoring setup](../../../../docs/en/setup/backend/backend-dolphinscheduler-monitoring.md)
