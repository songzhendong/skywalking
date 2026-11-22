# DolphinScheduler monitoring

SkyWalking ingests **Apache DolphinScheduler** metrics from its Prometheus Actuator
endpoints via the [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/)
and the [OpenTelemetry receiver](opentelemetry-receiver.md), aggregates them with
[MAL](../../concepts-and-designs/mal.md), and shows them in Horizon UI under
**Workflow Scheduler → DolphinScheduler**.

Official metric reference:
[DolphinScheduler Metrics](https://github.com/apache/dolphinscheduler/blob/dev/docs/docs/en/guide/metrics/metrics.md).

## Data flow

1. Enable metrics on DolphinScheduler Master / Worker / API / Alert (`metrics.enabled: true`).
2. OpenTelemetry Collector scrapes `/actuator/prometheus` (or standalone
   `/dolphinscheduler/actuator/prometheus`) and exports OTLP to SkyWalking OAP.
3. OAP aggregates metrics with [MAL](../../concepts-and-designs/mal.md)
   (`otel-rules/dolphinscheduler/*`).
4. Horizon UI displays them under **Workflow Scheduler → DolphinScheduler**.

## Setup

### 1. Enable DolphinScheduler Prometheus metrics

In each component's `application.yaml`:

```yaml
metrics:
  enabled: true
```

Default scrape ports (see DolphinScheduler docs): Master `5679`, Worker `1235`,
Alert `50053`, API `12345`. If Actuator Basic Auth is enabled, configure Collector
credentials accordingly.

### 2. OpenTelemetry Collector

Scrape each component, inject prometheus labels `cluster` and `host_name`
(underscore; OAP instance rules use `host_name`), and **force** metric label
`job_name=dolphinscheduler-monitoring` via `metric_relabel_configs`. Without that
relabel, OAP maps scrape `job` / `service.name` instead and MAL filters drop every
sample.

```yaml
receivers:
  prometheus:
    config:
      scrape_configs:
        - job_name: dolphinscheduler-master
          metrics_path: /actuator/prometheus
          static_configs:
            - targets: ['ds-master:5679']
              labels:
                cluster: prod-ds
                host_name: ds-master-1
          metric_relabel_configs:
            - source_labels: []
              target_label: job_name
              replacement: dolphinscheduler-monitoring
        - job_name: dolphinscheduler-worker
          metrics_path: /actuator/prometheus
          static_configs:
            - targets: ['ds-worker:1235']
              labels:
                cluster: prod-ds
                host_name: ds-worker-1
          metric_relabel_configs:
            - source_labels: []
              target_label: job_name
              replacement: dolphinscheduler-monitoring
        # Add API / Alert scrapes the same way.

exporters:
  otlp:
    endpoint: oap:11800
    tls:
      insecure: true

processors:
  batch:

service:
  pipelines:
    metrics:
      receivers: [prometheus]
      processors: [batch]
      exporters: [otlp]
```

Full example: [`test/e2e-v2/cases/dolphinscheduler/cluster/otel-collector-config.yaml`](../../../../test/e2e-v2/cases/dolphinscheduler/cluster/otel-collector-config.yaml).

### 3. SkyWalking OAP

Ensure `dolphinscheduler/*` is in `SW_OTEL_RECEIVER_ENABLED_OTEL_METRICS_RULES`
(enabled by default).

## Entity model

| SkyWalking entity | Mapping |
|-------------------|---------|
| Service | `dolphinscheduler::{cluster}` from Collector label `cluster` |
| Instance (OAP) / **Components** (UI) | Master / Worker / API / Alert host from `host_name` |

Service panels aggregate cluster-wide samples. Component panels are scoped per `host_name`
and appear when that component emits the underlying meters (`visibleWhen` / exists).

## Supported metrics

MAL rules: `otel-rules/dolphinscheduler/dolphinscheduler-service.yaml` and
`dolphinscheduler-instance.yaml` (**29** Prometheus metric families; Service + Instance
meters for Horizon panels).

### DolphinScheduler Service Supported Metrics

| Monitoring Panel | Unit | Metric Name | Description |
|------------------|------|-------------|-------------|
| Running Workflows | count | meter_dolphinscheduler_workflow_instance_running | Running workflow instances |
| Prepared Tasks | count | meter_dolphinscheduler_task_prepared | Tasks prepared for the queue |
| Active Worker Tasks | count | meter_dolphinscheduler_worker_active_execute_thread | Active execute threads across workers |
| Task Failures | count/min | meter_dolphinscheduler_task_fail_count | Failed task instances per minute |
| Task Success / Fail / Timeout | count/min | meter_dolphinscheduler_task_success_count, meter_dolphinscheduler_task_fail_count, meter_dolphinscheduler_task_timeout_count | Task outcomes per minute |
| Workflow Success / Fail | count/min | meter_dolphinscheduler_workflow_success_count, meter_dolphinscheduler_workflow_fail_count | Workflow outcomes per minute |
| Task Dispatch / Failure / Error | count/min | meter_dolphinscheduler_task_dispatch_count, meter_dolphinscheduler_task_dispatch_failure_count, meter_dolphinscheduler_task_dispatch_error_count | Master→Worker dispatch |
| Task Execution Duration | ms | meter_dolphinscheduler_task_execution_duration_percentile | Task execution latency percentiles |
| Commands Created / Workflow Submit | count/min | meter_dolphinscheduler_workflow_create_command_count, meter_dolphinscheduler_workflow_instance_submit_count | Command and submit pressure |

### DolphinScheduler Instance Supported Metrics

| Monitoring Panel | Unit | Metric Name | Description |
|------------------|------|-------------|-------------|
| Master Overload | count/min | meter_dolphinscheduler_instance_master_overload_count | Master overload events |
| Commands Consumed | count/min | meter_dolphinscheduler_instance_master_consume_command_count | Commands consumed by master |
| Failover Check | count/min, ms | meter_dolphinscheduler_instance_master_scheduler_failover_check_count, meter_dolphinscheduler_instance_master_scheduler_failover_check_time_percentile | Scheduler failover checks |
| RPC Exception / Duration | count/min, ms | meter_dolphinscheduler_instance_rpc_client_sync_request_exception_count, meter_dolphinscheduler_instance_rpc_client_sync_request_duration_percentile | Sync RPC health |
| Worker Tasks / Queue / Active Threads | count | meter_dolphinscheduler_instance_worker_task, meter_dolphinscheduler_instance_worker_execute_queue_size, meter_dolphinscheduler_instance_worker_active_execute_thread | Worker load |
| Worker CPU / Memory | %, 0.01 GB | meter_dolphinscheduler_instance_worker_cpu_usage, meter_dolphinscheduler_instance_worker_memory_usage, meter_dolphinscheduler_instance_worker_memory_available | Worker capacity (MAL stores `* 100`: usage as percent, available as centi-GB) |
| Worker Overload / Full Submit Queue | count/min | meter_dolphinscheduler_instance_worker_overload_count, meter_dolphinscheduler_instance_worker_full_submit_queue_count | Worker back-pressure |
| API Request / Response / Latency | count/min, ms | meter_dolphinscheduler_instance_api_request_count, meter_dolphinscheduler_instance_api_response_count, meter_dolphinscheduler_instance_api_response_time_percentile | API server traffic (`code` is class-based: `2xx`/`3xx`/`4xx`/`5xx`) |
| Alert Send / Pending | count/min, count | meter_dolphinscheduler_instance_alert_send_count, meter_dolphinscheduler_instance_alert_pending | Alert delivery |

## Verification

Run the e2e suites (same as CI):

```bash
# Full meter contract via OTLP JSON replay
e2e run -c test/e2e-v2/cases/dolphinscheduler/mock/e2e.yaml

# Live Prometheus scrape of Master / Worker / API / Alert
e2e run -c test/e2e-v2/cases/dolphinscheduler/cluster/e2e.yaml
```

See [`test/e2e-v2/cases/dolphinscheduler/README.md`](../../../../test/e2e-v2/cases/dolphinscheduler/README.md).

## Horizon UI

Open **Workflow Scheduler → DolphinScheduler** after OAP ingests metrics.
Dashboards ship in `apache/skywalking-horizon-ui`.
