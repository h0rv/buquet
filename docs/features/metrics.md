# Metrics

> **Status:** COMPLETED (2026-01-26)
> **Effort:** ~50 lines of Rust
> **Tier:** 2 (Production Essential)

oq uses the [`metrics`](https://docs.rs/metrics) crate - a facade that lets you instrument code once and pick your exporter at runtime.

## Metrics Emitted

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `oq.tasks.submitted` | Counter | `task_type` | Tasks submitted to queue |
| `oq.tasks.completed` | Counter | `task_type` | Tasks completed successfully |
| `oq.tasks.failed` | Counter | `task_type`, `reason` | Tasks failed (`timeout`, `retryable`, `permanent`, `no_handler`) |
| `oq.task.duration_seconds` | Histogram | `task_type` | Task execution duration |
| `oq.claims.success` | Counter | | Successful task claims |
| `oq.claims.conflict` | Counter | | Claim conflicts (another worker won) |
| `oq.tasks.timeout_recovered` | Counter | | Tasks recovered after timeout |
| `oq.tasks.retries_exhausted` | Counter | | Tasks failed after max retries |

## Python Configuration

Python workers have built-in support for configuring metrics exporters:

### Prometheus

```python
import oq

# Start Prometheus exporter on :9000/metrics
oq.metrics.enable_prometheus(port=9000)

# Now run your worker as usual
queue = await oq.connect()
worker = oq.Worker(queue, "worker-1", ["0", "1", "2", "3"])
await worker.run()
```

### StatsD / Datadog

```python
import oq

# Send metrics to Datadog agent via DogStatsD
oq.metrics.enable_statsd(host="127.0.0.1", port=8125)
```

### OpenTelemetry (OTLP)

```python
import oq

# Send metrics to an OpenTelemetry collector
oq.metrics.enable_opentelemetry(endpoint="http://localhost:4317")
```

### Environment Variable Configuration

Configure metrics via environment variables for deployment flexibility:

```bash
# Choose exporter
export OQ_METRICS_EXPORTER=prometheus  # or: statsd, datadog, opentelemetry, otlp

# Prometheus options
export OQ_METRICS_PROMETHEUS_PORT=9000

# StatsD/Datadog options
export OQ_METRICS_STATSD_HOST=127.0.0.1
export OQ_METRICS_STATSD_PORT=8125

# OpenTelemetry options
export OQ_METRICS_OTLP_ENDPOINT=http://localhost:4317
```

Then in Python:

```python
import oq

# Auto-configure from environment
if oq.metrics.auto_configure():
    print("Metrics configured from environment")
else:
    print("OQ_METRICS_EXPORTER not set, metrics disabled")
```

### Check Current Configuration

```python
import oq

exporter = oq.metrics.current_exporter()
if exporter:
    print(f"Using {exporter} exporter")
else:
    print("No metrics exporter configured")
```

## Rust Configuration

For Rust applications, add the exporter dependency and configure at startup:

### Prometheus

```toml
# Cargo.toml
[dependencies]
metrics-exporter-prometheus = "0.16"
```

```rust
use metrics_exporter_prometheus::PrometheusBuilder;

fn main() {
    PrometheusBuilder::new()
        .with_http_listener(([0, 0, 0, 0], 9000))
        .install()
        .expect("failed to install Prometheus recorder");

    // Metrics available at http://localhost:9000/metrics
}
```

### StatsD / Datadog

```toml
[dependencies]
metrics-exporter-statsd = "0.9"
```

```rust
use metrics_exporter_statsd::StatsdBuilder;

fn main() {
    let recorder = StatsdBuilder::from("127.0.0.1", 8125)
        .with_queue_size(5000)
        .with_buffer_size(1024)
        .build(None)
        .expect("failed to build StatsD recorder");

    metrics::set_global_recorder(recorder)
        .expect("failed to set recorder");
}
```

## Why a Facade?

The `metrics` crate is like `log`/`tracing` - define metrics once, swap backends:

- **Zero cost** if no exporter installed
- **No vendor lock-in** - switch Prometheus <-> StatsD without code changes
- **Composable** - multiple exporters can run simultaneously
