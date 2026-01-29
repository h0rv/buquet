# OpenTelemetry Tracing

> **Status:** Planned
> **Effort:** ~200 lines of Rust
> **Tier:** 3 (Polish)

Distributed tracing for task execution flow.

## Why

In production, you need to:
- Correlate logs across workers
- Trace a task from submission to completion
- Identify slow operations
- Debug complex multi-service flows

## What Gets Traced

```
Trace: task-submit (550e8400...)
├── S3 PutObject (tasks/a/550e8400.json) [23ms]
└── S3 PutObject (ready/a/1706000000/550e8400) [18ms]

Trace: task-execute (550e8400...)
├── claim [45ms]
│   ├── S3 GetObject (read task) [12ms]
│   ├── S3 PutObject (write with ETag) [28ms]
│   └── S3 DeleteObject (ready marker) [5ms]
├── handler: process_order [234ms]
│   ├── db.query (get order) [45ms]
│   ├── payment.charge [156ms]
│   └── db.update (mark paid) [33ms]
└── complete [38ms]
    ├── S3 PutObject (final state) [31ms]
    └── S3 DeleteObject (lease marker) [7ms]
```

## Configuration

```bash
# Enable tracing via environment
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
export OTEL_SERVICE_NAME=oq-worker

# Run worker with tracing
oq worker
```

## Implementation

```rust
use opentelemetry::trace::{Tracer, TracerProvider};
use opentelemetry_otlp::WithExportConfig;
use tracing_opentelemetry::OpenTelemetryLayer;

fn init_tracing() -> Result<()> {
    // Check if OTLP endpoint is configured
    let endpoint = match env::var("OTEL_EXPORTER_OTLP_ENDPOINT") {
        Ok(e) => e,
        Err(_) => return Ok(()), // No tracing configured
    };

    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(
            opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint(endpoint)
        )
        .install_batch(opentelemetry::runtime::Tokio)?;

    let telemetry = OpenTelemetryLayer::new(tracer);

    tracing_subscriber::registry()
        .with(telemetry)
        .with(tracing_subscriber::fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    Ok(())
}
```

## Instrumented Operations

```rust
// Task submission
#[tracing::instrument(skip(input), fields(task_type = %task_type))]
pub async fn submit(&self, task_type: &str, input: Value) -> Result<Task> {
    // ...
}

// Task execution
#[tracing::instrument(
    skip(handler),
    fields(
        task_id = %task.id,
        task_type = %task.task_type,
        attempt = %task.attempt
    )
)]
async fn execute_task(task: &Task, handler: &dyn TaskHandler) -> Result<Value> {
    // ...
}

// S3 operations
#[tracing::instrument(skip(body), fields(key = %key))]
pub async fn put_object(&self, key: &str, body: &[u8]) -> Result<String> {
    // ...
}
```

## Trace Context Propagation

For tasks that spawn other tasks, propagate trace context:

```rust
// When submitting a task, include parent trace ID
let task = queue.submit_with_context(
    "child_task",
    input,
    TraceContext::current(),
).await?;

// When executing, restore context
let _guard = task.trace_context.as_ref().map(|ctx| ctx.attach());
handler.handle(task.input).await
```

## Jaeger UI

With Jaeger running locally:

```bash
docker run -d --name jaeger \
  -p 16686:16686 \
  -p 4317:4317 \
  jaegertracing/all-in-one:latest
```

View traces at http://localhost:16686

## Files to Change

- `Cargo.toml` - Add OpenTelemetry dependencies
- `crates/oq/src/main.rs` - Initialize tracing
- `crates/oq/src/queue.rs` - Instrument submit/get operations
- `crates/oq/src/worker/execute.rs` - Instrument task execution
- `crates/oq/src/storage/s3.rs` - Instrument S3 operations

## Dependencies

```toml
opentelemetry = { version = "0.21", features = ["rt-tokio"] }
opentelemetry-otlp = { version = "0.14", features = ["tonic"] }
tracing-opentelemetry = "0.22"
```
