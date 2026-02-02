# Getting Started with buquet

buquet is a distributed task queue built entirely on S3-compatible object storage. No databases. No message brokers. No coordination services. Just workers polling object storage and claiming tasks atomically.

## Prerequisites

Before you begin, ensure you have the following installed:

- **Docker and Docker Compose** - For running the local development environment
- **uv** - Python package manager ([install guide](https://docs.astral.sh/uv/getting-started/installation/))
- **Rust toolchain** - Required only for development ([install guide](https://rustup.rs/))

## Quick Start with Docker

Start the complete buquet stack with a single command:

```bash
docker compose up -d
```

This starts:
- **LocalStack** - S3-compatible storage (AWS emulation) on port 4566
- **Worker** - Background task processor
- **Dashboard** - Web UI at http://localhost:8080

The dashboard provides visibility into task states, worker status, and queue metrics.

## Python SDK Installation

Install the buquet Python SDK using uv:

```bash
uv add buquet
```

## Submitting Tasks

Submit tasks to the queue using the Python SDK:

```python
import asyncio
from buquet import connect

async def main():
    queue = await connect(bucket="buquet-dev", endpoint="http://localhost:4566")
    task = await queue.submit("send_email", {"to": "user@example.com"})
    print(f"Task {task.id} submitted")

asyncio.run(main())
```

Tasks are stored as JSON objects in S3 and picked up by workers automatically.

## Running a Worker

Create a worker that processes tasks:

```python
import asyncio
from buquet import connect, Worker, RetryableError

async def main():
    queue = await connect(bucket="buquet-dev", endpoint="http://localhost:4566")
    worker = Worker(queue)

    @worker.task("send_email")
    async def handle_email(input):
        print(f"Sending to {input['to']}")
        return {"sent": True}

    await worker.run()

asyncio.run(main())
```

Workers poll for available tasks, claim them atomically using S3 conditional writes, and process them. Multiple workers can run concurrently for horizontal scaling.

## Configuration

Configure buquet using environment variables:

| Variable | Description | Example |
|----------|-------------|---------|
| `S3_ENDPOINT` | S3-compatible endpoint URL | `http://localhost:4566` |
| `S3_BUCKET` | Bucket name for task storage | `buquet-dev` |
| `S3_REGION` | AWS region | `us-east-1` |
| `AWS_ACCESS_KEY_ID` | Access key for S3 authentication | `test` |
| `AWS_SECRET_ACCESS_KEY` | Secret key for S3 authentication | `test` |

For local development with Docker Compose, these are pre-configured.

## Shell Completions

Generate shell completions for tab-completion support:

```bash
# Bash
buquet completions bash > ~/.local/share/bash-completion/completions/buquet

# Zsh (add to fpath)
buquet completions zsh > ~/.zfunc/_buquet

# Fish
buquet completions fish > ~/.config/fish/completions/buquet.fish
```

After installing, restart your shell or source the completion file.

## Error Handling

buquet distinguishes between two types of errors:

### RetryableError

Use for temporary failures that should be retried (network timeouts, rate limits, temporary service unavailability):

```python
from buquet import RetryableError

@worker.task("fetch_data")
async def fetch_data(input):
    try:
        response = await http_client.get(input["url"])
        return {"data": response.json()}
    except ConnectionError as e:
        raise RetryableError(f"Connection failed: {e}")
```

Tasks that fail with `RetryableError` are automatically retried with exponential backoff (default: up to 3 retries).

### PermanentError

Use for failures that should not be retried (invalid input, business logic errors, unrecoverable states):

```python
from buquet import PermanentError

@worker.task("process_order")
async def process_order(input):
    if "order_id" not in input:
        raise PermanentError("Missing required field: order_id")
    # Process order...
```

Tasks that fail with `PermanentError` are immediately moved to failed status without retry.

## Adaptive Polling (Cost Optimization)

buquet uses adaptive polling by default to minimize S3 costs. When tasks are available, polling is fast. When the queue is idle, polling backs off automatically.

```python
from buquet import Worker, WorkerRunOptions, PollingStrategy

options = WorkerRunOptions(
    polling=PollingStrategy.adaptive(
        min_interval_ms=100,
        max_interval_ms=5000,
        backoff_multiplier=2.0
    )
)
await worker.run(options)
```

```rust
// Rust example
use buquet::worker::{RunnerConfig, PollingStrategy};

let config = RunnerConfig {
    polling: PollingStrategy::adaptive(100, 5000),
    ..Default::default()
};
```

Or via config file (`buquet.toml`):

```toml
[worker.polling]
strategy = "adaptive"
min_interval_ms = 100
max_interval_ms = 5000
backoff_multiplier = 2.0
```

For details on trade-offs and cost estimates, see [Adaptive Polling](features/adaptive-polling.md).

## Configurable Shards (Scaling)

For high-volume queues, increase the shard count to improve parallelism and reduce LIST operation costs:

```python
queue = await connect(shard_prefix_len=2)  # 256 shards
```

Or via config file:

```toml
[queue]
shard_prefix_len = 2  # 1=16 shards, 2=256 shards, 3=4096 shards
```

| Scale | Recommended `shard_prefix_len` |
|-------|-------------------------------|
| < 10K tasks/day | 1 (default, 16 shards) |
| 10K - 1M tasks/day | 2 (256 shards) |
| > 1M tasks/day | 3 (4096 shards) |

**Note**: All producers and workers must use the same shard configuration. See [Configurable Shards](features/configurable-shards.md) for migration guidance.

## Shard Leasing (Worker Coordination)

For large deployments, shard leasing assigns shards dynamically to workers so each worker only polls its owned shards. This eliminates redundant LIST operations across workers.

```python
from buquet import WorkerRunOptions, ShardLeaseConfig

options = WorkerRunOptions(shard_leasing=ShardLeaseConfig.enabled())
await worker.run(options)
```

Or via config file:

```toml
[worker.shard_leasing]
enabled = true
shards_per_worker = 16
lease_ttl_secs = 30
```

See [Shard Leasing](features/shard-leases.md) for lease algorithm details and failure recovery.

## Version History and Retention

buquet uses S3 bucket versioning to maintain a complete audit trail of every task state transition. This enables powerful debugging with `buquet history <task_id>` and compliance capabilities, but requires lifecycle policy configuration to manage storage costs at scale.

**Important:** Without lifecycle policies, version history grows indefinitely. For production deployments, configure S3 lifecycle rules to expire old versions:

```bash
# Apply a 30-day retention policy
aws s3api put-bucket-lifecycle-configuration \
  --bucket your-buquet-bucket \
  --lifecycle-configuration '{
    "Rules": [{
      "ID": "buquet-version-retention",
      "Status": "Enabled",
      "Filter": {"Prefix": "tasks/"},
      "NoncurrentVersionExpiration": {"NoncurrentDays": 30}
    }]
  }'
```

For detailed guidance on cost implications and retention strategies, see [Version History Retention Guide](guides/version-history-retention.md).

## Supported Storage Backends

buquet works with any S3-compatible storage that supports strong consistency and conditional writes. Tested providers include:

| Provider | Status | Notes |
|----------|--------|-------|
| AWS S3 | Recommended | Reference implementation |
| LocalStack | Recommended | Default for local dev (full AWS emulation) |
| MinIO | Recommended | Best for self-hosted production |
| SeaweedFS | Supported | High-performance self-hosted |
| Cloudflare R2 | Supported | No versioning/history |
| Wasabi | Supported | Cost-effective |
| DigitalOcean Spaces | Supported | No versioning/history |

For detailed compatibility information, provider-specific configuration, and a compatibility test script, see the [S3 Compatibility Guide](guides/s3-compatibility.md).

## Next Steps

- View task status and metrics in the dashboard at http://localhost:8080
- Scale by adding more workers (they coordinate via S3)
- Inspect task data directly with `aws s3 ls s3://buquet/tasks/`
