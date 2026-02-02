# buquet

Task queue using S3-compatible object storage as the only backend. Works with AWS S3, Cloudflare R2, MinIO, Tigris, Backblaze B2, DigitalOcean Spaces, and any other S3-compatible provider.

## Installation

**CLI:**
```bash
cargo install buquet
```

**Rust library:**
```toml
[dependencies]
buquet = "0.1"
```

**Python:**
```bash
uv add buquet
```

## How It Works

Tasks are JSON objects stored in S3. Workers poll ready indexes and claim tasks atomically via `If-Match` (CAS). Leases expire on timeout and get recovered automatically.

```
tasks/{shard}/{task_id}.json      # task object (versioned)
ready/{shard}/{bucket}/{task_id}  # claimable tasks index
leases/{shard}/{bucket}/{task_id} # running tasks by expiry
```

Workers move tasks between states in-place. Ready/lease indexes are projections that can be rebuilt from the task log.

## Usage

### Python

See [python/README.md](python/README.md) for the full Python API.

```python
from buquet import connect, Worker

queue = await connect()
task = await queue.submit("send_email", {"to": "user@example.com"})

worker = Worker(queue, "worker-1", ["0", "1", "2", "3"])

@worker.task("send_email")
async def handle(input):
    return {"sent": True}

await worker.run()
```

### Rust

```rust
let queue = buquet::connect(Config::from_env()?).await?;

// Submit
let task = queue.submit("send_email", json!({"to": "user@example.com"})).await?;

// Claim and complete
if let Some(task) = queue.claim_next(&["0"]).await? {
    queue.complete(task.id, json!({"sent": true})).await?;
}
```

### CLI

```bash
buquet submit -t send_email -i '{"to": "user@example.com"}'
buquet worker --shards 0,1,2,3
buquet list 0 --status pending
buquet get <task-id>
```

## Features

- **Scheduling**: `--delay 1h` or `--at "2026-01-28T09:00:00Z"`
- **Cron schedules**: `buquet schedule create daily -t report -c "0 9 * * *"`
- **Cancellation**: `buquet cancel <id>` or `queue.cancel(id)`
- **TTL/Expiration**: `--ttl 1h` or `--expires-at "..."`
- **Retries**: Exponential backoff with configurable policy

## Configuration

Create `.buquet.toml` in your project root (auto-discovered):

```toml
[default]
bucket = "my-queue"
endpoint = "http://localhost:4566"  # optional, for S3-compatible
region = "us-east-1"
```

Or use environment variables:

| Variable | Required | Description |
|----------|----------|-------------|
| `S3_BUCKET` | Yes | Bucket name |
| `S3_ENDPOINT` | No | S3-compatible endpoint |
| `S3_REGION` | No | Region (default: us-east-1) |
| `AWS_ACCESS_KEY_ID` | Yes | Access key |
| `AWS_SECRET_ACCESS_KEY` | Yes | Secret key |

Priority: env vars > config file.

## Local Development

```bash
docker compose up -d          # LocalStack
source examples/.env.example
cargo run -- worker --shards 0,1,2,3
```

See [examples/](examples/) for working Python and Rust examples.

## Tradeoffs

- **Latency**: Seconds, not milliseconds. Polling-based.
- **Ordering**: None. Trades ordering for simplicity.
- **Delivery**: At-least-once. Handlers should be idempotent.
- **Throughput**: Medium. LIST is O(N/shard).

Good for batch jobs, async work, internal tooling. Not for real-time.
