# buquet

Bucket queue: simple task queue and workflow orchestration.

Works with any S3-compatible storage: [AWS S3](https://aws.amazon.com/s3/), [Cloudflare R2](https://www.cloudflare.com/developer-platform/r2/), [MinIO](https://min.io/), [Tigris](https://www.tigrisdata.com/), [Backblaze B2](https://www.backblaze.com/cloud-storage), [DigitalOcean Spaces](https://www.digitalocean.com/products/spaces), and more.

## Packages

- **[buquet](crates/buquet/)** — Core library (Rust + Python bindings)
- **[buquet-workflow](crates/buquet-workflow/)** — Durable workflow orchestration (to be merged)

## Quick Start

### With AWS S3 (Production)

```bash
cargo install buquet

# Configure your bucket
export S3_BUCKET=my-queue-bucket
export S3_REGION=us-east-1

# Run a worker
buquet worker --shards 0,1,2,3

# Submit a task
buquet submit -t my_task -i '{"foo": "bar"}'
```

### With LocalStack (Development)

```bash
# Start LocalStack
docker compose up -d

# Point to LocalStack
export S3_ENDPOINT=http://localhost:4566
export S3_BUCKET=buquet-dev

buquet worker --shards 0,1,2,3
```

See [Getting Started](docs/getting-started.md) for setup details.

## Why?

- **S3 is the control plane.** No databases, no brokers - just a bucket.
- **Durable.** Tasks survive crashes. Workers recover automatically via lease expiry.
- **Observable.** Every state transition is persisted: full audit trail by design.
- **Latency is seconds, not milliseconds.** Designed for background work, not real-time.

## License

MIT
