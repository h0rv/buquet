# buquet

[![GitHub](https://img.shields.io/badge/GitHub-h0rv%2Fbuquet-blue?logo=github)](https://github.com/h0rv/buquet)

A distributed task queue built on S3. No databases, no brokers—just a bucket.

Works with any S3-compatible storage: AWS S3, Cloudflare R2, MinIO, Tigris, Backblaze B2, DigitalOcean Spaces, and more.

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

## Documentation

- [Getting Started](docs/getting-started.md) — Setup and configuration
- [Python API](python/README.md) — Python bindings reference
- [Examples](examples/) — Working Python and Rust examples
- [Feature Roadmap](docs/features/) — Completed and planned features
- [Workflows](docs/features/workflows/) — Durable workflow orchestration

## Why?

- **S3 is the control plane.** No databases, no brokers - just a bucket.
- **Durable.** Tasks survive crashes. Workers recover automatically via lease expiry.
- **Observable.** Every state transition is persisted: full audit trail by design.
- **Latency is seconds, not milliseconds.** Designed for background work, not real-time.

## License

MIT
