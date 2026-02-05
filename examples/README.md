# buquet Examples

End-to-end examples demonstrating buquet's polyglot task queue. Python and Rust producers/workers can freely interoperate.

## Setup

```bash
# Start LocalStack
docker compose up -d
```

Config is read from `.buquet.toml` in the project root.

## Running Examples

### Python

```bash
# Terminal 1: Start worker
uv run python examples/worker.py

# Terminal 2: Submit tasks
uv run python examples/producer.py
```

### Rust

```bash
# Terminal 1: Start worker
cargo run --example worker

# Terminal 2: Submit tasks
cargo run --example producer
```

### Cross-Language

Tasks are language-agnostic. Mix and match:

```bash
# Rust worker + Python producer
cargo run --example worker    # Terminal 1
uv run python examples/producer.py  # Terminal 2

# Python worker + Rust producer
uv run python examples/worker.py    # Terminal 1
cargo run --example producer        # Terminal 2
```

## Files

| File | Description |
|------|-------------|
| `producer.py` | Python: submits orders, monitors status |
| `worker.py` | Python: processes orders |
| `producer.rs` | Rust: submits orders, monitors status |
| `worker.rs` | Rust: processes orders |
| `s3_lifecycle_demo.py` | Shows S3 file structure and task lifecycle |
| `config.example.toml` | Full config with all options documented |

## Cleanup

```bash
docker compose down -v
```
