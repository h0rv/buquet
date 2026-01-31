# qo Examples

End-to-end examples demonstrating qo's polyglot task queue. Python and Rust producers/workers can freely interoperate—tasks submitted from Python can be processed by Rust workers, and vice versa.

## Setup

```bash
# Start LocalStack
docker compose up -d
```

Config is read from `.qo.toml` in the crate directory. AWS credentials for LocalStack default to `test`/`test`.

## Running the Examples

Python commands below assume you are in `crates/qo` so `uv` uses the correct
`pyproject.toml`.

### Python Producer + Python Worker

#### Terminal 1: Start the Worker

```bash
uv run python examples/worker.py
```

You'll see output like:
```
[worker] Starting order processing worker...
[worker] Registered handlers: ['process_order']
[worker] Polling for tasks...
```

#### Terminal 2: Submit Orders

```bash
uv run python examples/producer.py
```

You'll see:
```
[producer] Submitting 5 orders...
[producer] Submitted order-1: 2x Widget, 1x Gadget = $45.97
[producer] Submitted order-2: 3x Gizmo = $44.97
...
[producer] Waiting for tasks to complete...
[producer] order-1: completed - Total: $45.97
[producer] order-2: completed - Total: $44.97
...
[producer] All orders processed!
```

#### Terminal 1 (Worker Output)

```
[worker] Processing order order-1...
[worker]   - 2x Widget @ $19.99 = $39.98
[worker]   - 1x Gadget @ $5.99 = $5.99
[worker]   - Subtotal: $45.97
[worker] Completed order-1 in 0.5s
```

### Rust Producer + Rust Worker

#### Terminal 1: Start the Rust Worker

```bash
cargo run -p qo --example worker
```

You'll see:
```
[worker] Rust worker starting...
[worker] Connecting to bucket: qo-dev
[worker] Worker ID: rust-worker-a1b2c3d4
[worker] Registered handlers: ["process_order"]
[worker] Polling for tasks (Ctrl+C to stop)...
```

#### Terminal 2: Submit Orders from Rust

```bash
cargo run -p qo --example producer
```

You'll see:
```
[producer] Rust producer starting...
[producer] Submitting 3 orders...

[producer] Submitted rust-order-1: 2x widget, 1x gadget = $45.97
[producer] Submitted rust-order-2: 3x gizmo = $44.97
[producer] Submitted rust-order-3: 1x doohickey, 2x thingamajig = $49.97

[producer] Waiting for tasks to complete...

[producer] rust-order-1: completed - Total: $45.97
[producer] rust-order-2: completed - Total: $44.97
[producer] rust-order-3: completed - Total: $49.97

[producer] All orders processed!
```

### Polyglot: Python Producer + Rust Worker

This demonstrates the key value proposition—**tasks are language-agnostic**.

#### Terminal 1: Start the Rust Worker

```bash
cargo run -p qo --example worker
```

#### Terminal 2: Submit Orders from Python

```bash
uv run python examples/producer.py
```

The Rust worker will process tasks submitted by Python. Both use the same S3 storage format and task protocol.

### Polyglot: Rust Producer + Python Worker

The reverse also works.

#### Terminal 1: Start the Python Worker

```bash
uv run python examples/worker.py
```

#### Terminal 2: Submit Orders from Rust

```bash
cargo run -p qo --example producer
```

## Example Files

### Python
- **producer.py** - Submits sample orders and monitors their status
- **worker.py** - Processes orders with simulated work
- **s3_lifecycle_demo.py** - Shows the S3 file structure and task lifecycle

### Rust
- **producer.rs** - Submits sample orders and monitors their status
- **worker.rs** - Processes orders using the `TaskHandler` trait

### Config
- **config.example.toml** - Full configuration file with all options documented

## Cleanup

```bash
docker compose down -v  # Removes volumes too
```
