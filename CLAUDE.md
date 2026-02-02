# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

buquet is a distributed task queue built on S3-compatible object storage. No databases, no brokers - just a bucket. Two crates:

- **buquet** (`crates/buquet/`) - Core task queue with Rust library + Python bindings
- **buquet-workflow** (`crates/buquet-workflow/`) - Workflow orchestration layer built on buquet (to be merged)

## Commands

All commands via justfile. Run from workspace root.

```bash
# Full check (format, lint, typecheck, test)
just all

# Individual checks
just fmt          # Format Rust + Python
just lint         # Clippy + Ruff
just check        # Cargo check + Pyright
just test         # Unit tests only

# Integration tests (requires LocalStack)
just up           # Start LocalStack
just integration  # Run integration tests
just down         # Stop LocalStack

# Python development
just py-dev       # Build and install Python wheel locally

# Run a single Rust test
cargo test -p buquet test_name

# Run a single Python test
cd crates/buquet && uv run pytest python/tests/test_file.py::test_name -v
```

## Architecture

### Storage Layout (S3)

```
tasks/{shard}/{task_id}.json       # Task object (versioned)
ready/{shard}/{bucket}/{task_id}   # Claimable tasks index
leases/{shard}/{bucket}/{task_id}  # Running tasks by lease expiry
schedules/{name}.json              # Cron schedules
workers/{worker_id}.json           # Worker registration
```

### Core Flow

1. `queue.submit()` → writes task to `tasks/`, adds to `ready/` index
2. Worker polls `ready/` index, claims via S3 conditional PUT (ETag/If-Match)
3. Claimed task moves to `leases/` with expiry time
4. On completion → task updated, removed from `leases/`
5. On crash → lease expires, sweeper moves back to `ready/`

### Key Modules

**buquet crate:**
- `queue/ops.rs` - Core queue operations (submit, claim, complete, fail)
- `worker/` - Worker loop, lease management, timeout monitor
- `storage/` - S3 abstraction layer
- `python/` - PyO3 bindings

**buquet-workflow crate:**
- `engine.rs` - Workflow execution engine
- `dag.rs` - Step dependency graph
- `state.rs` - Workflow state with CAS updates

## Code Standards

Strict linting enforced:

**Rust:**
- `unsafe_code = "forbid"`
- `unwrap_used = "deny"`, `expect_used = "deny"`, `panic = "deny"`
- `todo = "deny"`, `dbg_macro = "deny"`
- Clippy pedantic + nursery

**Python:**
- Pyright strict mode
- Ruff with 45+ rule sets

## Configuration

Environment variables or `.buquet.toml`:

```bash
# Required
S3_BUCKET=my-bucket
S3_REGION=us-east-1

# Optional: Only set for LocalStack/MinIO (real AWS is the default)
S3_ENDPOINT=http://localhost:4566
```

**Note:** The library defaults to real AWS S3. Only set `S3_ENDPOINT` for LocalStack, MinIO, or other S3-compatible services. Tests default to LocalStack for convenience.

## Python Bindings

Built with PyO3 + Maturin. After changes to Rust code:

```bash
just py-dev  # Rebuilds and installs wheel
```

Python API mirrors Rust: `Queue`, `Worker`, `Task`, `TaskStatus`, etc.
