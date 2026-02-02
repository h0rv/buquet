# buquet - distributed task queue and workflow orchestration on S3

export UV_CACHE_DIR := justfile_directory() / ".uv-cache"

# Default: show available commands
default:
    @just --list

# ============================================================================
# Combined commands
# ============================================================================

fmt: fmt-rs fmt-py
lint: lint-rs lint-py
check: check-rs check-py
test: test-rs test-py
typecheck: check-py
all: fmt lint check typecheck test

# ============================================================================
# Rust
# ============================================================================

fmt-rs:
    cargo fmt

lint-rs:
    uv run --project {{justfile_directory()}} cargo clippy --all-features --all-targets

check-rs:
    uv run --project {{justfile_directory()}} cargo check --all-features

test-rs:
    cargo test

build:
    cargo build

release:
    cargo build --release

load-test:
    cargo test --test load --release --features load-tests -- --nocapture

chaos-test:
    cargo test --test chaos --release --features chaos-tests -- --nocapture

clean:
    cargo clean

run *ARGS:
    cargo run -- {{ARGS}}

dev *ARGS:
    @export $(grep -v '^#' {{justfile_directory()}}/examples/.env.example | xargs) && cargo run -- {{ARGS}}

serve PORT="8080":
    @export $(grep -v '^#' {{justfile_directory()}}/examples/.env.example | xargs) && cargo run -- serve --port {{PORT}}

# ============================================================================
# Python
# ============================================================================

py-setup:
    uv sync --project {{justfile_directory()}} --group dev

py-dev: py-setup
    uv run --project {{justfile_directory()}} maturin develop

py-build: py-setup
    uv run --project {{justfile_directory()}} maturin build --release

fmt-py:
    uv run --project {{justfile_directory()}} ruff format python/ examples/
    uv run --project {{justfile_directory()}} ruff check --fix --select I python/ examples/

lint-py:
    uv run --project {{justfile_directory()}} ruff check python/ examples/

check-py: py-setup
    uv run --project {{justfile_directory()}} pyright python/

test-py: py-setup py-dev
    uv run --project {{justfile_directory()}} pytest python/tests/ -m "not integration"

test-py-integration: py-setup py-dev
    uv run --project {{justfile_directory()}} pytest python/tests/ -m integration

py-verify:
    uv run --project {{justfile_directory()}} python -c "from buquet import connect, Queue, Task, TaskStatus, Worker; from buquet.workflow import WorkflowEngine, WorkflowClient, StepDef; print('All imports OK')"

# ============================================================================
# Integration tests
# ============================================================================

integration: integration-rs integration-py

integration-rs: up
    if command -v awslocal >/dev/null 2>&1; then ./scripts/init-localstack.sh; else docker compose exec localstack awslocal s3 mb s3://buquet-dev || true; docker compose exec localstack awslocal s3api put-bucket-versioning --bucket buquet-dev --versioning-configuration Status=Enabled; fi
    S3_ENDPOINT=http://localhost:4566 S3_BUCKET=buquet-dev S3_REGION=us-east-1 \
      AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
      cargo test --tests --features integration

integration-py: up
    if command -v awslocal >/dev/null 2>&1; then ./scripts/init-localstack.sh; else docker compose exec localstack awslocal s3 mb s3://buquet-dev || true; docker compose exec localstack awslocal s3api put-bucket-versioning --bucket buquet-dev --versioning-configuration Status=Enabled; fi
    S3_ENDPOINT=http://localhost:4566 S3_BUCKET=buquet-dev S3_REGION=us-east-1 \
      AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
      just test-py-integration

# ============================================================================
# Docker
# ============================================================================

docker-build:
    docker build -t buquet:latest .

docker-run-worker:
    docker run --rm -it \
        -e S3_ENDPOINT -e S3_BUCKET -e S3_REGION \
        -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
        buquet:latest worker

docker-run-serve:
    docker run --rm -it -p 8080:8080 \
        -e S3_ENDPOINT -e S3_BUCKET -e S3_REGION \
        -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
        buquet:latest serve --port 8080

# ============================================================================
# Docker Compose
# ============================================================================

up:
    docker compose up -d

down:
    docker compose down

logs:
    docker compose logs -f

clean-docker:
    docker compose down -v

up-full:
    docker compose -f compose.full.yml up -d --build

down-full:
    docker compose -f compose.full.yml down

logs-full *ARGS:
    docker compose -f compose.full.yml logs -f {{ARGS}}
