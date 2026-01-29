# Workspace commands (delegates to crate-specific justfiles)

default:
    @just --list

# ============================================================================
# Combined commands
# ============================================================================

fmt: oq-fmt workflows-fmt
lint: oq-lint workflows-lint
check: oq-check workflows-check
test: oq-test workflows-test
typecheck: oq-typecheck workflows-typecheck
all: fmt lint check typecheck test
integration: integration-rs integration-py
load-test: oq-load-test
chaos-test: oq-chaos-test

integration-rs: up
    if command -v awslocal >/dev/null 2>&1; then ./scripts/init-localstack.sh; else docker compose exec localstack awslocal s3 mb s3://oq-dev || true; docker compose exec localstack awslocal s3api put-bucket-versioning --bucket oq-dev --versioning-configuration Status=Enabled; fi
    S3_ENDPOINT=http://localhost:4566 S3_BUCKET=oq-dev S3_REGION=us-east-1 \
      AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
      cargo test -p oq --tests --features integration

integration-py: up
    if command -v awslocal >/dev/null 2>&1; then ./scripts/init-localstack.sh; else docker compose exec localstack awslocal s3 mb s3://oq-dev || true; docker compose exec localstack awslocal s3api put-bucket-versioning --bucket oq-dev --versioning-configuration Status=Enabled; fi
    just -f crates/oq/justfile py-setup
    just -f crates/oq/justfile py-dev
    S3_ENDPOINT=http://localhost:4566 S3_BUCKET=oq-dev S3_REGION=us-east-1 \
      AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
      uv run --project crates/oq pytest crates/oq/python/tests -m integration

# ============================================================================
# oq crate (Rust + Python)
# ============================================================================

oq-fmt:
    just -f crates/oq/justfile fmt

oq-lint:
    just -f crates/oq/justfile lint

oq-check:
    just -f crates/oq/justfile check

oq-test:
    just -f crates/oq/justfile test

oq-load-test:
    just -f crates/oq/justfile load-test

oq-chaos-test:
    just -f crates/oq/justfile chaos-test

oq-typecheck:
    just -f crates/oq/justfile check-py

oq-run *ARGS:
    just -f crates/oq/justfile run -- {{ARGS}}

oq-dev *ARGS:
    just -f crates/oq/justfile dev -- {{ARGS}}

oq-serve PORT="8080":
    just -f crates/oq/justfile serve {{PORT}}

# Python helpers for oq
py-setup:
    just -f crates/oq/justfile py-setup

py-dev:
    just -f crates/oq/justfile py-dev

py-build:
    just -f crates/oq/justfile py-build

py-verify:
    just -f crates/oq/justfile py-verify

# ============================================================================
# oq-workflows crate
# ============================================================================

workflows-fmt:
    just -f crates/oq-workflows/justfile fmt

workflows-lint:
    just -f crates/oq-workflows/justfile lint

workflows-check:
    just -f crates/oq-workflows/justfile check

workflows-test:
    just -f crates/oq-workflows/justfile test

workflows-typecheck:
    just -f crates/oq-workflows/justfile pyright

# ============================================================================
# Docker
# ============================================================================

docker-build:
    docker build -t oq:latest .

docker-run-worker:
    docker run --rm -it \
        -e S3_ENDPOINT -e S3_BUCKET -e S3_REGION \
        -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
        oq:latest worker

docker-run-serve:
    docker run --rm -it -p 8080:8080 \
        -e S3_ENDPOINT -e S3_BUCKET -e S3_REGION \
        -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
        oq:latest serve --port 8080

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
