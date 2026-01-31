# Workspace commands (delegates to crate-specific justfiles)

default:
    @just --list

# ============================================================================
# Combined commands
# ============================================================================

fmt: qo-fmt workflows-fmt
lint: qo-lint workflows-lint
check: qo-check workflows-check
test: qo-test workflows-test
typecheck: qo-typecheck workflows-typecheck
all: fmt lint check typecheck test
integration: integration-rs integration-py
load-test: qo-load-test
chaos-test: qo-chaos-test

integration-rs: up
    if command -v awslocal >/dev/null 2>&1; then ./scripts/init-localstack.sh; else docker compose exec localstack awslocal s3 mb s3://qo-dev || true; docker compose exec localstack awslocal s3api put-bucket-versioning --bucket qo-dev --versioning-configuration Status=Enabled; fi
    S3_ENDPOINT=http://localhost:4566 S3_BUCKET=qo-dev S3_REGION=us-east-1 \
      AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
      cargo test -p qo --tests --features integration

integration-py: up
    if command -v awslocal >/dev/null 2>&1; then ./scripts/init-localstack.sh; else docker compose exec localstack awslocal s3 mb s3://qo-dev || true; docker compose exec localstack awslocal s3api put-bucket-versioning --bucket qo-dev --versioning-configuration Status=Enabled; fi
    S3_ENDPOINT=http://localhost:4566 S3_BUCKET=qo-dev S3_REGION=us-east-1 \
      AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
      just -f crates/qo/justfile test-py-integration

# ============================================================================
# qo crate (Rust + Python)
# ============================================================================

qo-fmt:
    just -f crates/qo/justfile fmt

qo-lint:
    just -f crates/qo/justfile lint

qo-check:
    just -f crates/qo/justfile check

qo-test:
    just -f crates/qo/justfile test

qo-load-test:
    just -f crates/qo/justfile load-test

qo-chaos-test:
    just -f crates/qo/justfile chaos-test

qo-typecheck:
    just -f crates/qo/justfile check-py

qo-run *ARGS:
    just -f crates/qo/justfile run -- {{ARGS}}

qo-dev *ARGS:
    just -f crates/qo/justfile dev -- {{ARGS}}

qo-serve PORT="8080":
    just -f crates/qo/justfile serve {{PORT}}

# Python helpers for qo
py-setup:
    just -f crates/qo/justfile py-setup

py-dev:
    just -f crates/qo/justfile py-dev

py-build:
    just -f crates/qo/justfile py-build

py-verify:
    just -f crates/qo/justfile py-verify

# ============================================================================
# qow crate
# ============================================================================

workflows-fmt:
    just -f crates/qow/justfile fmt

workflows-lint:
    just -f crates/qow/justfile lint

workflows-check:
    just -f crates/qow/justfile check

workflows-test:
    just -f crates/qow/justfile test

workflows-typecheck:
    just -f crates/qow/justfile pyright

# ============================================================================
# Docker
# ============================================================================

docker-build:
    docker build -t qo:latest .

docker-run-worker:
    docker run --rm -it \
        -e S3_ENDPOINT -e S3_BUCKET -e S3_REGION \
        -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
        qo:latest worker

docker-run-serve:
    docker run --rm -it -p 8080:8080 \
        -e S3_ENDPOINT -e S3_BUCKET -e S3_REGION \
        -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
        qo:latest serve --port 8080

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
