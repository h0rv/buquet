# Workspace commands (delegates to crate-specific justfiles)

default:
    @just --list

# ============================================================================
# Combined commands
# ============================================================================

fmt: buquet-fmt workflows-fmt
lint: buquet-lint workflows-lint
check: buquet-check workflows-check
test: buquet-test workflows-test
typecheck: buquet-typecheck workflows-typecheck
all: fmt lint check typecheck test
integration: integration-rs integration-py
load-test: buquet-load-test
chaos-test: buquet-chaos-test

integration-rs: up
    if command -v awslocal >/dev/null 2>&1; then ./scripts/init-localstack.sh; else docker compose exec localstack awslocal s3 mb s3://buquet-dev || true; docker compose exec localstack awslocal s3api put-bucket-versioning --bucket buquet-dev --versioning-configuration Status=Enabled; fi
    S3_ENDPOINT=http://localhost:4566 S3_BUCKET=buquet-dev S3_REGION=us-east-1 \
      AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
      cargo test -p buquet --tests --features integration

integration-py: up
    if command -v awslocal >/dev/null 2>&1; then ./scripts/init-localstack.sh; else docker compose exec localstack awslocal s3 mb s3://buquet-dev || true; docker compose exec localstack awslocal s3api put-bucket-versioning --bucket buquet-dev --versioning-configuration Status=Enabled; fi
    S3_ENDPOINT=http://localhost:4566 S3_BUCKET=buquet-dev S3_REGION=us-east-1 \
      AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
      just -f crates/buquet/justfile test-py-integration

# ============================================================================
# buquet crate (Rust + Python)
# ============================================================================

buquet-fmt:
    just -f crates/buquet/justfile fmt

buquet-lint:
    just -f crates/buquet/justfile lint

buquet-check:
    just -f crates/buquet/justfile check

buquet-test:
    just -f crates/buquet/justfile test

buquet-load-test:
    just -f crates/buquet/justfile load-test

buquet-chaos-test:
    just -f crates/buquet/justfile chaos-test

buquet-typecheck:
    just -f crates/buquet/justfile check-py

buquet-run *ARGS:
    just -f crates/buquet/justfile run -- {{ARGS}}

buquet-dev *ARGS:
    just -f crates/buquet/justfile dev -- {{ARGS}}

buquet-serve PORT="8080":
    just -f crates/buquet/justfile serve {{PORT}}

# Python helpers for buquet
py-setup:
    just -f crates/buquet/justfile py-setup

py-dev:
    just -f crates/buquet/justfile py-dev

py-build:
    just -f crates/buquet/justfile py-build

py-verify:
    just -f crates/buquet/justfile py-verify

# ============================================================================
# buquet-workflow crate
# ============================================================================

workflows-fmt:
    just -f crates/buquet-workflow/justfile fmt

workflows-lint:
    just -f crates/buquet-workflow/justfile lint

workflows-check:
    just -f crates/buquet-workflow/justfile check

workflows-test:
    just -f crates/buquet-workflow/justfile test

workflows-typecheck:
    just -f crates/buquet-workflow/justfile pyright

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
