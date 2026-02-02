//! Chaos tests for buquet task queue.
//!
//! These tests validate failure recovery scenarios:
//! - Worker crashes mid-task (timeout recovery)
//! - Slow storage operations (timeout handling)
//!
//! Configuration is read from environment variables. To run:
//!
//! ```bash
//! source crates/buquet/examples/.env.example
//! cargo test -p buquet --test chaos --release --features chaos-tests -- --nocapture
//! ```

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod slow_storage;
mod worker_crash;

use buquet::queue::Queue;
use buquet::storage::{S3Client, S3Config};
use uuid::Uuid;

/// Creates a test queue using environment variables for configuration.
///
/// Environment variables (with defaults from .env.example):
/// - `S3_ENDPOINT`: S3 endpoint URL (default: `http://localhost:4566`)
/// - `S3_BUCKET`: Bucket name (default: `buquet-dev`)
/// - `S3_REGION`: AWS region (default: `us-east-1`)
async fn test_queue() -> Queue {
    let endpoint =
        std::env::var("S3_ENDPOINT").unwrap_or_else(|_| "http://localhost:4566".to_string());
    let bucket = std::env::var("S3_BUCKET").unwrap_or_else(|_| "buquet-dev".to_string());
    let region = std::env::var("S3_REGION").unwrap_or_else(|_| "us-east-1".to_string());

    let config = S3Config::new(Some(endpoint), bucket, region);
    let client = S3Client::new(config).await.expect(
        "Failed to create S3 client - is LocalStack running? Try: just up && source crates/buquet/examples/.env.example",
    );
    Queue::new(client)
}

/// Generates a unique task type for test isolation.
fn unique_task_type() -> String {
    format!("chaos-test-{}", Uuid::new_v4())
}
