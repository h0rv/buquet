//! Common test utilities.
//!
//! Configuration is read from environment variables with defaults matching
//! `crates/oq/examples/.env.example`. To run tests:
//!
//! ```bash
//! source crates/oq/examples/.env.example
//! cargo test --test integration -- --ignored
//! ```

use oq::queue::Queue;
use oq::storage::{S3Client, S3Config};
use tokio::sync::OnceCell;
use uuid::Uuid;

static INIT_BUCKET: OnceCell<()> = OnceCell::const_new();

/// Creates a test queue using environment variables for configuration.
///
/// Environment variables (with defaults from .env.example):
/// - `S3_ENDPOINT`: S3 endpoint URL (default: `http://localhost:4566`)
/// - `S3_BUCKET`: Bucket name (default: `oq-dev`)
/// - `S3_REGION`: AWS region (default: `us-east-1`)
///
/// AWS credentials are read automatically by the AWS SDK from:
/// - `AWS_ACCESS_KEY_ID`
/// - `AWS_SECRET_ACCESS_KEY`
///
/// # Panics
///
/// Panics if the S3 client cannot be created (e.g., docker not running).
pub async fn test_queue() -> Queue {
    let endpoint =
        std::env::var("S3_ENDPOINT").unwrap_or_else(|_| "http://localhost:4566".to_string());
    let bucket = std::env::var("S3_BUCKET").unwrap_or_else(|_| "oq-dev".to_string());
    let region = std::env::var("S3_REGION").unwrap_or_else(|_| "us-east-1".to_string());

    let config = S3Config::new(Some(endpoint), bucket, region);
    let client = S3Client::new(config).await.expect(
        "Failed to create S3 client - is LocalStack running? Try: just up && source crates/oq/examples/.env.example",
    );
    INIT_BUCKET
        .get_or_init(|| async {
            if let Ok(keys) = client.list_objects_paginated("", 10_000).await {
                for key in keys {
                    let _ = client.delete_object(&key).await;
                }
            }
        })
        .await;
    Queue::new(client)
}

/// Generates a unique task type for test isolation.
#[must_use]
pub fn unique_task_type() -> String {
    format!("test-{}", Uuid::new_v4())
}
