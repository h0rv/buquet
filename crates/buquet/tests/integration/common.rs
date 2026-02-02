//! Common test utilities.
//!
//! Configuration is read from environment variables with defaults matching
//! `crates/buquet/examples/.env.example`. To run tests:
//!
//! ```bash
//! source crates/buquet/examples/.env.example
//! cargo test --test integration -- --ignored
//! ```

use buquet::queue::Queue;
use buquet::storage::{S3Client, S3Config};
use tokio::sync::OnceCell;
use uuid::Uuid;

static INIT_BUCKET: OnceCell<()> = OnceCell::const_new();

/// Creates a test queue using environment variables for configuration.
///
/// **For tests only:** Defaults to LocalStack for developer convenience.
/// The library itself defaults to real AWS when no endpoint is specified.
///
/// Environment variables:
/// - `S3_ENDPOINT`: S3 endpoint URL (test default: `http://localhost:4566`)
/// - `S3_BUCKET`: Bucket name (test default: `buquet-dev`)
/// - `S3_REGION`: AWS region (test default: `us-east-1`)
///
/// To test against real AWS:
/// ```bash
/// S3_ENDPOINT= S3_BUCKET=my-bucket S3_REGION=us-east-2 cargo test ...
/// ```
///
/// # Panics
///
/// Panics if the S3 client cannot be created.
pub async fn test_queue() -> Queue {
    // TEST-ONLY: Default to LocalStack for developer convenience.
    // The library itself does NOT have this default - it uses real AWS.
    let endpoint = match std::env::var("S3_ENDPOINT") {
        Ok(val) if val.is_empty() => None, // Explicitly empty = real AWS
        Ok(val) => Some(val),              // Has value = use that endpoint
        Err(_) => Some("http://localhost:4566".to_string()), // Test default = LocalStack
    };
    let bucket = std::env::var("S3_BUCKET").unwrap_or_else(|_| "buquet-dev".to_string());
    let region = std::env::var("S3_REGION").unwrap_or_else(|_| "us-east-1".to_string());

    eprintln!("Test config: endpoint={endpoint:?}, bucket={bucket}, region={region}");

    let config = S3Config::new(endpoint, bucket, region);
    let client = S3Client::new(config).await.expect(
        "Failed to create S3 client - is LocalStack running? Try: just up && source crates/buquet/examples/.env.example",
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
