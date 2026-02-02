//! Load tests for buquet task queue.
//!
//! These tests stress-test the queue under concurrent load to validate
//! throughput, latency, and S3 operation efficiency.
//!
//! Configuration is read from environment variables. To run:
//!
//! ```bash
//! source crates/buquet/examples/.env.example
//! cargo test -p buquet --test load --release --features load-tests -- --nocapture
//! ```

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod metrics;
mod producer;
mod worker;

use crate::metrics::LoadMetrics;
use crate::producer::Producer;
use crate::worker::LoadWorker;
use buquet::queue::Queue;
use buquet::storage::{S3Client, S3Config};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::watch;
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
    format!("load-test-{}", Uuid::new_v4())
}

/// Configuration for load tests.
#[derive(Debug, Clone)]
pub struct LoadTestConfig {
    /// Number of concurrent producers.
    pub num_producers: usize,
    /// Number of concurrent workers.
    pub num_workers: usize,
    /// Total number of tasks to submit.
    pub task_count: usize,
    /// Maximum delay between task submissions (in milliseconds).
    pub max_submit_delay_ms: u64,
    /// Minimum processing time per task (in milliseconds).
    pub min_process_time_ms: u64,
    /// Maximum processing time per task (in milliseconds).
    pub max_process_time_ms: u64,
    /// How long to wait for all tasks to complete.
    pub timeout: Duration,
}

impl Default for LoadTestConfig {
    fn default() -> Self {
        Self {
            num_producers: 3,
            num_workers: 5,
            task_count: 100,
            max_submit_delay_ms: 50,
            min_process_time_ms: 10,
            max_process_time_ms: 100,
            timeout: Duration::from_secs(120),
        }
    }
}

/// Runs a load test with the given configuration.
async fn run_load_test(config: LoadTestConfig) -> LoadMetrics {
    let queue = test_queue().await;
    let task_type = unique_task_type();
    let metrics = Arc::new(LoadMetrics::new());

    // Shutdown channel
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let start = Instant::now();

    // Calculate tasks per producer
    let tasks_per_producer = config.task_count / config.num_producers;
    let extra_tasks = config.task_count % config.num_producers;

    // Spawn producers
    let mut producer_handles = Vec::new();
    for i in 0..config.num_producers {
        let producer = Producer::new(
            queue.clone(),
            task_type.clone(),
            metrics.clone(),
            config.max_submit_delay_ms,
        );

        // Distribute tasks evenly, with remainder going to first producers
        let task_count = if i < extra_tasks {
            tasks_per_producer + 1
        } else {
            tasks_per_producer
        };

        let handle = tokio::spawn(async move { producer.run(task_count).await });
        producer_handles.push(handle);
    }

    // Wait for all producers to finish
    for handle in producer_handles {
        if let Err(e) = handle.await {
            eprintln!("Producer failed: {e:?}");
        }
    }

    let submission_time = start.elapsed();
    println!(
        "All {} tasks submitted in {:?}",
        config.task_count, submission_time
    );

    // Spawn workers
    let mut worker_handles = Vec::new();
    for i in 0..config.num_workers {
        let worker = LoadWorker::new(
            queue.clone(),
            format!("load-worker-{i}"),
            task_type.clone(),
            metrics.clone(),
            config.min_process_time_ms,
            config.max_process_time_ms,
        );

        let rx = shutdown_rx.clone();
        let handle = tokio::spawn(async move { worker.run(rx).await });
        worker_handles.push(handle);
    }

    // Wait for all tasks to be processed or timeout
    let deadline = Instant::now() + config.timeout;
    loop {
        let completed = metrics.completed_tasks();
        let failed = metrics.failed_tasks();
        let total = completed + failed;

        if total >= config.task_count {
            break;
        }

        if Instant::now() > deadline {
            eprintln!(
                "Timeout reached. Completed: {}, Failed: {}, Expected: {}",
                completed, failed, config.task_count
            );
            break;
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Signal workers to stop
    let _ = shutdown_tx.send(true);

    // Wait for workers to finish
    for handle in worker_handles {
        let _ = handle.await;
    }

    let total_time = start.elapsed();
    println!("Load test completed in {total_time:?}");

    // Return a copy of metrics
    Arc::try_unwrap(metrics).unwrap_or_else(|arc| (*arc).clone())
}

#[tokio::test]
#[cfg_attr(
    not(feature = "load-tests"),
    ignore = "requires load-tests feature (S3)"
)]
async fn test_basic_load() {
    let config = LoadTestConfig {
        num_producers: 2,
        num_workers: 3,
        task_count: 50,
        max_submit_delay_ms: 20,
        min_process_time_ms: 5,
        max_process_time_ms: 20,
        timeout: Duration::from_secs(60),
    };

    let metrics = run_load_test(config.clone()).await;

    // Print results
    println!("\n=== Load Test Results ===");
    println!("Configuration:");
    println!("  Producers: {}", config.num_producers);
    println!("  Workers: {}", config.num_workers);
    println!("  Task count: {}", config.task_count);
    println!();
    metrics.print_report();

    // Assertions
    let completed = metrics.completed_tasks();
    let failed = metrics.failed_tasks();
    assert!(
        completed + failed >= config.task_count,
        "Not all tasks processed: completed={}, failed={}, expected={}",
        completed,
        failed,
        config.task_count
    );
}

#[tokio::test]
#[cfg_attr(
    not(feature = "load-tests"),
    ignore = "requires load-tests feature (S3)"
)]
async fn test_high_contention() {
    // Many workers competing for few tasks
    let config = LoadTestConfig {
        num_producers: 1,
        num_workers: 10,
        task_count: 30,
        max_submit_delay_ms: 100,
        min_process_time_ms: 50,
        max_process_time_ms: 100,
        timeout: Duration::from_secs(60),
    };

    let metrics = run_load_test(config.clone()).await;

    println!("\n=== High Contention Test Results ===");
    println!("Configuration:");
    println!("  Producers: {}", config.num_producers);
    println!("  Workers: {}", config.num_workers);
    println!("  Task count: {}", config.task_count);
    println!();
    metrics.print_report();

    // With high contention, we expect claim conflicts
    let conflicts = metrics.claim_conflicts();
    println!("Claim conflicts (expected due to high contention): {conflicts}");

    let completed = metrics.completed_tasks();
    let failed = metrics.failed_tasks();
    assert!(
        completed + failed >= config.task_count,
        "Not all tasks processed"
    );
}

#[tokio::test]
#[cfg_attr(
    not(feature = "load-tests"),
    ignore = "requires load-tests feature (S3)"
)]
async fn test_high_throughput() {
    // Many producers, many workers, many tasks
    let config = LoadTestConfig {
        num_producers: 5,
        num_workers: 10,
        task_count: 200,
        max_submit_delay_ms: 10,
        min_process_time_ms: 5,
        max_process_time_ms: 20,
        timeout: Duration::from_secs(120),
    };

    let metrics = run_load_test(config.clone()).await;

    println!("\n=== High Throughput Test Results ===");
    println!("Configuration:");
    println!("  Producers: {}", config.num_producers);
    println!("  Workers: {}", config.num_workers);
    println!("  Task count: {}", config.task_count);
    println!();
    metrics.print_report();

    let completed = metrics.completed_tasks();
    let failed = metrics.failed_tasks();
    assert!(
        completed + failed >= config.task_count,
        "Not all tasks processed"
    );

    // Check throughput is reasonable
    let throughput = metrics.throughput_tasks_per_sec();
    println!("Throughput: {throughput:.2} tasks/sec");
    assert!(throughput > 1.0, "Throughput too low: {throughput}");
}
