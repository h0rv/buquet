//! Chaos test: Worker crash mid-task.
//!
//! This test simulates a worker crashing while processing a task
//! and verifies that the timeout monitor recovers the task.

use crate::{test_queue, unique_task_type};
use oq::models::TaskStatus;
use oq::queue::SubmitOptions;
use oq::worker::{claim_task, ClaimResult, MonitorConfig, TimeoutMonitor};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
#[cfg_attr(
    not(feature = "chaos-tests"),
    ignore = "requires chaos-tests feature (S3)"
)]
async fn test_worker_crash_recovery() {
    let queue = test_queue().await;
    let task_type = unique_task_type();

    println!("=== Worker Crash Recovery Test ===");
    println!("Task type: {}", task_type);

    // Submit a task with a short timeout (1 second)
    let options = SubmitOptions {
        timeout_seconds: Some(1),
        max_retries: Some(3),
        ..Default::default()
    };
    let task = queue
        .submit(&task_type, json!({"test": "crash_recovery"}), options)
        .await
        .expect("Failed to submit task");

    println!("Submitted task: {}", task.id);
    assert_eq!(task.status, TaskStatus::Pending);

    // Claim the task (simulating worker starting work)
    let result = claim_task(&queue, task.id, "crashing-worker")
        .await
        .expect("Claim failed");

    let claimed = match result {
        ClaimResult::Claimed { task, etag: _ } => *task,
        other => panic!("Expected Claimed, got {:?}", other),
    };

    println!(
        "Claimed task: {} (status: {:?})",
        claimed.id, claimed.status
    );
    assert_eq!(claimed.status, TaskStatus::Running);
    assert_eq!(claimed.worker_id, Some("crashing-worker".to_string()));

    // "Crash" the worker by simply not completing the task
    // Wait for the timeout to expire
    println!("Simulating worker crash (waiting for timeout to expire)...");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Run the timeout monitor to recover the task
    let config = MonitorConfig {
        check_interval: Duration::from_millis(100),
        sweep_interval: None,
        sweep_page_size: 1000,
        ..Default::default()
    };
    let mut monitor = TimeoutMonitor::new(queue.clone(), config);

    println!("Running timeout monitor...");
    let recovered = monitor
        .check_shard(&claimed.shard)
        .await
        .expect("Monitor check failed");

    println!("Recovered tasks: {:?}", recovered);
    assert!(
        recovered.contains(&task.id),
        "Task {} should have been recovered. Recovered: {:?}",
        task.id,
        recovered
    );

    // Verify the task is back to Pending status
    let (updated, _) = queue
        .get(task.id)
        .await
        .expect("Get failed")
        .expect("Task not found");

    println!(
        "Task after recovery: status={:?}, retry_count={}, last_error={:?}",
        updated.status, updated.retry_count, updated.last_error
    );

    assert_eq!(updated.status, TaskStatus::Pending);
    assert_eq!(updated.retry_count, 1); // Should have incremented
    assert!(updated.last_error.is_some());
    assert!(updated.last_error.as_ref().unwrap().contains("Timeout"));
    assert!(updated.worker_id.is_none()); // Worker should be cleared
    assert!(updated.lease_id.is_none()); // Lease should be cleared

    println!("Worker crash recovery test PASSED");
}

#[tokio::test]
#[cfg_attr(
    not(feature = "chaos-tests"),
    ignore = "requires chaos-tests feature (S3)"
)]
async fn test_repeated_crashes_exhaust_retries() {
    let queue = test_queue().await;
    let task_type = unique_task_type();

    println!("=== Repeated Crashes Exhaust Retries Test ===");
    println!("Task type: {}", task_type);

    // Submit a task with short timeout and limited retries
    let options = SubmitOptions {
        timeout_seconds: Some(1),
        max_retries: Some(2), // Only 2 retries
        ..Default::default()
    };
    let task = queue
        .submit(&task_type, json!({"test": "exhaust_retries"}), options)
        .await
        .expect("Failed to submit task");

    println!("Submitted task: {} (max_retries=2)", task.id);

    let config = MonitorConfig {
        check_interval: Duration::from_millis(100),
        sweep_interval: None,
        sweep_page_size: 1000,
        ..Default::default()
    };
    let mut monitor = TimeoutMonitor::new(queue.clone(), config);

    // Simulate multiple crashes and recoveries
    for attempt in 1..=3 {
        println!("\n--- Attempt {} ---", attempt);

        // Get current task state
        let (current, _) = queue
            .get(task.id)
            .await
            .expect("Get failed")
            .expect("Task not found");

        if current.status == TaskStatus::Failed {
            println!("Task already failed after {} attempts", attempt - 1);
            break;
        }

        // Wait for task to become available (backoff may have set available_at in the future)
        // Use queue time to avoid clock drift issues between local time and S3 time
        let now = queue.now().await.expect("Failed to get queue time");
        if current.available_at > now {
            let wait_time = (current.available_at - now)
                .to_std()
                .unwrap_or(Duration::from_millis(100));
            println!("Waiting {:?} for backoff to expire...", wait_time);
            tokio::time::sleep(wait_time + Duration::from_millis(200)).await;
        }

        // Claim the task
        let result = claim_task(&queue, task.id, &format!("crashing-worker-{}", attempt))
            .await
            .expect("Claim failed");

        match result {
            ClaimResult::Claimed { task: claimed, .. } => {
                println!(
                    "Claimed by crashing-worker-{} (retry_count={})",
                    attempt, claimed.retry_count
                );
            }
            ClaimResult::NotAvailable => {
                println!("Task not available (already failed or not ready)");
                break;
            }
            other => {
                println!("Unexpected claim result: {:?}", other);
                break;
            }
        }

        // "Crash" by waiting for timeout
        println!("Simulating crash...");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Run monitor to recover
        let recovered = monitor
            .check_shard(&task.shard)
            .await
            .expect("Monitor check failed");
        println!("Monitor recovered {} tasks", recovered.len());
    }

    // Verify final task state
    let (final_task, _) = queue
        .get(task.id)
        .await
        .expect("Get failed")
        .expect("Task not found");

    println!("\n=== Final Task State ===");
    println!("Status: {:?}", final_task.status);
    println!("Retry count: {}", final_task.retry_count);
    println!("Max retries: {}", final_task.max_retries);
    println!("Last error: {:?}", final_task.last_error);

    // After exhausting retries, task should be Failed
    assert_eq!(
        final_task.status,
        TaskStatus::Failed,
        "Task should be Failed after exhausting retries"
    );
    assert!(final_task.completed_at.is_some());
    assert!(final_task
        .last_error
        .as_ref()
        .unwrap()
        .contains("max retries"));

    println!("\nRepeated crashes exhaust retries test PASSED");
}

#[tokio::test]
#[cfg_attr(
    not(feature = "chaos-tests"),
    ignore = "requires chaos-tests feature (S3)"
)]
async fn test_crash_with_concurrent_monitor() {
    let queue = test_queue().await;
    let task_type = unique_task_type();

    println!("=== Concurrent Monitor Recovery Test ===");

    // Submit multiple tasks
    let mut task_ids = Vec::new();
    for i in 0..5 {
        let options = SubmitOptions {
            timeout_seconds: Some(1),
            ..Default::default()
        };
        let task = queue
            .submit(&task_type, json!({"index": i}), options)
            .await
            .expect("Failed to submit task");
        task_ids.push(task.id);
    }
    println!("Submitted {} tasks", task_ids.len());

    // Claim all tasks
    for (i, task_id) in task_ids.iter().enumerate() {
        let result = claim_task(&queue, *task_id, &format!("worker-{}", i))
            .await
            .expect("Claim failed");
        assert!(matches!(result, ClaimResult::Claimed { .. }));
    }
    println!("All tasks claimed by different workers");

    // Wait for timeout
    println!("Waiting for timeouts to expire...");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Run monitor to recover all tasks
    let config = MonitorConfig {
        check_interval: Duration::from_millis(100),
        sweep_interval: None,
        sweep_page_size: 1000,
        ..Default::default()
    };
    let mut monitor = TimeoutMonitor::new(queue.clone(), config);

    let recovered = monitor
        .check_all_shards()
        .await
        .expect("Monitor check failed");

    println!("Recovered {} tasks", recovered.len());

    // Verify all tasks were recovered
    for task_id in &task_ids {
        let (task, _) = queue
            .get(*task_id)
            .await
            .expect("Get failed")
            .expect("Task not found");

        assert_eq!(
            task.status,
            TaskStatus::Pending,
            "Task {} should be Pending",
            task_id
        );
        assert_eq!(task.retry_count, 1);
    }

    println!("Concurrent monitor recovery test PASSED");
}
