//! Test: Timeout monitor recovers stuck tasks.

use oq::models::TaskStatus;
use oq::queue::SubmitOptions;
use oq::worker::{claim_task, ClaimResult, MonitorConfig, TimeoutMonitor};
use serde_json::json;
use std::time::Duration;

use crate::common::{test_queue, unique_task_type};

#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_timeout_recovery() {
    let queue = test_queue().await;
    let task_type = unique_task_type();

    // Submit task with 1 second timeout
    let options = SubmitOptions {
        timeout_seconds: Some(1),
        ..Default::default()
    };
    let task = queue
        .submit(&task_type, json!({}), options)
        .await
        .expect("Failed to submit");

    // Claim the task
    let result = claim_task(&queue, task.id, "test-worker")
        .await
        .expect("Claim failed");
    let claimed = match result {
        ClaimResult::Claimed { task, etag: _ } => *task,
        other => panic!("Expected Claimed: {:?}", other),
    };

    assert_eq!(claimed.status, TaskStatus::Running);

    // Wait for timeout to expire
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Run timeout monitor
    let config = MonitorConfig {
        check_interval: Duration::from_millis(100),
        sweep_interval: None,
        sweep_page_size: 1000,
        ..Default::default()
    };
    let mut monitor = TimeoutMonitor::new(queue.clone(), config);

    let recovered = monitor
        .check_shard(&claimed.shard)
        .await
        .expect("Check failed");

    // Task should have been recovered
    assert!(
        recovered.contains(&task.id),
        "Task should be recovered. Recovered: {:?}",
        recovered
    );

    // Verify task is back to Pending
    let (updated, _) = queue
        .get(task.id)
        .await
        .expect("Get failed")
        .expect("Task not found");
    assert_eq!(updated.status, TaskStatus::Pending);
}
