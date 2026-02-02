//! Test: Task fails until it reaches Failed status.

use buquet::models::TaskStatus;
use buquet::queue::SubmitOptions;
use buquet::worker::{claim_task, fail_task, retry_task, ClaimResult};
use serde_json::json;

use crate::common::{test_queue, unique_task_type};

#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_retry_exhaustion() {
    let queue = test_queue().await;
    let task_type = unique_task_type();

    // Submit task with max_retries = 2
    let options = SubmitOptions {
        max_retries: Some(2),
        ..Default::default()
    };
    let task = queue
        .submit(&task_type, json!({}), options)
        .await
        .expect("Failed to submit");

    // Retry loop - we need 3 attempts total (initial + 2 retries)
    for attempt in 0..3 {
        // Wait for task to become available (backoff)
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Claim - might need to retry if task is in backoff
        let mut claim_result = None;
        for _ in 0..10 {
            let result = claim_task(&queue, task.id, "test-worker")
                .await
                .expect("Claim failed");

            match result {
                ClaimResult::Claimed { task, etag: _ } => {
                    claim_result = Some(*task);
                    break;
                }
                ClaimResult::NotAvailable => {
                    // Task might still be in backoff, wait and retry
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
                other => panic!("Unexpected claim result: {:?}", other),
            }
        }

        let claimed = claim_result.expect("Should have claimed task");
        let lease_id = claimed.lease_id.expect("Should have lease_id");

        if attempt < 2 {
            // Should retry
            let retried = retry_task(&queue, claimed.id, lease_id, "Test failure")
                .await
                .expect("Retry failed");
            assert_eq!(retried.status, TaskStatus::Pending);
        } else {
            // Should fail permanently
            let failed = fail_task(&queue, claimed.id, lease_id, "Final failure")
                .await
                .expect("Fail failed");
            assert_eq!(failed.status, TaskStatus::Failed);
            return;
        }
    }
}
