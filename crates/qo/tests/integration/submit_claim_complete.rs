//! Test: Submit -> Claim -> Complete happy path.

use qo::models::TaskStatus;
use qo::queue::SubmitOptions;
use qo::worker::{claim_task, complete_task, ClaimResult};
use serde_json::json;

use crate::common::{test_queue, unique_task_type};

#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_submit_claim_complete() {
    let queue = test_queue().await;
    let task_type = unique_task_type();

    // Submit task
    let task = queue
        .submit(&task_type, json!({"test": true}), SubmitOptions::default())
        .await
        .expect("Failed to submit task");

    assert_eq!(task.status, TaskStatus::Pending);

    // Claim task
    let result = claim_task(&queue, task.id, "test-worker")
        .await
        .expect("Failed to claim");

    let (claimed_task, lease_id) = match result {
        ClaimResult::Claimed { task, etag: _ } => {
            let lease_id = task.lease_id.expect("Missing lease_id");
            (*task, lease_id)
        }
        other => panic!("Expected Claimed, got {:?}", other),
    };

    assert_eq!(claimed_task.status, TaskStatus::Running);
    assert_eq!(claimed_task.worker_id.as_deref(), Some("test-worker"));

    // Complete task
    let completed = complete_task(
        &queue,
        claimed_task.id,
        lease_id,
        json!({"result": "success"}),
    )
    .await
    .expect("Failed to complete");

    assert_eq!(completed.status, TaskStatus::Completed);
    assert!(completed.output.is_some());
}
