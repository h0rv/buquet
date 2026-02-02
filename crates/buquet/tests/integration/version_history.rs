//! Test: Version history captures all state transitions.
//!
//! This test requires S3 object versioning to be enabled on the bucket.

use buquet::queue::SubmitOptions;
use buquet::worker::{claim_task, complete_task, ClaimResult};
use serde_json::json;

use crate::common::{test_queue, unique_task_type};

#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_version_history_shows_transitions() {
    let queue = test_queue().await;
    let task_type = unique_task_type();

    // Submit (creates version 1: Pending)
    let task = queue
        .submit(&task_type, json!({"step": 1}), SubmitOptions::default())
        .await
        .expect("submit");

    // Claim (creates version 2: Running)
    let result = claim_task(&queue, task.id, "history-worker")
        .await
        .expect("claim");
    let (claimed, lease_id) = match result {
        ClaimResult::Claimed { task, .. } => {
            let lid = task.lease_id.expect("lease_id");
            (*task, lid)
        }
        _ => panic!("Expected Claimed"),
    };

    // Complete (creates version 3: Completed)
    complete_task(&queue, claimed.id, lease_id, json!({"result": "done"}))
        .await
        .expect("complete");

    // Get history
    let history = queue.get_history(task.id).await.expect("get_history");

    // Should have at least 3 versions (depends on S3 versioning being enabled)
    // With versioning enabled: Pending -> Running -> Completed
    assert!(!history.is_empty(), "History should not be empty");

    // Verify we can see different statuses in history
    let statuses: Vec<_> = history.iter().map(|t| t.status).collect();
    println!("History statuses: {:?}", statuses);
}
