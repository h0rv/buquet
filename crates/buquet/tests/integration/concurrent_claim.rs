//! Test: Multiple workers racing to claim the same task.
//!
//! This test validates CAS (compare-and-swap) behavior using S3's `If-Match`
//! conditional header. Only one worker should successfully claim the task.

use buquet::queue::SubmitOptions;
use buquet::worker::{claim_task, ClaimResult};
use serde_json::json;
use tokio::task::JoinSet;

use crate::common::{test_queue, unique_task_type};

#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_concurrent_claim() {
    let queue = test_queue().await;
    let task_type = unique_task_type();

    // Submit one task
    let task = queue
        .submit(&task_type, json!({"test": true}), SubmitOptions::default())
        .await
        .expect("Failed to submit task");

    // Race 10 workers to claim it
    let mut set = JoinSet::new();
    for i in 0..10 {
        let q = queue.clone();
        let tid = task.id;
        set.spawn(async move {
            let worker_id = format!("worker-{}", i);
            claim_task(&q, tid, &worker_id).await
        });
    }

    // Collect results
    let mut claimed_count = 0;
    let mut already_claimed_count = 0;

    while let Some(result) = set.join_next().await {
        match result.expect("Task panicked") {
            Ok(ClaimResult::Claimed { .. }) => claimed_count += 1,
            Ok(ClaimResult::AlreadyClaimed) => already_claimed_count += 1,
            Ok(ClaimResult::NotAvailable) => {
                // Task might have been claimed and transitioned already
                already_claimed_count += 1;
            }
            Ok(other) => panic!("Unexpected result: {:?}", other),
            Err(e) => panic!("Error: {:?}", e),
        }
    }

    // Exactly one worker should have claimed it
    assert_eq!(claimed_count, 1, "Exactly one worker should claim the task");
    assert_eq!(
        already_claimed_count, 9,
        "9 workers should get AlreadyClaimed or NotAvailable"
    );
}
