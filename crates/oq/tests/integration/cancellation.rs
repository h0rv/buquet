//! Test: Task cancellation functionality.
//!
//! Blind tests based on the spec in docs/features/task-cancellation.md.
//! These tests verify the documented behavior without looking at implementation.

use oq::models::TaskStatus;
use oq::queue::SubmitOptions;
use oq::worker::{claim_task, ClaimResult};
use serde_json::json;
use uuid::Uuid;

use crate::common::{test_queue, unique_task_type};

#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_cancel_pending_task() {
    // From spec: "Immediate: CAS update from Pending to Cancelled"
    let queue = test_queue().await;
    let task_type = unique_task_type();

    // Submit a task
    let task = queue
        .submit(&task_type, json!({"test": true}), SubmitOptions::default())
        .await
        .expect("Failed to submit task");

    assert_eq!(task.status, TaskStatus::Pending);

    // Cancel the task
    let cancelled = queue.cancel(task.id).await.expect("Failed to cancel task");

    // Verify status is Cancelled
    assert_eq!(cancelled.status, TaskStatus::Cancelled);

    // Verify by fetching fresh copy
    let (fetched, _etag) = queue
        .get(task.id)
        .await
        .expect("Failed to get task")
        .expect("Task not found");

    assert_eq!(fetched.status, TaskStatus::Cancelled);
}

#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_cancel_with_reason() {
    // From spec: "Cancel with reason (stored in last_error)"
    // queue.cancel(task_id, reason="User requested cancellation")
    let queue = test_queue().await;
    let task_type = unique_task_type();

    let task = queue
        .submit(&task_type, json!({}), SubmitOptions::default())
        .await
        .expect("Failed to submit task");

    // Cancel with reason
    let reason = "User requested cancellation";
    let cancelled = queue
        .cancel_with_reason(task.id, Some(reason), None)
        .await
        .expect("Failed to cancel with reason");

    assert_eq!(cancelled.status, TaskStatus::Cancelled);

    // Verify reason is stored in last_error
    assert!(
        cancelled.last_error.as_deref() == Some(reason),
        "Expected last_error to contain reason. Got: {:?}",
        cancelled.last_error
    );
}

#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_cancel_non_pending_task_fails() {
    // From spec: "Returns error. Cannot cancel completed/failed/archived tasks."
    // Also: "if task.status != TaskStatus::Pending { return Err(CannotCancel { status: task.status }); }"
    let queue = test_queue().await;
    let task_type = unique_task_type();

    let task = queue
        .submit(&task_type, json!({}), SubmitOptions::default())
        .await
        .expect("Failed to submit task");

    // Claim the task to make it Running
    let result = claim_task(&queue, task.id, "test-worker")
        .await
        .expect("Failed to claim");

    match result {
        ClaimResult::Claimed { task: claimed, .. } => {
            assert_eq!(claimed.status, TaskStatus::Running);
        }
        other => panic!("Expected Claimed, got {:?}", other),
    }

    // Try to cancel the running task - should fail
    let cancel_result = queue.cancel(task.id).await;

    assert!(
        cancel_result.is_err(),
        "Cancelling a running task should fail. Got: {:?}",
        cancel_result
    );

    // Error should indicate cannot cancel
    let err = cancel_result.unwrap_err();
    let err_str = err.to_string().to_lowercase();
    assert!(
        err_str.contains("cannot cancel")
            || err_str.contains("running")
            || err_str.contains("invalid"),
        "Error should indicate cannot cancel running task. Got: {}",
        err
    );
}

#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_cancel_nonexistent_task_fails() {
    // From spec: Cancelling non-existent task should fail with NotFound
    let queue = test_queue().await;

    // Try to cancel a random UUID that doesn't exist
    let random_id = Uuid::new_v4();
    let result = queue.cancel(random_id).await;

    assert!(
        result.is_err(),
        "Cancelling non-existent task should fail. Got: {:?}",
        result
    );

    let err = result.unwrap_err();
    let err_str = err.to_string().to_lowercase();
    assert!(
        err_str.contains("not found") || err_str.contains("notfound"),
        "Error should indicate task not found. Got: {}",
        err
    );
}

#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_cancel_already_cancelled_is_idempotent() {
    // From spec: "Idempotent success. Returns current task state."
    let queue = test_queue().await;
    let task_type = unique_task_type();

    let task = queue
        .submit(&task_type, json!({}), SubmitOptions::default())
        .await
        .expect("Failed to submit task");

    // Cancel once
    let cancelled1 = queue
        .cancel(task.id)
        .await
        .expect("First cancel should succeed");
    assert_eq!(cancelled1.status, TaskStatus::Cancelled);

    // Cancel again - should be idempotent (succeed or return current state)
    let result2 = queue.cancel(task.id).await;

    // Should either succeed or return the already-cancelled task
    match result2 {
        Ok(cancelled2) => {
            assert_eq!(cancelled2.status, TaskStatus::Cancelled);
            assert_eq!(cancelled1.id, cancelled2.id);
        }
        Err(e) => {
            // If it errors, it should indicate already cancelled, not a failure
            let err_str = e.to_string().to_lowercase();
            assert!(
                err_str.contains("cancelled") || err_str.contains("idempotent"),
                "If cancelling already-cancelled task errors, it should be informative. Got: {}",
                e
            );
        }
    }
}

#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_cancelled_task_not_claimed() {
    // From spec: "Workers already skip non-Pending tasks"
    // "if task.status == TaskStatus::Cancelled { continue; }"
    let queue = test_queue().await;
    let task_type = unique_task_type();

    let task = queue
        .submit(&task_type, json!({}), SubmitOptions::default())
        .await
        .expect("Failed to submit task");

    // Cancel the task
    let cancelled = queue.cancel(task.id).await.expect("Failed to cancel task");
    assert_eq!(cancelled.status, TaskStatus::Cancelled);

    // Try to claim the cancelled task - should fail or return NotAvailable
    let claim_result = claim_task(&queue, task.id, "test-worker")
        .await
        .expect("Claim request failed");

    match claim_result {
        ClaimResult::Claimed { .. } => {
            panic!("Should not be able to claim a cancelled task");
        }
        ClaimResult::NotAvailable | ClaimResult::AlreadyClaimed => {
            // Expected - cancelled tasks should not be claimable
        }
        other => {
            // Any other result that indicates task wasn't claimed is acceptable
            panic!("Unexpected claim result for cancelled task: {:?}", other);
        }
    }

    // Verify task is still cancelled (not transitioned to Running)
    let (fetched, _etag) = queue
        .get(task.id)
        .await
        .expect("Failed to get task")
        .expect("Task not found");

    assert_eq!(
        fetched.status,
        TaskStatus::Cancelled,
        "Cancelled task should remain cancelled after claim attempt"
    );
}

#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_cancelled_task_has_cancelled_at_timestamp() {
    // From spec: Task fields include "cancelled_at": "2026-01-28T12:00:00Z"
    let queue = test_queue().await;
    let task_type = unique_task_type();

    let task = queue
        .submit(&task_type, json!({}), SubmitOptions::default())
        .await
        .expect("Failed to submit task");

    let before_cancel = queue
        .now()
        .await
        .expect("Failed to fetch authoritative queue time");

    let cancelled = queue.cancel(task.id).await.expect("Failed to cancel task");

    let after_cancel = queue
        .now()
        .await
        .expect("Failed to fetch authoritative queue time");

    // Verify cancelled_at is set and reasonable
    assert!(
        cancelled.cancelled_at.is_some(),
        "Cancelled task should have cancelled_at timestamp"
    );

    let cancelled_at = cancelled.cancelled_at.unwrap();
    assert!(
        cancelled_at >= before_cancel && cancelled_at <= after_cancel,
        "cancelled_at should be between before and after cancel. Got: {:?}",
        cancelled_at
    );
}

#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_cancel_completed_task_fails() {
    // From spec: "Returns error. Cannot cancel completed/failed/archived tasks."
    use oq::worker::complete_task;

    let queue = test_queue().await;
    let task_type = unique_task_type();

    let task = queue
        .submit(&task_type, json!({}), SubmitOptions::default())
        .await
        .expect("Failed to submit task");

    // Claim and complete the task
    let result = claim_task(&queue, task.id, "test-worker")
        .await
        .expect("Failed to claim");

    let (claimed_task, lease_id) = match result {
        ClaimResult::Claimed { task, .. } => {
            let lease_id = task.lease_id.expect("Missing lease_id");
            (*task, lease_id)
        }
        other => panic!("Expected Claimed, got {:?}", other),
    };

    let completed = complete_task(&queue, claimed_task.id, lease_id, json!({"result": "done"}))
        .await
        .expect("Failed to complete task");

    assert_eq!(completed.status, TaskStatus::Completed);

    // Try to cancel the completed task - should fail
    let cancel_result = queue.cancel(task.id).await;

    assert!(
        cancel_result.is_err(),
        "Cancelling a completed task should fail. Got: {:?}",
        cancel_result
    );
}

#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_cancel_failed_task_fails() {
    // From spec: "Returns error. Cannot cancel completed/failed/archived tasks."
    use oq::worker::fail_task;

    let queue = test_queue().await;
    let task_type = unique_task_type();

    // Submit with no retries so it fails immediately
    let options = SubmitOptions {
        max_retries: Some(0),
        ..Default::default()
    };

    let task = queue
        .submit(&task_type, json!({}), options)
        .await
        .expect("Failed to submit task");

    // Claim and fail the task
    let result = claim_task(&queue, task.id, "test-worker")
        .await
        .expect("Failed to claim");

    let (claimed_task, lease_id) = match result {
        ClaimResult::Claimed { task, .. } => {
            let lease_id = task.lease_id.expect("Missing lease_id");
            (*task, lease_id)
        }
        other => panic!("Expected Claimed, got {:?}", other),
    };

    let failed = fail_task(&queue, claimed_task.id, lease_id, "Test failure")
        .await
        .expect("Failed to fail task");

    assert_eq!(failed.status, TaskStatus::Failed);

    // Try to cancel the failed task - should fail
    let cancel_result = queue.cancel(task.id).await;

    assert!(
        cancel_result.is_err(),
        "Cancelling a failed task should fail. Got: {:?}",
        cancel_result
    );
}

#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_cancel_task_during_backoff() {
    // From spec: "Cancellation During Retry Backoff: Task in Pending with future available_at can still be cancelled."
    use oq::worker::{retry_task, ClaimResult};

    let queue = test_queue().await;
    let task_type = unique_task_type();

    let options = SubmitOptions {
        max_retries: Some(3),
        ..Default::default()
    };

    let task = queue
        .submit(&task_type, json!({}), options)
        .await
        .expect("Failed to submit task");

    // Claim the task
    let result = claim_task(&queue, task.id, "test-worker")
        .await
        .expect("Failed to claim");

    let (claimed_task, lease_id) = match result {
        ClaimResult::Claimed { task, .. } => {
            let lease_id = task.lease_id.expect("Missing lease_id");
            (*task, lease_id)
        }
        other => panic!("Expected Claimed, got {:?}", other),
    };

    // Retry the task - this puts it back to Pending with a future available_at (backoff)
    let retried = retry_task(&queue, claimed_task.id, lease_id, "Temporary failure")
        .await
        .expect("Failed to retry task");

    assert_eq!(retried.status, TaskStatus::Pending);
    // Task should have future available_at due to backoff
    let now = queue
        .now()
        .await
        .expect("Failed to fetch authoritative queue time");
    assert!(
        retried.available_at > now,
        "Task should be in backoff with future available_at"
    );

    // Cancel the task while it's in backoff
    let cancelled = queue
        .cancel(task.id)
        .await
        .expect("Should be able to cancel task during backoff");

    assert_eq!(
        cancelled.status,
        TaskStatus::Cancelled,
        "Task in backoff should be cancellable"
    );
}

#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_cancel_removes_from_ready_index() {
    // From spec: "On cancellation, delete ready index (best-effort)"
    // We verify this indirectly by checking the task doesn't show up in polls
    let queue = test_queue().await;
    let task_type = unique_task_type();

    let task = queue
        .submit(&task_type, json!({}), SubmitOptions::default())
        .await
        .expect("Failed to submit task");

    // Cancel the task
    queue.cancel(task.id).await.expect("Failed to cancel task");

    // List pending tasks - cancelled task should not appear
    let pending = queue
        .list(&task.shard, Some(TaskStatus::Pending), 100)
        .await
        .expect("Failed to list tasks");

    let found = pending.iter().any(|t| t.id == task.id);
    assert!(
        !found,
        "Cancelled task should not appear in pending tasks list"
    );

    // List cancelled tasks - task should appear there
    let cancelled_tasks = queue
        .list(&task.shard, Some(TaskStatus::Cancelled), 100)
        .await
        .expect("Failed to list cancelled tasks");

    let found_cancelled = cancelled_tasks.iter().any(|t| t.id == task.id);
    assert!(
        found_cancelled,
        "Cancelled task should appear in cancelled tasks list"
    );
}
