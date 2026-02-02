//! Task claiming logic with atomic CAS operations.
//!
//! This module implements the core task claiming logic that ensures
//! exactly-once task execution through atomic compare-and-swap (CAS)
//! operations using S3's conditional writes.

use chrono::Duration;
use metrics::counter;
use uuid::Uuid;

use crate::models::{Task, TaskStatus};
use crate::queue::Queue;
use crate::storage::StorageError;

/// Result of a claim attempt.
///
/// This enum represents all possible outcomes when attempting to claim a task.
#[derive(Debug)]
pub enum ClaimResult {
    /// Successfully claimed the task.
    ///
    /// The task has been updated to `Running` status and is now owned by this worker.
    Claimed {
        /// The claimed task with updated state (boxed to reduce enum size).
        task: Box<Task>,
        /// The new `ETag` after the update.
        etag: String,
    },
    /// Another worker claimed the task first (HTTP 412 Precondition Failed).
    ///
    /// This is a normal occurrence in a distributed system and indicates
    /// the worker should try claiming another task.
    AlreadyClaimed,
    /// Task is not available for claiming.
    ///
    /// This can happen if:
    /// - The task status is not `Pending`
    /// - The task's `available_at` is in the future
    NotAvailable,
    /// Task was not found in the queue.
    NotFound,
    /// Task has expired (exceeded its TTL before being claimed).
    ///
    /// The worker should skip this task and optionally mark it as expired.
    Expired,
}

/// Attempts to claim a task atomically.
///
/// This function implements the following algorithm:
/// 1. GET the task and capture its `ETag`
/// 2. Verify status == Pending and `is_available_at()` returns true
/// 3. PUT with `If-Match` to atomically update to Running state
/// 4. Create lease index, delete ready index (best-effort)
///
/// The use of `If-Match` with the `ETag` ensures that if two workers
/// try to claim the same task simultaneously, only one will succeed.
/// The other will receive a 412 Precondition Failed response.
///
/// # Arguments
///
/// * `queue` - The queue containing the task
/// * `task_id` - The UUID of the task to claim
/// * `worker_id` - The ID of the worker claiming the task
///
/// # Returns
///
/// * `Ok(ClaimResult::Claimed { task, etag })` - Task was successfully claimed
/// * `Ok(ClaimResult::AlreadyClaimed)` - Another worker claimed it first
/// * `Ok(ClaimResult::NotAvailable)` - Task is not in a claimable state
/// * `Ok(ClaimResult::NotFound)` - Task does not exist
/// * `Err(StorageError)` - A storage error occurred
///
/// # Errors
///
/// Returns `StorageError` for S3 communication errors.
pub async fn claim_task(
    queue: &Queue,
    task_id: Uuid,
    worker_id: &str,
) -> Result<ClaimResult, StorageError> {
    // Get current task state with ETag
    let Some((mut task, etag)) = queue.get(task_id).await? else {
        return Ok(ClaimResult::NotFound);
    };

    // Check if task is in claimable status
    if task.status != TaskStatus::Pending {
        return Ok(ClaimResult::NotAvailable);
    }

    let now = queue.now().await?;

    // Check if task has expired (exceeded TTL)
    if task.is_expired_at(now) {
        return Ok(ClaimResult::Expired);
    }

    // Check if task is available (available_at <= now)
    if !task.is_available_at(now) {
        return Ok(ClaimResult::NotAvailable);
    }

    // Prepare the task for claiming
    let old_ready_key = task.ready_index_key();

    // Update task state
    task.status = TaskStatus::Running;
    task.worker_id = Some(worker_id.to_string());
    task.lease_id = Some(Uuid::new_v4());
    task.attempt += 1;
    // Safe conversion: timeout_seconds is typically small (< i64::MAX)
    // Using try_from with a fallback for safety
    let timeout_secs = i64::try_from(task.timeout_seconds).unwrap_or(i64::MAX);
    task.lease_expires_at = Some(now + Duration::seconds(timeout_secs));
    task.updated_at = now;

    // Atomic update with If-Match condition
    let new_etag = match queue.update(&task, &etag).await {
        Ok(etag) => etag,
        Err(StorageError::PreconditionFailed { .. }) => {
            // Another worker claimed this task first
            counter!("buquet.claims.conflict").increment(1);
            return Ok(ClaimResult::AlreadyClaimed);
        }
        Err(e) => return Err(e),
    };

    counter!("buquet.claims.success").increment(1);

    // Best-effort: create lease index for timeout monitoring
    if let Some(lease_key) = task.lease_index_key() {
        if let Err(e) = queue.create_index(&lease_key).await {
            tracing::warn!(key = %lease_key, error = %e, "Failed to create lease index");
        }
    }

    // Best-effort: delete ready index since task is no longer ready
    if let Err(e) = queue.delete_index(&old_ready_key).await {
        tracing::warn!(key = %old_ready_key, error = %e, "Failed to delete ready index");
    }

    Ok(ClaimResult::Claimed {
        task: Box::new(task),
        etag: new_etag,
    })
}

/// Attempts to claim a task only if a handler exists for its type.
///
/// This function implements the key safety check: before claiming a task,
/// it first fetches the task to determine its type, then checks if a handler
/// is registered for that type. This prevents workers from claiming tasks
/// they cannot handle, which would waste resources and potentially fail
/// tasks meant for other workers.
///
/// This is the canonical implementation of the "check handler before claiming"
/// pattern. Both Rust and Python workers should use this logic.
///
/// # Arguments
///
/// * `queue` - The queue containing the task
/// * `task_id` - The UUID of the task to claim
/// * `worker_id` - The ID of the worker claiming the task
/// * `has_handler` - A function that returns true if a handler exists for the given task type
///
/// # Returns
///
/// * `Ok(ClaimResult::Claimed { task, etag })` - Task was successfully claimed
/// * `Ok(ClaimResult::NotFound)` - Task does not exist
/// * `Ok(ClaimResult::NotAvailable)` - Task exists but no handler is registered for its type
/// * Other `ClaimResult` variants from `claim_task`
/// * `Err(StorageError)` - A storage error occurred
///
/// # Example
///
/// ```ignore
/// let result = try_claim_for_handler(
///     &queue,
///     task_id,
///     "worker-1",
///     |task_type| handlers.has_handler(task_type),
/// ).await?;
/// ```
pub async fn try_claim_for_handler<F>(
    queue: &Queue,
    task_id: Uuid,
    worker_id: &str,
    has_handler: F,
) -> Result<ClaimResult, StorageError>
where
    F: Fn(&str) -> bool,
{
    // First, fetch the task to determine its type
    let task_type = match queue.get(task_id).await? {
        Some((task, _etag)) => task.task_type.clone(),
        None => {
            return Ok(ClaimResult::NotFound);
        }
    };

    // Check if we have a handler for this task type
    if !has_handler(&task_type) {
        tracing::debug!(
            task_id = %task_id,
            task_type = %task_type,
            "Skipping task - no handler registered"
        );
        // Return NotAvailable rather than NotFound to distinguish
        // "no handler" from "task doesn't exist"
        return Ok(ClaimResult::NotAvailable);
    }

    // Now attempt to claim the task
    claim_task(queue, task_id, worker_id).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_claim_result_debug() {
        let task = Task::new("test", serde_json::json!({}));
        let claimed = ClaimResult::Claimed {
            task: Box::new(task),
            etag: "etag123".to_string(),
        };
        let debug_str = format!("{:?}", claimed);
        assert!(debug_str.contains("Claimed"));

        let already_claimed = ClaimResult::AlreadyClaimed;
        let debug_str = format!("{:?}", already_claimed);
        assert!(debug_str.contains("AlreadyClaimed"));

        let not_available = ClaimResult::NotAvailable;
        let debug_str = format!("{:?}", not_available);
        assert!(debug_str.contains("NotAvailable"));

        let not_found = ClaimResult::NotFound;
        let debug_str = format!("{:?}", not_found);
        assert!(debug_str.contains("NotFound"));
    }
}
