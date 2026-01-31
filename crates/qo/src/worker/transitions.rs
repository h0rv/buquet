//! Task state transitions: complete, retry, and fail.
//!
//! This module provides atomic state transitions for tasks with proper
//! lease verification to prevent stale workers from corrupting state.

use chrono::Duration;
use serde_json::Value;
use uuid::Uuid;

use crate::models::{Task, TaskStatus};
use crate::queue::{
    should_offload_payload, store_payload, PayloadType, Queue, DEFAULT_PAYLOAD_REF_THRESHOLD_BYTES,
};
use crate::storage::StorageError;

/// Options for completing a task with payload ref support.
#[derive(Debug, Clone, Default)]
pub struct CompleteOptions {
    /// Whether to use payload references for large outputs.
    pub use_payload_refs: bool,
    /// Threshold in bytes for offloading payloads (default: 256KB).
    pub payload_ref_threshold_bytes: Option<usize>,
}

/// Error type for state transitions.
#[derive(Debug, thiserror::Error)]
pub enum TransitionError {
    /// Task not found in the queue.
    #[error("Task not found: {0}")]
    NotFound(Uuid),

    /// Lease ID does not match the current task's lease.
    #[error("Lease mismatch: expected {expected}, found {found:?}")]
    LeaseMismatch {
        /// The expected `lease_id` provided by the caller.
        expected: Uuid,
        /// The actual `lease_id` found on the task (None if no lease).
        found: Option<Uuid>,
    },

    /// Task is not in the expected status for this transition.
    #[error("Invalid status: expected {expected:?}, found {found:?}")]
    InvalidStatus {
        /// The expected status(es) for this transition.
        expected: Vec<TaskStatus>,
        /// The actual status found on the task.
        found: TaskStatus,
    },

    /// Another worker modified the task concurrently (`ETag` mismatch).
    #[error("Concurrent modification (task was modified by another worker)")]
    ConcurrentModification,

    /// Maximum reschedules exceeded for this task.
    #[error("Max reschedules ({0}) exceeded")]
    MaxReschedulesExceeded(u32),

    /// Underlying storage error.
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),
}

/// Marks a task as completed successfully.
///
/// Steps:
/// 1. GET task (capture `ETag`)
/// 2. Verify `lease_id` matches (prevents stale workers)
/// 3. PUT with `If-Match`: status=Completed, output=result, `completed_at`=now, clear lease fields
/// 4. Delete lease index (best-effort)
///
/// # Arguments
///
/// * `queue` - The queue containing the task
/// * `task_id` - The UUID of the task to complete
/// * `lease_id` - The lease ID from when the task was claimed
/// * `output` - The output data from task execution
///
/// # Returns
///
/// The updated task on success.
///
/// # Errors
///
/// * `TransitionError::NotFound` - Task does not exist
/// * `TransitionError::LeaseMismatch` - Lease ID does not match (stale worker)
/// * `TransitionError::InvalidStatus` - Task is not in Running status
/// * `TransitionError::ConcurrentModification` - Another worker modified the task
/// * `TransitionError::Storage` - Underlying storage error
pub async fn complete_task(
    queue: &Queue,
    task_id: Uuid,
    lease_id: Uuid,
    output: Value,
) -> Result<Task, TransitionError> {
    complete_task_with_options(queue, task_id, lease_id, output, CompleteOptions::default()).await
}

/// Marks a task as completed successfully, with options for payload refs.
///
/// Steps:
/// 1. GET task (capture `ETag`)
/// 2. Verify `lease_id` matches (prevents stale workers)
/// 3. If payload refs enabled and output is large, store output separately
/// 4. PUT with `If-Match`: status=Completed, output=result, `completed_at`=now, clear lease fields
/// 5. Delete lease index (best-effort)
///
/// # Arguments
///
/// * `queue` - The queue containing the task
/// * `task_id` - The UUID of the task to complete
/// * `lease_id` - The lease ID from when the task was claimed
/// * `output` - The output data from task execution
/// * `options` - Options for payload ref handling
///
/// # Returns
///
/// The updated task on success.
pub async fn complete_task_with_options(
    queue: &Queue,
    task_id: Uuid,
    lease_id: Uuid,
    output: Value,
    options: CompleteOptions,
) -> Result<Task, TransitionError> {
    // Step 1: GET task with ETag
    let (mut task, etag) = queue
        .get(task_id)
        .await?
        .ok_or(TransitionError::NotFound(task_id))?;

    // Step 2: Verify lease_id matches
    if task.lease_id != Some(lease_id) {
        return Err(TransitionError::LeaseMismatch {
            expected: lease_id,
            found: task.lease_id,
        });
    }

    // Verify task is in Running status
    if task.status != TaskStatus::Running {
        return Err(TransitionError::InvalidStatus {
            expected: vec![TaskStatus::Running],
            found: task.status,
        });
    }

    // Capture the old lease index key before clearing lease fields
    let old_lease_key = task.lease_index_key();

    // Step 3: Update task state
    let now = queue.now().await?;
    task.status = TaskStatus::Completed;
    task.completed_at = Some(now);
    task.updated_at = now;

    // Handle output payload refs if enabled
    if options.use_payload_refs {
        let threshold = options
            .payload_ref_threshold_bytes
            .unwrap_or(DEFAULT_PAYLOAD_REF_THRESHOLD_BYTES);

        if should_offload_payload(&output, threshold) {
            // Store output payload separately
            let output_ref =
                store_payload(queue.client(), task_id, PayloadType::Output, &output).await?;
            task.output_ref = Some(output_ref);
            task.output = Some(Value::Null);
        } else {
            task.output = Some(output);
        }
    } else {
        task.output = Some(output);
    }

    // Clear lease fields
    task.lease_id = None;
    task.lease_expires_at = None;
    task.worker_id = None;

    // Atomic update with If-Match condition
    match queue.update(&task, &etag).await {
        Ok(_) => {}
        Err(StorageError::PreconditionFailed { .. }) => {
            return Err(TransitionError::ConcurrentModification);
        }
        Err(e) => return Err(TransitionError::Storage(e)),
    }

    // Step 4: Best-effort delete lease index
    if let Some(lease_key) = old_lease_key {
        if let Err(e) = queue.delete_index(&lease_key).await {
            tracing::warn!(key = %lease_key, error = %e, "Failed to delete lease index");
        }
    }

    Ok(task)
}

/// Marks a task for retry (sets status back to Pending with future `available_at`).
///
/// Steps:
/// 1. GET task (capture `ETag`)
/// 2. Verify `lease_id` matches
/// 3. Calculate backoff delay using `retry_policy`
/// 4. PUT with `If-Match`: status=Pending, `retry_count`++, `available_at`=now+backoff, clear lease fields
/// 5. Create ready index, delete lease index (best-effort)
///
/// # Arguments
///
/// * `queue` - The queue containing the task
/// * `task_id` - The UUID of the task to retry
/// * `lease_id` - The lease ID from when the task was claimed
/// * `error` - The error message describing why the task failed
///
/// # Returns
///
/// The updated task on success.
///
/// # Errors
///
/// * `TransitionError::NotFound` - Task does not exist
/// * `TransitionError::LeaseMismatch` - Lease ID does not match (stale worker)
/// * `TransitionError::InvalidStatus` - Task is not in Running status
/// * `TransitionError::ConcurrentModification` - Another worker modified the task
/// * `TransitionError::Storage` - Underlying storage error
pub async fn retry_task(
    queue: &Queue,
    task_id: Uuid,
    lease_id: Uuid,
    error: &str,
) -> Result<Task, TransitionError> {
    // Step 1: GET task with ETag
    let (mut task, etag) = queue
        .get(task_id)
        .await?
        .ok_or(TransitionError::NotFound(task_id))?;

    // Step 2: Verify lease_id matches
    if task.lease_id != Some(lease_id) {
        return Err(TransitionError::LeaseMismatch {
            expected: lease_id,
            found: task.lease_id,
        });
    }

    // Verify task is in Running status
    if task.status != TaskStatus::Running {
        return Err(TransitionError::InvalidStatus {
            expected: vec![TaskStatus::Running],
            found: task.status,
        });
    }

    // Capture the old lease index key before clearing lease fields
    let old_lease_key = task.lease_index_key();

    // Step 3: Calculate backoff delay
    let backoff = task.retry_policy.calculate_delay(task.retry_count);
    let backoff_millis = i64::try_from(backoff.as_millis()).unwrap_or(i64::MAX);

    // Step 4: Update task state
    let now = queue.now().await?;
    task.status = TaskStatus::Pending;
    task.retry_count += 1;
    task.last_error = Some(error.to_string());
    task.available_at = now + Duration::milliseconds(backoff_millis);
    task.updated_at = now;
    // Clear lease fields
    task.lease_id = None;
    task.lease_expires_at = None;
    task.worker_id = None;

    // Atomic update with If-Match condition
    match queue.update(&task, &etag).await {
        Ok(_) => {}
        Err(StorageError::PreconditionFailed { .. }) => {
            return Err(TransitionError::ConcurrentModification);
        }
        Err(e) => return Err(TransitionError::Storage(e)),
    }

    // Step 5: Best-effort index management
    // Create ready index for the new available_at time
    if let Err(e) = queue.create_index(&task.ready_index_key()).await {
        tracing::warn!(key = %task.ready_index_key(), error = %e, "Failed to create ready index");
    }

    // Delete old lease index
    if let Some(lease_key) = old_lease_key {
        if let Err(e) = queue.delete_index(&lease_key).await {
            tracing::warn!(key = %lease_key, error = %e, "Failed to delete lease index");
        }
    }

    Ok(task)
}

/// Reschedules a task for later execution (e.g., for rate limiting or backpressure).
///
/// Steps:
/// 1. Verify `lease_id` matches (prevents stale workers)
/// 2. Verify task is in Running status
/// 3. Check if `reschedule_count` >= `max_reschedules` (if set)
///    - If exceeded, fail the task with "Max reschedules (N) exceeded"
/// 4. Otherwise:
///    - Increment `reschedule_count`
///    - Set status = Pending
///    - Set `available_at` = now + `delay_seconds`
///    - Clear lease fields (`lease_id`, `lease_expires_at`, `worker_id`)
/// 5. Update task with If-Match condition
/// 6. Delete lease index, create ready index (best-effort)
///
/// # Arguments
///
/// * `queue` - The queue containing the task
/// * `task` - The task to reschedule (with current state)
/// * `etag` - The `ETag` from when the task was fetched
/// * `delay_seconds` - How long to wait before the task becomes available again
///
/// # Returns
///
/// The updated task on success.
///
/// # Errors
///
/// * `TransitionError::LeaseMismatch` - Lease ID does not match (stale worker)
/// * `TransitionError::InvalidStatus` - Task is not in Running status
/// * `TransitionError::MaxReschedulesExceeded` - Task has exceeded `max_reschedules` limit
/// * `TransitionError::ConcurrentModification` - Another worker modified the task
/// * `TransitionError::Storage` - Underlying storage error
pub async fn reschedule_task(
    queue: &Queue,
    task: &Task,
    etag: &str,
    lease_id: Uuid,
    delay_seconds: u64,
) -> Result<Task, TransitionError> {
    // Step 1: Verify lease_id matches
    if task.lease_id != Some(lease_id) {
        return Err(TransitionError::LeaseMismatch {
            expected: lease_id,
            found: task.lease_id,
        });
    }

    // Step 2: Verify task is in Running status
    if task.status != TaskStatus::Running {
        return Err(TransitionError::InvalidStatus {
            expected: vec![TaskStatus::Running],
            found: task.status,
        });
    }

    // Step 3: Check if reschedule limit exceeded
    if let Some(max_reschedules) = task.max_reschedules {
        if task.reschedule_count >= max_reschedules {
            return Err(TransitionError::MaxReschedulesExceeded(max_reschedules));
        }
    }

    // Capture the old lease index key before clearing lease fields
    let old_lease_key = task.lease_index_key();

    // Step 4: Update task state
    let mut updated_task = task.clone();
    let now = queue.now().await?;
    let delay_millis = i64::try_from(delay_seconds * 1000).unwrap_or(i64::MAX);

    updated_task.status = TaskStatus::Pending;
    updated_task.reschedule_count += 1;
    updated_task.available_at = now + Duration::milliseconds(delay_millis);
    updated_task.updated_at = now;
    // Clear lease fields
    updated_task.lease_id = None;
    updated_task.lease_expires_at = None;
    updated_task.worker_id = None;

    // Step 5: Atomic update with If-Match condition
    match queue.update(&updated_task, etag).await {
        Ok(_) => {}
        Err(StorageError::PreconditionFailed { .. }) => {
            return Err(TransitionError::ConcurrentModification);
        }
        Err(e) => return Err(TransitionError::Storage(e)),
    }

    // Step 6: Best-effort index management
    // Create ready index for the new available_at time
    if let Err(e) = queue.create_index(&updated_task.ready_index_key()).await {
        tracing::warn!(key = %updated_task.ready_index_key(), error = %e, "Failed to create ready index");
    }

    // Delete old lease index
    if let Some(lease_key) = old_lease_key {
        if let Err(e) = queue.delete_index(&lease_key).await {
            tracing::warn!(key = %lease_key, error = %e, "Failed to delete lease index");
        }
    }

    Ok(updated_task)
}

/// Marks a task as permanently failed (DLQ state).
///
/// Steps:
/// 1. GET task (capture `ETag`)
/// 2. Verify `lease_id` matches
/// 3. PUT with `If-Match`: status=Failed, `last_error`=error, `completed_at`=now, clear lease fields
/// 4. Delete lease index (best-effort)
///
/// # Arguments
///
/// * `queue` - The queue containing the task
/// * `task_id` - The UUID of the task to fail
/// * `lease_id` - The lease ID from when the task was claimed
/// * `error` - The error message describing why the task failed
///
/// # Returns
///
/// The updated task on success.
///
/// # Errors
///
/// * `TransitionError::NotFound` - Task does not exist
/// * `TransitionError::LeaseMismatch` - Lease ID does not match (stale worker)
/// * `TransitionError::InvalidStatus` - Task is not in Running status
/// * `TransitionError::ConcurrentModification` - Another worker modified the task
/// * `TransitionError::Storage` - Underlying storage error
pub async fn fail_task(
    queue: &Queue,
    task_id: Uuid,
    lease_id: Uuid,
    error: &str,
) -> Result<Task, TransitionError> {
    // Step 1: GET task with ETag
    let (mut task, etag) = queue
        .get(task_id)
        .await?
        .ok_or(TransitionError::NotFound(task_id))?;

    // Step 2: Verify lease_id matches
    if task.lease_id != Some(lease_id) {
        return Err(TransitionError::LeaseMismatch {
            expected: lease_id,
            found: task.lease_id,
        });
    }

    // Verify task is in Running status
    if task.status != TaskStatus::Running {
        return Err(TransitionError::InvalidStatus {
            expected: vec![TaskStatus::Running],
            found: task.status,
        });
    }

    // Capture the old lease index key before clearing lease fields
    let old_lease_key = task.lease_index_key();

    // Step 3: Update task state
    let now = queue.now().await?;
    task.status = TaskStatus::Failed;
    task.last_error = Some(error.to_string());
    task.completed_at = Some(now);
    task.updated_at = now;
    // Clear lease fields
    task.lease_id = None;
    task.lease_expires_at = None;
    task.worker_id = None;

    // Atomic update with If-Match condition
    match queue.update(&task, &etag).await {
        Ok(_) => {}
        Err(StorageError::PreconditionFailed { .. }) => {
            return Err(TransitionError::ConcurrentModification);
        }
        Err(e) => return Err(TransitionError::Storage(e)),
    }

    // Step 4: Best-effort delete lease index
    if let Some(lease_key) = old_lease_key {
        if let Err(e) = queue.delete_index(&lease_key).await {
            tracing::warn!(key = %lease_key, error = %e, "Failed to delete lease index");
        }
    }

    Ok(task)
}

/// Archives a completed or failed task.
///
/// This operation is used to move tasks to an archived state for long-term
/// storage or cleanup. Only tasks in Completed or Failed status can be archived.
///
/// # Arguments
///
/// * `queue` - The queue containing the task
/// * `task_id` - The UUID of the task to archive
///
/// # Returns
///
/// The updated task on success.
///
/// # Errors
///
/// * `TransitionError::NotFound` - Task does not exist
/// * `TransitionError::InvalidStatus` - Task is not in Completed or Failed status
/// * `TransitionError::ConcurrentModification` - Another worker modified the task
/// * `TransitionError::Storage` - Underlying storage error
pub async fn archive_task(queue: &Queue, task_id: Uuid) -> Result<Task, TransitionError> {
    // GET task with ETag
    let (mut task, etag) = queue
        .get(task_id)
        .await?
        .ok_or(TransitionError::NotFound(task_id))?;

    // Verify task is in Completed or Failed status
    if task.status != TaskStatus::Completed && task.status != TaskStatus::Failed {
        return Err(TransitionError::InvalidStatus {
            expected: vec![TaskStatus::Completed, TaskStatus::Failed],
            found: task.status,
        });
    }

    // Update task state
    task.status = TaskStatus::Archived;
    task.updated_at = queue.now().await?;

    // Atomic update with If-Match condition
    match queue.update(&task, &etag).await {
        Ok(_) => {}
        Err(StorageError::PreconditionFailed { .. }) => {
            return Err(TransitionError::ConcurrentModification);
        }
        Err(e) => return Err(TransitionError::Storage(e)),
    }

    Ok(task)
}

/// Replays a failed task (resets to Pending with `available_at`=now).
///
/// This operation allows failed tasks to be retried from scratch. The `retry_count`
/// is reset to 0 and the task becomes immediately available for claiming.
///
/// # Arguments
///
/// * `queue` - The queue containing the task
/// * `task_id` - The UUID of the task to replay
///
/// # Returns
///
/// The updated task on success.
///
/// # Errors
///
/// * `TransitionError::NotFound` - Task does not exist
/// * `TransitionError::InvalidStatus` - Task is not in Failed status
/// * `TransitionError::ConcurrentModification` - Another worker modified the task
/// * `TransitionError::Storage` - Underlying storage error
pub async fn replay_task(queue: &Queue, task_id: Uuid) -> Result<Task, TransitionError> {
    // GET task with ETag
    let (mut task, etag) = queue
        .get(task_id)
        .await?
        .ok_or(TransitionError::NotFound(task_id))?;

    // Verify task is in Failed status
    if task.status != TaskStatus::Failed {
        return Err(TransitionError::InvalidStatus {
            expected: vec![TaskStatus::Failed],
            found: task.status,
        });
    }

    // Update task state
    let now = queue.now().await?;
    task.status = TaskStatus::Pending;
    task.retry_count = 0;
    task.available_at = now;
    task.updated_at = now;
    // Clear completion timestamp since we're replaying
    task.completed_at = None;
    // Clear error since we're starting fresh
    task.last_error = None;

    // Atomic update with If-Match condition
    match queue.update(&task, &etag).await {
        Ok(_) => {}
        Err(StorageError::PreconditionFailed { .. }) => {
            return Err(TransitionError::ConcurrentModification);
        }
        Err(e) => return Err(TransitionError::Storage(e)),
    }

    // Best-effort create ready index
    if let Err(e) = queue.create_index(&task.ready_index_key()).await {
        tracing::warn!(key = %task.ready_index_key(), error = %e, "Failed to create ready index");
    }

    Ok(task)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transition_error_display() {
        let task_id = Uuid::new_v4();
        let lease_id = Uuid::new_v4();

        let err = TransitionError::NotFound(task_id);
        assert!(err.to_string().contains(&task_id.to_string()));

        let err = TransitionError::LeaseMismatch {
            expected: lease_id,
            found: None,
        };
        assert!(err.to_string().contains(&lease_id.to_string()));
        assert!(err.to_string().contains("None"));

        let err = TransitionError::LeaseMismatch {
            expected: lease_id,
            found: Some(Uuid::new_v4()),
        };
        assert!(err.to_string().contains("Lease mismatch"));

        let err = TransitionError::InvalidStatus {
            expected: vec![TaskStatus::Running],
            found: TaskStatus::Pending,
        };
        assert!(err.to_string().contains("Invalid status"));

        let err = TransitionError::ConcurrentModification;
        assert!(err.to_string().contains("Concurrent modification"));
    }

    #[test]
    fn test_transition_error_from_storage_error() {
        let storage_err = StorageError::NotFound {
            key: "test".to_string(),
        };
        let transition_err: TransitionError = storage_err.into();
        assert!(matches!(transition_err, TransitionError::Storage(_)));
    }
}
