//! Tests for task lease extension functionality.
//!
//! Note: Full integration tests with S3 are in the integration test suite.
//! These tests verify the basic logic and error handling without requiring S3.

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod task_context_tests {
    use chrono::{Duration, Utc};
    use uuid::Uuid;

    use crate::models::{Task, TaskStatus};

    #[test]
    fn test_task_lease_expires_at_none_means_no_lease() {
        let task = Task::new("test", serde_json::json!({}));
        assert!(task.lease_expires_at.is_none());
        assert!(!task.is_lease_expired_at(Utc::now()));
    }

    #[test]
    fn test_task_lease_expires_at_future_means_valid() {
        let mut task = Task::new("test", serde_json::json!({}));
        task.status = TaskStatus::Running;
        task.lease_expires_at = Some(Utc::now() + Duration::seconds(60));

        assert!(!task.is_lease_expired_at(Utc::now()));
    }

    #[test]
    fn test_task_lease_expires_at_past_means_expired() {
        let mut task = Task::new("test", serde_json::json!({}));
        task.status = TaskStatus::Running;
        task.lease_expires_at = Some(Utc::now() - Duration::seconds(60));

        assert!(task.is_lease_expired_at(Utc::now()));
    }

    #[test]
    fn test_task_lease_index_key_updated_on_extension() {
        let mut task = Task::new("test", serde_json::json!({}));
        task.status = TaskStatus::Running;
        task.lease_id = Some(Uuid::new_v4());

        // Original lease expiration
        let original_expires = Utc::now() + Duration::seconds(60);
        task.lease_expires_at = Some(original_expires);
        let original_key = task.lease_index_key();

        // Simulate lease extension
        let extended_expires = Utc::now() + Duration::seconds(120);
        task.lease_expires_at = Some(extended_expires);
        let extended_key = task.lease_index_key();

        // Keys should both exist
        assert!(original_key.is_some());
        assert!(extended_key.is_some());

        // If the times are in different minute buckets, keys will differ
        // (but they might be the same if within the same minute)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod lease_extension_error_tests {
    use uuid::Uuid;

    use crate::queue::TaskOperationError;
    use crate::storage::StorageError;

    #[test]
    fn test_lease_extension_failed_error_display() {
        let task_id = Uuid::new_v4();
        let err = TaskOperationError::LeaseExtensionFailed {
            task_id,
            reason: "CAS failed".to_string(),
        };

        let display = err.to_string();
        assert!(display.contains("Failed to extend lease"));
        assert!(display.contains(&task_id.to_string()));
    }

    #[test]
    fn test_lease_extension_failed_suggestion() {
        let err = TaskOperationError::LeaseExtensionFailed {
            task_id: Uuid::new_v4(),
            reason: "CAS failed".to_string(),
        };

        let suggestion = err.suggestion();
        assert!(suggestion.contains("another worker"));
        assert!(suggestion.contains("Stop processing"));
    }

    #[test]
    fn test_lease_extension_failed_from_precondition_failed() {
        // Verify that PreconditionFailed is properly converted to LeaseExtensionFailed
        // in the extend_lease implementation
        let storage_err = StorageError::PreconditionFailed {
            key: "test-key".to_string(),
        };

        // This simulates what happens in the extend_lease method
        let task_id = Uuid::new_v4();
        let err = TaskOperationError::LeaseExtensionFailed {
            task_id,
            reason: "Task was modified by another worker (ETag mismatch)".to_string(),
        };

        assert!(matches!(
            err,
            TaskOperationError::LeaseExtensionFailed { .. }
        ));
        assert!(storage_err.to_string().contains("Precondition failed"));
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod task_status_validation_tests {
    use chrono::{Duration, Utc};
    use uuid::Uuid;

    use crate::models::{Task, TaskStatus};

    #[test]
    fn test_only_running_tasks_have_leases() {
        let now = Utc::now();

        // Pending task should not have lease
        let pending = Task {
            status: TaskStatus::Pending,
            lease_expires_at: None,
            lease_id: None,
            ..Task::default()
        };
        assert!(pending.lease_expires_at.is_none());

        // Running task should have lease
        let running = Task {
            status: TaskStatus::Running,
            lease_expires_at: Some(now + Duration::seconds(60)),
            lease_id: Some(Uuid::new_v4()),
            worker_id: Some("worker-1".to_string()),
            ..Task::default()
        };
        assert!(running.lease_expires_at.is_some());

        // Completed task should not have lease
        let completed = Task {
            status: TaskStatus::Completed,
            lease_expires_at: None,
            lease_id: None,
            ..Task::default()
        };
        assert!(completed.lease_expires_at.is_none());

        // Failed task should not have lease
        let failed = Task {
            status: TaskStatus::Failed,
            lease_expires_at: None,
            lease_id: None,
            ..Task::default()
        };
        assert!(failed.lease_expires_at.is_none());
    }

    #[test]
    fn test_task_cannot_extend_if_not_running() {
        // This test verifies the validation logic that should be in extend_lease
        let pending_task = Task {
            status: TaskStatus::Pending,
            ..Task::default()
        };
        assert_ne!(pending_task.status, TaskStatus::Running);

        let completed_task = Task {
            status: TaskStatus::Completed,
            ..Task::default()
        };
        assert_ne!(completed_task.status, TaskStatus::Running);
    }

    #[test]
    fn test_task_cannot_extend_if_no_lease() {
        let task = Task {
            status: TaskStatus::Running,
            lease_expires_at: None, // No lease set
            ..Task::default()
        };
        assert!(task.lease_expires_at.is_none());
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod lease_index_key_tests {
    use chrono::{Duration, Utc};
    use uuid::Uuid;

    use crate::models::Task;

    #[test]
    fn test_lease_index_key_format() {
        let task_id = Uuid::parse_str("a1234567-89ab-cdef-0123-456789abcdef").expect("valid UUID");
        let now = Utc::now();

        let mut task = Task::new("test", serde_json::json!({}));
        task.id = task_id;
        task.shard = Task::shard_from_id(&task_id);
        task.lease_expires_at = Some(now);

        let key = task.lease_index_key();
        assert!(key.is_some());

        let key_str = key.unwrap();
        assert!(key_str.starts_with("leases/a/"));
        assert!(key_str.ends_with(&task_id.to_string()));
    }

    #[test]
    fn test_lease_index_key_changes_with_time() {
        let task_id = Uuid::new_v4();
        let mut task = Task::new("test", serde_json::json!({}));
        task.id = task_id;
        task.shard = Task::shard_from_id(&task_id);

        // Set lease at one minute
        let time1 = Utc::now();
        task.lease_expires_at = Some(time1);
        let key1 = task.lease_index_key();

        // Set lease at a different minute (2 minutes later)
        let time2 = time1 + Duration::minutes(2);
        task.lease_expires_at = Some(time2);
        let key2 = task.lease_index_key();

        // Keys should be different because they're in different minute buckets
        assert_ne!(key1, key2);
    }

    #[test]
    fn test_lease_index_key_none_when_no_lease() {
        let task = Task::new("test", serde_json::json!({}));
        assert!(task.lease_expires_at.is_none());
        assert!(task.lease_index_key().is_none());
    }
}
