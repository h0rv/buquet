//! Rich error types for task queue operations.
//!
//! These error types provide context-aware error messages with helpful
//! suggestions for resolving common issues.

use thiserror::Error;
use uuid::Uuid;

use crate::storage::StorageError;

/// Errors that can occur during task operations.
///
/// These errors provide rich context and suggestions to help users
/// diagnose and resolve issues.
#[derive(Debug, Error)]
pub enum TaskOperationError {
    /// Failed to claim a task due to concurrent access.
    #[error("Failed to claim task {task_id}")]
    ClaimFailed {
        /// The ID of the task that could not be claimed.
        task_id: Uuid,
        /// The underlying storage error.
        #[source]
        source: StorageError,
    },

    /// Task was not found in the queue.
    #[error("Task not found: {task_id}")]
    NotFound {
        /// The ID of the task that was not found.
        task_id: Uuid,
    },

    /// Task has already completed and cannot be modified.
    #[error("Task has already completed: {task_id}")]
    AlreadyCompleted {
        /// The ID of the completed task.
        task_id: Uuid,
    },

    /// Task is already running (claimed by another worker).
    #[error("Task is already running: {task_id}")]
    AlreadyRunning {
        /// The ID of the running task.
        task_id: Uuid,
        /// The ID of the worker that claimed it, if known.
        worker_id: Option<String>,
    },

    /// Failed to connect to the storage backend.
    #[error("Cannot connect to S3")]
    ConnectionFailed {
        /// Details about the connection failure.
        details: String,
        /// The endpoint that was being connected to, if known.
        endpoint: Option<String>,
    },

    /// Permission denied when accessing storage.
    #[error("Access denied to bucket '{bucket}'")]
    PermissionDenied {
        /// The bucket that access was denied to.
        bucket: String,
    },

    /// A storage error occurred.
    #[error("{0}")]
    Storage(#[from] StorageError),

    /// Failed to extend task lease (CAS failed).
    ///
    /// This indicates the task was modified by another worker, possibly
    /// because the lease expired and another worker claimed it.
    #[error("Failed to extend lease for task {task_id}")]
    LeaseExtensionFailed {
        /// The ID of the task that could not have its lease extended.
        task_id: Uuid,
        /// The reason the extension failed.
        reason: String,
    },

    /// Idempotency key conflict - same key used with different input.
    ///
    /// This indicates that an idempotency key was reused with different
    /// input data. This is likely a bug in the caller's code.
    #[error("Idempotency conflict for key '{key}': {message}")]
    IdempotencyConflict {
        /// The idempotency key that caused the conflict.
        key: String,
        /// The existing task ID associated with this key.
        existing_task_id: Uuid,
        /// A human-readable message describing the conflict.
        message: String,
    },
}

impl TaskOperationError {
    /// Returns a helpful suggestion for resolving this error.
    ///
    /// These suggestions are designed to be educational and actionable,
    /// helping users diagnose and fix common issues.
    #[must_use]
    #[allow(clippy::missing_const_for_fn)] // Calls storage_err.suggestion() which is not const
    pub fn suggestion(&self) -> &'static str {
        match self {
            Self::ClaimFailed { .. } => {
                "Another worker claimed this task first. This is normal behavior \
                 in high-concurrency environments - the task will be processed \
                 by the worker that won the race. Your worker should simply \
                 try to claim another task."
            }
            Self::NotFound { .. } => {
                "The task may have been archived or the task ID may be incorrect. \
                 Try `oq history <id>` to see if it was previously processed, \
                 or verify the task ID is correct."
            }
            Self::AlreadyCompleted { .. } => {
                "This task has already finished. Check the task status with \
                 `oq status <id>` to see the result and any output data."
            }
            Self::AlreadyRunning { .. } => {
                "This task is currently being processed by another worker. \
                 If the task appears stuck, it may complete or timeout soon. \
                 The monitor process will automatically reclaim timed-out tasks."
            }
            Self::ConnectionFailed { .. } => {
                "Check that your S3 endpoint is correct and the service is running. \
                 For local development:\n  \
                 - Verify S3_ENDPOINT is set correctly\n  \
                 - Ensure your S3 service is running: docker compose up -d\n  \
                 - Test connectivity: curl <your-endpoint>"
            }
            Self::PermissionDenied { .. } => {
                "Check that AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are set correctly. \
                 Verify the credentials have permission to access this bucket. \
                 Check the bucket policy allows your IAM user/role."
            }
            Self::Storage(storage_err) => storage_err.suggestion(),
            Self::LeaseExtensionFailed { .. } => {
                "The task may have been taken by another worker. If the lease expired, \
                 another worker may have claimed the task. Stop processing this task \
                 immediately to avoid duplicate work."
            }
            Self::IdempotencyConflict { .. } => {
                "An idempotency key was reused with different input data. This is \
                 likely a bug in your application. Each idempotency key should only \
                 be used with the same input. If you need to submit a different task, \
                 use a different idempotency key."
            }
        }
    }

    /// Returns a richly formatted error message with context and suggestions.
    ///
    /// This format is designed for CLI output to help users understand
    /// what went wrong and how to fix it.
    #[must_use]
    pub fn display_rich(&self) -> String {
        use std::fmt::Write;
        let mut output = format!("Error: {self}");

        // Add cause information for certain error types
        match self {
            Self::ClaimFailed { source, .. } => {
                let _ = write!(output, "\n\nCause: {}", cause_description(source));
                output.push_str("\n\nWhat happened:");
                output.push_str("\n  1. You read the task and captured its ETag");
                output.push_str("\n  2. Another worker updated the task (new ETag)");
                output.push_str("\n  3. Your conditional write failed because ETags didn't match");
                output.push_str("\n\nThis is not a bug - it's the system working correctly!");
            }
            Self::ConnectionFailed { details, endpoint } => {
                if let Some(ep) = endpoint {
                    let _ = write!(output, "\n\nEndpoint: {ep}");
                }
                let _ = write!(output, "\n\nDetails: {details}");
            }
            _ => {}
        }

        let _ = write!(output, "\n\nSuggestion:\n  {}", self.suggestion());
        output
    }

    /// Returns true if this error type supports rich display.
    #[must_use]
    pub const fn has_rich_display(&self) -> bool {
        true
    }

    /// Creates a `ClaimFailed` error from a task ID and storage error.
    #[must_use]
    pub const fn claim_failed(task_id: Uuid, source: StorageError) -> Self {
        Self::ClaimFailed { task_id, source }
    }

    /// Creates a `NotFound` error from a task ID.
    #[must_use]
    pub const fn not_found(task_id: Uuid) -> Self {
        Self::NotFound { task_id }
    }

    /// Creates a `ConnectionFailed` error.
    #[must_use]
    pub fn connection_failed(details: impl Into<String>, endpoint: Option<String>) -> Self {
        Self::ConnectionFailed {
            details: details.into(),
            endpoint,
        }
    }
}

/// Returns a human-readable description of the cause for display purposes.
fn cause_description(err: &StorageError) -> String {
    match err {
        StorageError::PreconditionFailed { .. } => {
            "ETag mismatch (HTTP 412 Precondition Failed)".to_string()
        }
        StorageError::NotFound { .. } => "Object not found (HTTP 404)".to_string(),
        StorageError::ConnectionError(details) => format!("Connection failed: {details}"),
        StorageError::AccessDenied { bucket } => {
            format!("Access denied to bucket '{bucket}' (HTTP 403)")
        }
        other => other.to_string(),
    }
}

/// Trait for errors that support rich display with suggestions.
pub trait RichError: std::error::Error {
    /// Returns a helpful suggestion for resolving this error.
    fn suggestion(&self) -> &'static str;

    /// Returns a richly formatted error message with context and suggestions.
    fn display_rich(&self) -> String;
}

impl RichError for StorageError {
    fn suggestion(&self) -> &'static str {
        Self::suggestion(self)
    }

    fn display_rich(&self) -> String {
        Self::display_rich(self)
    }
}

impl RichError for TaskOperationError {
    fn suggestion(&self) -> &'static str {
        Self::suggestion(self)
    }

    fn display_rich(&self) -> String {
        Self::display_rich(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_claim_failed_suggestion() {
        let err = TaskOperationError::ClaimFailed {
            task_id: Uuid::nil(),
            source: StorageError::PreconditionFailed {
                key: "test".to_string(),
            },
        };
        assert!(err.suggestion().contains("Another worker"));
        assert!(err.suggestion().contains("normal behavior"));
    }

    #[test]
    fn test_not_found_suggestion() {
        let err = TaskOperationError::NotFound {
            task_id: Uuid::nil(),
        };
        assert!(err.suggestion().contains("archived"));
        assert!(err.suggestion().contains("oq history"));
    }

    #[test]
    fn test_already_completed_suggestion() {
        let err = TaskOperationError::AlreadyCompleted {
            task_id: Uuid::nil(),
        };
        assert!(err.suggestion().contains("finished"));
        assert!(err.suggestion().contains("oq status"));
    }

    #[test]
    fn test_connection_failed_suggestion() {
        let err = TaskOperationError::ConnectionFailed {
            details: "connection refused".to_string(),
            endpoint: Some("http://localhost:3900".to_string()),
        };
        assert!(err.suggestion().contains("S3_ENDPOINT"));
        assert!(err.suggestion().contains("docker compose"));
    }

    #[test]
    fn test_permission_denied_suggestion() {
        let err = TaskOperationError::PermissionDenied {
            bucket: "my-bucket".to_string(),
        };
        assert!(err.suggestion().contains("AWS_ACCESS_KEY_ID"));
        assert!(err.suggestion().contains("bucket policy"));
    }

    #[test]
    fn test_display_rich_claim_failed() {
        let err = TaskOperationError::ClaimFailed {
            task_id: Uuid::nil(),
            source: StorageError::PreconditionFailed {
                key: "tasks/0/00000000-0000-0000-0000-000000000000.json".to_string(),
            },
        };
        let rich = err.display_rich();
        assert!(rich.contains("Error:"));
        assert!(rich.contains("Cause:"));
        assert!(rich.contains("ETag mismatch"));
        assert!(rich.contains("What happened:"));
        assert!(rich.contains("Suggestion:"));
    }

    #[test]
    fn test_display_rich_connection_failed() {
        let err = TaskOperationError::ConnectionFailed {
            details: "connection refused".to_string(),
            endpoint: Some("http://localhost:3900".to_string()),
        };
        let rich = err.display_rich();
        assert!(rich.contains("Error:"));
        assert!(rich.contains("Endpoint: http://localhost:3900"));
        assert!(rich.contains("Details:"));
        assert!(rich.contains("Suggestion:"));
    }

    #[test]
    fn test_storage_error_rich_display() {
        let err = StorageError::ConnectionError("timeout".to_string());
        let rich = err.display_rich();
        assert!(rich.contains("Error:"));
        assert!(rich.contains("Suggestion:"));
        assert!(rich.contains("S3 endpoint"));
    }
}
