//! Error types for buquet-workflow.

use thiserror::Error;

/// Errors that can occur in workflow operations.
#[derive(Error, Debug)]
pub enum WorkflowError {
    /// Storage error from buquet.
    #[error("storage error: {0}")]
    Storage(#[from] crate::storage::StorageError),

    /// State not found.
    #[error("workflow not found: {0}")]
    NotFound(String),

    /// CAS conflict (state was modified).
    #[error("state conflict: workflow was modified")]
    Conflict,

    /// Invalid workflow state.
    #[error("invalid state: {0}")]
    InvalidState(String),

    /// DAG validation error.
    #[error("dag error: {0}")]
    DagError(String),

    /// JSON serialization/deserialization error.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    /// Workflow already in terminal state.
    #[error("workflow in terminal state: {0}")]
    TerminalState(String),
}

/// Result type for workflow operations.
pub type Result<T> = std::result::Result<T, WorkflowError>;
