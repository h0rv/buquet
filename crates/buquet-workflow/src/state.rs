//! Workflow state management operations.
//!
//! This module provides state management for workflows stored in S3.
//! State is stored at `workflow/{wf_id}/state.json` and step results
//! are stored at `workflow/{wf_id}/steps/{step_name}.json`.

use crate::error::{Result, WorkflowError};
use crate::types::WorkflowState;
use buquet::storage::{PutCondition, S3Client};
use std::sync::Arc;

/// Get the S3 key for workflow state.
fn state_key(wf_id: &str) -> String {
    format!("workflow/{wf_id}/state.json")
}

/// Get the S3 key for step result.
fn step_result_key(wf_id: &str, step_name: &str) -> String {
    format!("workflow/{wf_id}/steps/{step_name}.json")
}

/// Manages workflow state in S3.
///
/// Provides operations for creating, reading, updating, and deleting
/// workflow state with support for optimistic concurrency control via `ETags`.
#[derive(Debug, Clone)]
pub struct StateManager {
    client: Arc<S3Client>,
}

impl StateManager {
    /// Create a new `StateManager` with the given S3 client.
    #[must_use]
    pub const fn new(client: Arc<S3Client>) -> Self {
        Self { client }
    }

    /// Get workflow state from storage.
    ///
    /// Returns the workflow state and its `ETag` for use in CAS updates.
    ///
    /// # Errors
    ///
    /// Returns `WorkflowError::NotFound` if the workflow doesn't exist.
    pub async fn get(&self, wf_id: &str) -> Result<(WorkflowState, String)> {
        let key = state_key(wf_id);
        let (data, etag) = self.client.get_object(&key).await.map_err(|e| {
            if matches!(e, buquet::storage::StorageError::NotFound { .. }) {
                WorkflowError::NotFound(wf_id.to_string())
            } else {
                WorkflowError::Storage(e)
            }
        })?;

        let state = WorkflowState::from_json(&data)?;
        Ok((state, etag))
    }

    /// Update workflow state with CAS (compare-and-swap).
    ///
    /// The update will only succeed if the current `ETag` matches the provided
    /// `etag`. The `now` timestamp is used to update the `updated_at` field.
    ///
    /// # Arguments
    ///
    /// * `wf_id` - Workflow ID
    /// * `state` - New state to write
    /// * `etag` - Expected `ETag` from previous read
    /// * `now` - Current timestamp to set as `updated_at`
    ///
    /// # Returns
    ///
    /// New `ETag` after successful write.
    ///
    /// # Errors
    ///
    /// Returns `WorkflowError::Conflict` if the `ETag` doesn't match (state was
    /// modified by another process).
    pub async fn update(
        &self,
        wf_id: &str,
        state: &WorkflowState,
        etag: &str,
        now: &str,
    ) -> Result<String> {
        // Clone and update timestamp
        let mut state = state.clone();
        state.updated_at = now.to_string();

        let key = state_key(wf_id);
        let data = state.to_json()?;

        let new_etag = self
            .client
            .put_object(&key, data, PutCondition::IfMatch(etag.to_string()))
            .await
            .map_err(|e| {
                if matches!(e, buquet::storage::StorageError::PreconditionFailed { .. }) {
                    WorkflowError::Conflict
                } else {
                    WorkflowError::Storage(e)
                }
            })?;

        Ok(new_etag)
    }

    /// Create initial workflow state.
    ///
    /// This does not use CAS - it will overwrite existing state.
    /// Use with caution and ensure idempotency at a higher level.
    ///
    /// # Returns
    ///
    /// `ETag` of the created state.
    pub async fn create(&self, wf_id: &str, state: &WorkflowState) -> Result<String> {
        let key = state_key(wf_id);
        let data = state.to_json()?;

        let etag = self
            .client
            .put_object(&key, data, PutCondition::None)
            .await?;

        Ok(etag)
    }

    /// Create workflow state only if it doesn't already exist.
    ///
    /// Uses `IfNoneMatch` condition for idempotent creation.
    ///
    /// # Returns
    ///
    /// `Ok(Some(etag))` if created, `Ok(None)` if already exists.
    pub async fn create_if_not_exists(
        &self,
        wf_id: &str,
        state: &WorkflowState,
    ) -> Result<Option<String>> {
        let key = state_key(wf_id);
        let data = state.to_json()?;

        match self
            .client
            .put_object(&key, data, PutCondition::IfNoneMatch)
            .await
        {
            Ok(etag) => Ok(Some(etag)),
            Err(buquet::storage::StorageError::PreconditionFailed { .. }) => Ok(None),
            Err(e) => Err(WorkflowError::Storage(e)),
        }
    }

    /// Check if a workflow exists.
    pub async fn exists(&self, wf_id: &str) -> Result<bool> {
        let key = state_key(wf_id);
        let exists = self.client.head_object(&key).await?;
        Ok(exists)
    }

    /// Delete workflow state.
    ///
    /// This is primarily for cleanup and testing. In production, consider
    /// archiving workflows instead of deleting them.
    pub async fn delete(&self, wf_id: &str) -> Result<()> {
        let key = state_key(wf_id);
        self.client.delete_object(&key).await?;
        Ok(())
    }

    /// List workflow IDs with an optional prefix filter.
    ///
    /// # Arguments
    ///
    /// * `prefix` - Optional prefix to filter workflows (appended to "workflow/")
    /// * `limit` - Maximum number of workflows to return
    ///
    /// # Returns
    ///
    /// List of workflow IDs (not full keys).
    pub async fn list(&self, prefix: &str, limit: i32) -> Result<Vec<String>> {
        let search_prefix = format!("workflow/{prefix}");
        let keys = self
            .client
            .list_objects_paginated(&search_prefix, limit)
            .await?;

        // Extract workflow IDs from state.json keys
        // Key format: workflow/{wf_id}/state.json or workflow/{wf_id}/steps/...
        let mut workflow_ids = std::collections::HashSet::new();
        for key in keys {
            let parts: Vec<&str> = key.split('/').collect();
            if parts.len() >= 2 {
                workflow_ids.insert(parts[1].to_string());
            }
        }

        let mut ids: Vec<String> = workflow_ids.into_iter().collect();
        ids.sort();
        Ok(ids)
    }

    /// Save step result to storage.
    ///
    /// Step results are immutable after completion and stored separately
    /// from workflow state for easier inspection and potential payload
    /// reference support.
    ///
    /// # Arguments
    ///
    /// * `wf_id` - Workflow ID
    /// * `step` - Name of the completed step
    /// * `result` - Serialized step result (JSON bytes)
    ///
    /// # Returns
    ///
    /// `Ok(Some(etag))` if created, `Ok(None)` if result already exists.
    pub async fn save_step_result(
        &self,
        wf_id: &str,
        step: &str,
        result: &[u8],
    ) -> Result<Option<String>> {
        let key = step_result_key(wf_id, step);
        match self
            .client
            .put_object(&key, result.to_vec(), PutCondition::IfNoneMatch)
            .await
        {
            Ok(etag) => Ok(Some(etag)),
            Err(buquet::storage::StorageError::PreconditionFailed { .. }) => Ok(None),
            Err(e) => Err(WorkflowError::Storage(e)),
        }
    }

    /// Get step result from storage.
    ///
    /// # Returns
    ///
    /// Tuple of (result bytes, `ETag`) or `None` if not found.
    pub async fn get_step_result(
        &self,
        wf_id: &str,
        step: &str,
    ) -> Result<Option<(Vec<u8>, String)>> {
        let key = step_result_key(wf_id, step);
        match self.client.get_object(&key).await {
            Ok((data, etag)) => Ok(Some((data, etag))),
            Err(buquet::storage::StorageError::NotFound { .. }) => Ok(None),
            Err(e) => Err(WorkflowError::Storage(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_key() {
        assert_eq!(state_key("wf-123"), "workflow/wf-123/state.json");
    }

    #[test]
    fn test_step_result_key() {
        assert_eq!(
            step_result_key("wf-123", "process_order"),
            "workflow/wf-123/steps/process_order.json"
        );
    }
}
