//! Python bindings for workflow state operations.
//!
//! This module provides `PyO3` bindings for the `StateManager`,
//! enabling Python code to manage workflow state in S3.

use pyo3::prelude::*;
use pyo3::types::PyBytes;
use std::sync::Arc;

use crate::state::StateManager;
use oq::storage::S3Client;

use super::types::PyWorkflowState;

/// Python wrapper for the `StateManager`.
///
/// Manages workflow state in S3 storage with support for optimistic
/// concurrency control via `ETags`.
///
/// # Example
///
/// ```python
/// # Get workflow state
/// state, etag = await state_manager.get("wf-123")
///
/// # Update with CAS
/// new_etag = await state_manager.update("wf-123", state, etag, "2026-01-28T12:00:00Z")
///
/// # Create new workflow state
/// etag = await state_manager.create("wf-123", state)
/// ```
#[pyclass(name = "StateManager")]
pub struct PyStateManager {
    inner: StateManager,
}

impl PyStateManager {
    /// Create a new `PyStateManager` from an S3 client.
    #[allow(clippy::missing_const_for_fn)]
    pub fn new(client: Arc<S3Client>) -> Self {
        Self {
            inner: StateManager::new(client),
        }
    }

    /// Create from an existing `StateManager`.
    #[allow(clippy::missing_const_for_fn)]
    pub fn from_manager(manager: StateManager) -> Self {
        Self { inner: manager }
    }

    /// Get the inner `StateManager` (for internal use).
    #[allow(clippy::missing_const_for_fn)]
    pub fn inner(&self) -> &StateManager {
        &self.inner
    }
}

#[pymethods]
impl PyStateManager {
    /// Get workflow state from storage.
    ///
    /// # Arguments
    ///
    /// * `wf_id` - Workflow ID
    ///
    /// # Returns
    ///
    /// A tuple of (`WorkflowState`, etag). The etag can be used for
    /// conditional updates via `update(..., etag=etag)`.
    ///
    /// # Raises
    ///
    /// `RuntimeError`: If the workflow doesn't exist or read fails.
    ///
    /// # Example
    ///
    /// ```python
    /// state, etag = await state_manager.get("wf-123")
    /// print(f"Workflow status: {state.status}")
    /// ```
    fn get<'py>(&self, py: Python<'py>, wf_id: String) -> PyResult<Bound<'py, PyAny>> {
        let manager = self.inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let (state, etag) = manager
                .get(&wf_id)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

            Ok((PyWorkflowState::from(state), etag))
        })
    }

    /// Update workflow state with CAS (compare-and-swap).
    ///
    /// The update will only succeed if the current `ETag` matches the provided
    /// `etag`. This enables safe concurrent updates.
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
    /// # Raises
    ///
    /// `RuntimeError`: If the `ETag` doesn't match (state was modified by another process).
    ///
    /// # Example
    ///
    /// ```python
    /// state, etag = await state_manager.get("wf-123")
    /// state.status = "running"
    /// new_etag = await state_manager.update("wf-123", state, etag, "2026-01-28T12:00:00Z")
    /// ```
    fn update<'py>(
        &self,
        py: Python<'py>,
        wf_id: String,
        state: PyWorkflowState,
        etag: String,
        now: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let manager = self.inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let rust_state: crate::types::WorkflowState = state.into();
            let new_etag = manager
                .update(&wf_id, &rust_state, &etag, &now)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

            Ok(new_etag)
        })
    }

    /// Create initial workflow state.
    ///
    /// This does not use CAS - it will overwrite existing state.
    /// Use with caution and ensure idempotency at a higher level.
    ///
    /// # Arguments
    ///
    /// * `wf_id` - Workflow ID
    /// * `state` - Initial state to write
    ///
    /// # Returns
    ///
    /// `ETag` of the created state.
    ///
    /// # Example
    ///
    /// ```python
    /// state = WorkflowState(
    ///     id="wf-123",
    ///     workflow_type="order_fulfillment",
    ///     ...
    /// )
    /// etag = await state_manager.create("wf-123", state)
    /// ```
    fn create<'py>(
        &self,
        py: Python<'py>,
        wf_id: String,
        state: PyWorkflowState,
    ) -> PyResult<Bound<'py, PyAny>> {
        let manager = self.inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let rust_state: crate::types::WorkflowState = state.into();
            let etag = manager
                .create(&wf_id, &rust_state)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

            Ok(etag)
        })
    }

    /// Check if a workflow exists.
    ///
    /// # Arguments
    ///
    /// * `wf_id` - Workflow ID
    ///
    /// # Returns
    ///
    /// True if the workflow exists, False otherwise.
    ///
    /// # Example
    ///
    /// ```python
    /// if await state_manager.exists("wf-123"):
    ///     state, etag = await state_manager.get("wf-123")
    /// ```
    fn exists<'py>(&self, py: Python<'py>, wf_id: String) -> PyResult<Bound<'py, PyAny>> {
        let manager = self.inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let exists = manager
                .exists(&wf_id)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

            Ok(exists)
        })
    }

    /// Delete workflow state.
    ///
    /// This is primarily for cleanup and testing. In production, consider
    /// archiving workflows instead of deleting them.
    ///
    /// # Arguments
    ///
    /// * `wf_id` - Workflow ID
    ///
    /// # Example
    ///
    /// ```python
    /// await state_manager.delete("wf-123")
    /// ```
    fn delete<'py>(&self, py: Python<'py>, wf_id: String) -> PyResult<Bound<'py, PyAny>> {
        let manager = self.inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            manager
                .delete(&wf_id)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

            Ok(())
        })
    }

    /// List workflow IDs with an optional prefix filter.
    ///
    /// # Arguments
    ///
    /// * `prefix` - Optional prefix to filter workflows (appended to "workflow/")
    /// * `limit` - Maximum number of workflows to return (default: 1000)
    ///
    /// # Returns
    ///
    /// List of workflow IDs (not full keys).
    ///
    /// # Example
    ///
    /// ```python
    /// # List all workflows
    /// wf_ids = await state_manager.list("", 100)
    ///
    /// # List workflows starting with "order-"
    /// order_wfs = await state_manager.list("order-", 50)
    /// ```
    #[pyo3(signature = (prefix="", limit=1000))]
    fn list<'py>(&self, py: Python<'py>, prefix: &str, limit: i32) -> PyResult<Bound<'py, PyAny>> {
        let manager = self.inner.clone();
        let prefix = prefix.to_string();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let ids = manager
                .list(&prefix, limit)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

            Ok(ids)
        })
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
    /// * `result` - Step result as bytes (typically JSON)
    ///
    /// # Returns
    ///
    /// `ETag` of the saved result.
    ///
    /// # Example
    ///
    /// ```python
    /// import json
    /// result = json.dumps({"processed": True, "count": 42}).encode()
    /// etag = await state_manager.save_step_result("wf-123", "process_order", result)
    /// ```
    fn save_step_result<'py>(
        &self,
        py: Python<'py>,
        wf_id: String,
        step: String,
        result: Vec<u8>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let manager = self.inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let etag = manager
                .save_step_result(&wf_id, &step, &result)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

            Ok(etag)
        })
    }

    /// Get step result from storage.
    ///
    /// # Arguments
    ///
    /// * `wf_id` - Workflow ID
    /// * `step` - Name of the step
    ///
    /// # Returns
    ///
    /// Tuple of (result bytes, `ETag`) or None if not found.
    ///
    /// # Example
    ///
    /// ```python
    /// import json
    /// result = await state_manager.get_step_result("wf-123", "process_order")
    /// if result:
    ///     data, etag = result
    ///     parsed = json.loads(data)
    ///     print(f"Step result: {parsed}")
    /// ```
    fn get_step_result<'py>(
        &self,
        py: Python<'py>,
        wf_id: String,
        step: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let manager = self.inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = manager
                .get_step_result(&wf_id, &step)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

            match result {
                Some((data, etag)) => Python::with_gil(|py| {
                    let bytes = PyBytes::new(py, &data);
                    Ok(Some((bytes.unbind(), etag)))
                }),
                None => Ok(None),
            }
        })
    }

    #[allow(clippy::missing_const_for_fn, clippy::unused_self)]
    fn __repr__(&self) -> String {
        "StateManager()".to_string()
    }
}
