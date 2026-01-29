//! Python bindings for signal operations.
//!
//! This module provides `PyO3` bindings for the `SignalManager` and related types.

use pyo3::prelude::*;
use std::sync::Arc;

use super::types::PySignal;
use crate::signals::SignalManager;
use oq::storage::S3Client;

/// Manager for workflow signal operations.
///
/// Provides methods to send, list, and consume signals for workflows.
/// Signals are stored as immutable S3 objects with timestamp-based keys
/// for ordering.
///
/// # Example
///
/// ```python
/// # Send a signal
/// signal_id = await signal_manager.send(
///     wf_id="wf-123",
///     name="approval",
///     payload={"approved": True, "by": "alice"},
///     now="2026-01-28T12:00:00Z"
/// )
///
/// # Get next signal
/// result = await signal_manager.get_next(
///     wf_id="wf-123",
///     name="approval",
///     cursor=None
/// )
/// if result:
///     cursor, signal = result
///     print(f"Received: {signal.payload}")
/// ```
#[pyclass(name = "SignalManager")]
#[derive(Clone)]
pub struct PySignalManager {
    inner: SignalManager,
}

impl PySignalManager {
    /// Create a new `PySignalManager` with the given S3 client.
    pub const fn new(client: Arc<S3Client>) -> Self {
        Self {
            inner: SignalManager::new(client),
        }
    }

    /// Create from an existing `SignalManager`.
    pub const fn from_manager(manager: SignalManager) -> Self {
        Self { inner: manager }
    }
}

#[pymethods]
impl PySignalManager {
    /// Send a signal to a workflow.
    ///
    /// Signals are stored as immutable S3 objects with timestamp-based keys
    /// for ordering. The timestamp is provided externally to allow for
    /// consistent time handling.
    ///
    /// # Arguments
    ///
    /// * `wf_id` - Workflow ID
    /// * `name` - Signal name (e.g., "approval", "`payment_received`")
    /// * `payload` - Signal data as a Python dict
    /// * `now` - Current timestamp in ISO 8601 format
    ///
    /// # Returns
    ///
    /// The signal ID (UUID).
    ///
    /// # Example
    ///
    /// ```python
    /// signal_id = await signal_manager.send(
    ///     "wf-123",
    ///     "approval",
    ///     {"approved": True},
    ///     "2026-01-28T12:00:00Z"
    /// )
    /// ```
    #[allow(clippy::needless_pass_by_value)]
    fn send<'py>(
        &self,
        py: Python<'py>,
        wf_id: String,
        name: String,
        payload: PyObject,
        now: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let payload_json: serde_json::Value = pythonize::depythonize(payload.bind(py))
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
        let manager = self.inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            manager
                .send(&wf_id, &name, payload_json, &now)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
        })
    }

    /// List signals after the cursor.
    ///
    /// # Arguments
    ///
    /// * `wf_id` - Workflow ID
    /// * `name` - Signal name
    /// * `cursor` - Optional cursor (`timestamp_uuid` suffix) to start after
    /// * `limit` - Maximum number of signals to return (default: 100)
    ///
    /// # Returns
    ///
    /// List of (cursor, Signal) tuples, where cursor is the position value.
    ///
    /// # Example
    ///
    /// ```python
    /// signals = await signal_manager.list("wf-123", "approval", cursor=None, limit=10)
    /// for cursor, signal in signals:
    ///     print(f"{cursor}: {signal.payload}")
    /// ```
    #[pyo3(signature = (wf_id, name, cursor=None, limit=100))]
    fn list<'py>(
        &self,
        py: Python<'py>,
        wf_id: String,
        name: String,
        cursor: Option<String>,
        limit: i32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let manager = self.inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let signals = manager
                .list(&wf_id, &name, cursor.as_deref(), limit)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

            // Convert to list of (cursor, PySignal) tuples
            let result: Vec<(String, PySignal)> = signals
                .into_iter()
                .map(|(cursor, signal)| (cursor, PySignal::from(signal)))
                .collect();

            Ok(result)
        })
    }

    /// Get the next unconsumed signal.
    ///
    /// This is a convenience wrapper around list that returns only the first signal.
    ///
    /// # Arguments
    ///
    /// * `wf_id` - Workflow ID
    /// * `name` - Signal name
    /// * `cursor` - Current cursor position (None to start from beginning)
    ///
    /// # Returns
    ///
    /// Tuple of (`new_cursor`, Signal) or None if no signals available.
    ///
    /// # Example
    ///
    /// ```python
    /// result = await signal_manager.get_next("wf-123", "approval", cursor=None)
    /// if result:
    ///     new_cursor, signal = result
    ///     print(f"Got signal: {signal.id}")
    /// ```
    #[pyo3(signature = (wf_id, name, cursor=None))]
    fn get_next<'py>(
        &self,
        py: Python<'py>,
        wf_id: String,
        name: String,
        cursor: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let manager = self.inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = manager
                .get_next(&wf_id, &name, cursor.as_deref())
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

            Ok(result.map(|(cursor, signal)| (cursor, PySignal::from(signal))))
        })
    }

    /// Count signals after the cursor.
    ///
    /// # Arguments
    ///
    /// * `wf_id` - Workflow ID
    /// * `name` - Signal name
    /// * `cursor` - Optional cursor to count signals after
    ///
    /// # Returns
    ///
    /// Number of signals available.
    ///
    /// # Example
    ///
    /// ```python
    /// count = await signal_manager.count("wf-123", "approval")
    /// print(f"Pending signals: {count}")
    /// ```
    #[pyo3(signature = (wf_id, name, cursor=None))]
    fn count<'py>(
        &self,
        py: Python<'py>,
        wf_id: String,
        name: String,
        cursor: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let manager = self.inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let count = manager
                .count(&wf_id, &name, cursor.as_deref())
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

            Ok(count)
        })
    }

    /// Delete all signals of a given name (for cleanup/testing).
    ///
    /// Note: In production, signals should be retained for audit purposes.
    /// Use retention policies instead of explicit deletion.
    ///
    /// # Arguments
    ///
    /// * `wf_id` - Workflow ID
    /// * `name` - Signal name
    ///
    /// # Returns
    ///
    /// Number of signals deleted.
    ///
    /// # Example
    ///
    /// ```python
    /// deleted = await signal_manager.delete_all("wf-123", "approval")
    /// print(f"Deleted {deleted} signals")
    /// ```
    fn delete_all<'py>(
        &self,
        py: Python<'py>,
        wf_id: String,
        name: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let manager = self.inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let count = manager
                .delete_all(&wf_id, &name)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

            Ok(count)
        })
    }

    #[allow(clippy::unused_self, clippy::missing_const_for_fn)]
    fn __repr__(&self) -> &'static str {
        "SignalManager()"
    }
}

/// Parse a signal key into its components.
///
/// # Arguments
///
/// * `key` - Full S3 key (e.g., "workflow/wf-123/signals/approval/2026-01-28T12:00:00Z_abc123.json")
///
/// # Returns
///
/// Tuple of (`workflow_id`, `signal_name`, timestamp, `signal_id`), or None if parsing fails.
///
/// # Example
///
/// ```python
/// result = parse_signal_key("workflow/wf-123/signals/approval/2026-01-28T12:00:00Z_abc123.json")
/// if result:
///     wf_id, name, timestamp, signal_id = result
///     print(f"Signal {signal_id} for workflow {wf_id}")
/// ```
#[pyfunction]
pub fn parse_signal_key(key: &str) -> Option<(String, String, String, String)> {
    crate::signals::parse_signal_key(key)
}
