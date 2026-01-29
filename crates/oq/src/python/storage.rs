//! Python bindings for low-level storage operations.
//!
//! This module provides direct S3 access for advanced use cases like
//! workflow orchestration that need to store custom objects.

use pyo3::prelude::*;
use pyo3::types::PyBytes;
use std::sync::Arc;

use crate::storage::{PutCondition, S3Client};

/// Low-level storage client for direct S3 operations.
///
/// Access via `queue.storage`. These operations work on arbitrary S3 keys
/// within the queue's bucket, enabling advanced use cases like workflow
/// state management.
///
/// # Example
///
/// ```python
/// # Read object
/// data, etag = await queue.storage.get("workflow/wf-123/state.json")
///
/// # Write with CAS
/// new_etag = await queue.storage.put(
///     "workflow/wf-123/state.json",
///     data,
///     if_match=etag,  # Fails if changed since read
/// )
///
/// # List objects
/// keys = await queue.storage.list("workflow/wf-123/signals/")
/// ```
#[pyclass(name = "StorageClient")]
#[derive(Clone)]
pub struct PyStorageClient {
    client: Arc<S3Client>,
}

impl PyStorageClient {
    /// Create a new storage client wrapping the given S3 client.
    pub const fn new(client: Arc<S3Client>) -> Self {
        Self { client }
    }
}

#[pymethods]
impl PyStorageClient {
    /// Get an object from storage.
    ///
    /// # Arguments
    ///
    /// * `key` - The object key (path)
    ///
    /// # Returns
    ///
    /// A tuple of (data: bytes, etag: str). The etag can be used for
    /// conditional updates via `put(..., if_match=etag)`.
    ///
    /// # Raises
    ///
    /// `RuntimeError`: If the object doesn't exist or read fails.
    ///
    /// # Example
    ///
    /// ```python
    /// data, etag = await queue.storage.get("workflow/wf-123/state.json")
    /// state = json.loads(data)
    /// ```
    fn get<'py>(&self, py: Python<'py>, key: String) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let (data, etag) = client
                .get_object(&key)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

            Python::with_gil(|py| {
                let bytes = PyBytes::new(py, &data);
                Ok((bytes.unbind(), etag))
            })
        })
    }

    /// Put an object to storage.
    ///
    /// # Arguments
    ///
    /// * `key` - The object key (path)
    /// * `data` - The object content as bytes
    /// * `if_match` - Optional etag for CAS (compare-and-swap) update.
    ///   If provided, the write only succeeds if the current object's
    ///   etag matches. This enables safe concurrent updates.
    ///
    /// # Returns
    ///
    /// The new etag of the written object.
    ///
    /// # Raises
    ///
    /// `RuntimeError`: If write fails or `if_match` condition not met.
    ///
    /// # Example
    ///
    /// ```python
    /// # Unconditional write
    /// etag = await queue.storage.put("my/key.json", b'{"x": 1}')
    ///
    /// # CAS update (fails if object changed since read)
    /// data, old_etag = await queue.storage.get("my/key.json")
    /// new_data = modify(data)
    /// new_etag = await queue.storage.put("my/key.json", new_data, if_match=old_etag)
    /// ```
    #[pyo3(signature = (key, data, if_match=None))]
    fn put<'py>(
        &self,
        py: Python<'py>,
        key: String,
        data: Vec<u8>,
        if_match: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let condition = if_match.map_or(PutCondition::None, PutCondition::IfMatch);

            let etag = client
                .put_object(&key, data, condition)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

            Ok(etag)
        })
    }

    /// Delete an object from storage.
    ///
    /// This is a best-effort operation. S3 returns success even if the
    /// object doesn't exist.
    ///
    /// # Arguments
    ///
    /// * `key` - The object key (path)
    ///
    /// # Example
    ///
    /// ```python
    /// await queue.storage.delete("workflow/wf-123/signals/old.json")
    /// ```
    fn delete<'py>(&self, py: Python<'py>, key: String) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .delete_object(&key)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

            Ok(())
        })
    }

    /// List objects by prefix.
    ///
    /// # Arguments
    ///
    /// * `prefix` - The key prefix to filter objects
    /// * `start_after` - Optional key to start listing after (exclusive).
    ///   Useful for cursor-based pagination.
    /// * `limit` - Maximum number of keys to return (default: 1000)
    ///
    /// # Returns
    ///
    /// A list of object keys matching the prefix.
    ///
    /// # Example
    ///
    /// ```python
    /// # List all signals
    /// keys = await queue.storage.list("workflow/wf-123/signals/approval/")
    ///
    /// # Cursor-based: list signals after a specific key
    /// keys = await queue.storage.list(
    ///     "workflow/wf-123/signals/approval/",
    ///     start_after="workflow/wf-123/signals/approval/2026-01-28T12:00:00Z_abc",
    /// )
    /// ```
    #[pyo3(signature = (prefix, start_after=None, limit=1000))]
    fn list<'py>(
        &self,
        py: Python<'py>,
        prefix: String,
        start_after: Option<String>,
        limit: i32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let (keys, _next_token) = client
                .list_objects_after(&prefix, limit, None, start_after.as_deref())
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

            Ok(keys)
        })
    }

    /// Check if an object exists.
    ///
    /// # Arguments
    ///
    /// * `key` - The object key (path)
    ///
    /// # Returns
    ///
    /// True if the object exists, False otherwise.
    ///
    /// # Example
    ///
    /// ```python
    /// if await queue.storage.exists("workflow/wf-123/state.json"):
    ///     data, etag = await queue.storage.get("workflow/wf-123/state.json")
    /// ```
    fn exists<'py>(&self, py: Python<'py>, key: String) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            match client.get_object(&key).await {
                Ok(_) => Ok(true),
                Err(crate::storage::StorageError::NotFound { .. }) => Ok(false),
                Err(e) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    e.to_string(),
                )),
            }
        })
    }

    fn __repr__(&self) -> String {
        format!("StorageClient(bucket='{}')", self.client.bucket())
    }
}
