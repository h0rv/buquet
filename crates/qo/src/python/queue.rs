//! Python bindings for queue operations.
//!
//! This module provides the `PyQueue` class that wraps the Rust `Queue`
//! and exposes async methods as Python awaitables.

use std::env;
use std::sync::Arc;

use futures::future::join_all;
use pyo3::prelude::*;
use pyo3::types::PyList;
use serde_json::Value;

use crate::queue::{connect, ConnectOptions, IdempotencyScope, Queue, SubmitOptions, TaskSchema};

use super::storage::PyStorageClient;
use super::task::{py_to_json, PyRetryPolicy, PyTask, PyTaskStatus};

/// A connection to the S3-based task queue.
///
/// Use `qo.connect()` to create a new queue connection.
#[pyclass(name = "Queue")]
#[derive(Clone)]
pub struct PyQueue {
    inner: Arc<Queue>,
}

impl PyQueue {
    /// Creates a new `PyQueue` from a Rust `Queue`.
    pub fn new(queue: Queue) -> Self {
        Self {
            inner: Arc::new(queue),
        }
    }

    /// Returns a reference to the inner Queue.
    pub fn inner(&self) -> &Queue {
        &self.inner
    }
}

/// Internal implementation of `connect()`.
///
/// Delegates to the Rust `connect()` function which handles all config resolution.
pub async fn connect_impl(
    endpoint: Option<String>,
    bucket: Option<String>,
    region: Option<String>,
    shard_prefix_len: Option<usize>,
) -> PyResult<PyQueue> {
    // Build options from provided arguments (Rust connect handles env/config fallbacks)
    let options =
        if endpoint.is_some() || bucket.is_some() || region.is_some() || shard_prefix_len.is_some()
        {
            Some(ConnectOptions {
                endpoint,
                bucket,
                region,
                shard_prefix_len,
            })
        } else {
            None
        };

    // Use Rust core connect function
    let queue = connect(options)
        .await
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    // Check versioning (unless disabled)
    let allow_no_versioning = env::var("QO_ALLOW_NO_VERSIONING")
        .map(|value| matches!(value.to_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(false);

    if !allow_no_versioning {
        queue
            .client()
            .ensure_bucket_versioning_enabled()
            .await
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
    }

    Ok(PyQueue::new(queue))
}

/// Result of cancelling tasks by type.
///
/// Contains lists of successfully cancelled tasks and failures.
#[pyclass(name = "CancelByTypeResult")]
#[derive(Clone)]
pub struct PyCancelByTypeResult {
    cancelled: Vec<PyTask>,
    failed: Vec<(String, String)>,
}

#[pymethods]
impl PyCancelByTypeResult {
    /// List of successfully cancelled tasks.
    #[getter]
    fn cancelled(&self) -> Vec<PyTask> {
        self.cancelled.clone()
    }

    /// List of (`task_id`, `error_message`) tuples for tasks that failed to cancel.
    #[getter]
    fn failed(&self) -> Vec<(String, String)> {
        self.failed.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "CancelByTypeResult(cancelled={}, failed={})",
            self.cancelled.len(),
            self.failed.len()
        )
    }
}

#[pymethods]
impl PyQueue {
    /// Submit a new task to the queue.
    ///
    /// # Arguments
    ///
    /// * `task_type` - The type of task (e.g., `send_email`, `process_image`)
    /// * `input` - The input data for the task as a dict
    /// * `timeout_seconds` - Optional timeout in seconds (default: 300)
    /// * `max_retries` - Optional maximum retries (default: 3)
    /// * `retry_policy` - Optional custom retry policy
    /// * `use_payload_refs` - Whether to store large inputs externally (default: False)
    /// * `payload_ref_threshold_bytes` - Size threshold for offloading (default: 256KB)
    /// * `idempotency_key` - Optional idempotency key for preventing duplicate tasks
    /// * `idempotency_ttl_days` - Optional TTL in days for idempotency record (default: 30)
    ///
    /// # Returns
    ///
    /// The created task, or existing task if idempotency key matched.
    ///
    /// # Example
    ///
    /// ```python
    /// # Basic submit
    /// task = await queue.submit("send_email", {"to": "user@example.com"})
    ///
    /// # With payload refs for large data
    /// task = await queue.submit(
    ///     "process_data",
    ///     {"large_data": [1] * 1000000},
    ///     use_payload_refs=True
    /// )
    ///
    /// # With idempotency key
    /// task = await queue.submit(
    ///     "charge_customer",
    ///     {"order_id": "ORD-123", "amount": 100},
    ///     idempotency_key="charge-ORD-123",
    ///     idempotency_ttl_days=30,
    /// )
    ///
    /// # Scheduled for later
    /// from datetime import datetime, timedelta
    /// task = await queue.submit(
    ///     "send_reminder",
    ///     {"user_id": "123"},
    ///     schedule_at=datetime.now() + timedelta(hours=1),
    /// )
    /// ```
    #[pyo3(signature = (task_type, input, timeout_seconds=None, max_retries=None, retry_policy=None, schedule_at=None, use_payload_refs=false, payload_ref_threshold_bytes=None, idempotency_key=None, idempotency_ttl_days=None, idempotency_scope=None, max_reschedules=None, ttl_seconds=None, expires_at=None))]
    #[allow(
        clippy::too_many_arguments,
        clippy::needless_pass_by_value,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    fn submit<'py>(
        &self,
        py: Python<'py>,
        task_type: String,
        input: Bound<'py, PyAny>,
        timeout_seconds: Option<u64>,
        max_retries: Option<u32>,
        retry_policy: Option<PyRetryPolicy>,
        schedule_at: Option<Bound<'py, PyAny>>,
        use_payload_refs: bool,
        payload_ref_threshold_bytes: Option<usize>,
        idempotency_key: Option<String>,
        idempotency_ttl_days: Option<i64>,
        idempotency_scope: Option<String>,
        max_reschedules: Option<u32>,
        ttl_seconds: Option<u64>,
        expires_at: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        use chrono::{DateTime, TimeZone, Utc};

        let input_json = py_to_json(py, &input)?;
        let queue = self.inner.clone();

        // Parse idempotency_scope string to enum
        let scope = match idempotency_scope.as_deref() {
            None => None,
            Some("task_type" | "TaskType") => Some(IdempotencyScope::TaskType),
            Some("queue" | "Queue") => Some(IdempotencyScope::Queue),
            Some(custom) => Some(IdempotencyScope::Custom(custom.to_string())),
        };

        // Parse schedule_at from Python datetime
        let schedule_at_chrono: Option<DateTime<Utc>> = if let Some(ref dt) = schedule_at {
            // Try to extract timestamp from Python datetime
            let timestamp: f64 = dt.call_method0("timestamp")?.extract()?;
            let secs = timestamp.trunc() as i64;
            let nsecs = ((timestamp.fract()) * 1_000_000_000.0) as u32;
            Some(Utc.timestamp_opt(secs, nsecs).single().ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid datetime")
            })?)
        } else {
            None
        };

        // Parse expires_at from Python datetime
        let expires_at_chrono: Option<DateTime<Utc>> = if let Some(ref dt) = expires_at {
            let timestamp: f64 = dt.call_method0("timestamp")?.extract()?;
            let secs = timestamp.trunc() as i64;
            let nsecs = ((timestamp.fract()) * 1_000_000_000.0) as u32;
            Some(Utc.timestamp_opt(secs, nsecs).single().ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid expires_at datetime")
            })?)
        } else {
            None
        };

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let options = SubmitOptions {
                timeout_seconds,
                max_retries,
                retry_policy: retry_policy.map(Into::into),
                schedule_at: schedule_at_chrono,
                use_payload_refs,
                payload_ref_threshold_bytes,
                idempotency_key,
                idempotency_ttl_days,
                idempotency_scope: scope,
                max_reschedules,
                ttl_seconds,
                expires_at: expires_at_chrono,
            };

            let task = queue
                .submit(task_type, input_json, options)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

            Ok(PyTask::new(task, None))
        })
    }

    /// Load the full input data for a task, resolving payload refs if needed.
    ///
    /// If the task's input was offloaded to S3 (via payload refs), this method
    /// loads and returns the full input data. Otherwise, returns the inline input.
    ///
    /// # Arguments
    ///
    /// * `task` - The task to load input for
    ///
    /// # Returns
    ///
    /// The input data as a Python object.
    ///
    /// # Example
    ///
    /// ```python
    /// task = await queue.get(task_id)
    /// input_data = await queue.load_input(task)
    /// ```
    fn load_input<'py>(&self, py: Python<'py>, task: &PyTask) -> PyResult<Bound<'py, PyAny>> {
        let queue = self.inner.clone();
        let task_inner = task.inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let input = queue
                .load_task_input(&task_inner)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

            Python::attach(|py| {
                pythonize::pythonize(py, &input)
                    .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
                    .map(Bound::unbind)
            })
        })
    }

    /// Load the full output data for a task, resolving payload refs if needed.
    ///
    /// If the task's output was offloaded to S3 (via payload refs), this method
    /// loads and returns the full output data. Otherwise, returns the inline output.
    ///
    /// # Arguments
    ///
    /// * `task` - The task to load output for
    ///
    /// # Returns
    ///
    /// The output data as a Python object, or None if task hasn't completed.
    ///
    /// # Example
    ///
    /// ```python
    /// task = await queue.get(task_id)
    /// output_data = await queue.load_output(task)
    /// ```
    fn load_output<'py>(&self, py: Python<'py>, task: &PyTask) -> PyResult<Bound<'py, PyAny>> {
        let queue = self.inner.clone();
        let task_inner = task.inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let output_opt = queue
                .load_task_output(&task_inner)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

            Python::attach(|py| {
                output_opt.map_or_else(
                    || Ok(py.None()),
                    |output| {
                        pythonize::pythonize(py, &output)
                            .map_err(|e| {
                                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
                            })
                            .map(Bound::unbind)
                    },
                )
            })
        })
    }

    /// Get a task by ID.
    ///
    /// # Arguments
    ///
    /// * `task_id` - The UUID of the task as a string
    ///
    /// # Returns
    ///
    /// The task if found, None otherwise.
    ///
    /// # Example
    ///
    /// ```python
    /// task = await queue.get("550e8400-e29b-41d4-a716-446655440000")
    /// if task:
    ///     print(f"Task status: {task.status}")
    /// ```
    fn get<'py>(&self, py: Python<'py>, task_id: String) -> PyResult<Bound<'py, PyAny>> {
        let queue = self.inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let task_id = uuid::Uuid::parse_str(&task_id)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

            let result = queue
                .get(task_id)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

            Ok(result.map(|(task, etag)| PyTask::new(task, Some(etag))))
        })
    }

    /// List tasks in a shard with optional status filter.
    ///
    /// # Arguments
    ///
    /// * `shard` - The shard to list (single hex character, 0-f)
    /// * `status` - Optional status to filter by
    /// * `limit` - Maximum number of tasks to return (default: 100)
    ///
    /// # Returns
    ///
    /// A list of tasks matching the criteria.
    ///
    /// # Example
    ///
    /// ```python
    /// # List all pending tasks in shard 'a'
    /// tasks = await queue.list("a", status=TaskStatus.Pending, limit=50)
    /// ```
    #[pyo3(signature = (shard, status=None, limit=100))]
    fn list<'py>(
        &self,
        py: Python<'py>,
        shard: String,
        status: Option<PyTaskStatus>,
        limit: i32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let queue = self.inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let status_filter = status.map(Into::into);

            let tasks = queue
                .list(&shard, status_filter, limit)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

            let py_tasks: Vec<PyTask> = tasks.into_iter().map(|t| PyTask::new(t, None)).collect();

            Ok(py_tasks)
        })
    }

    /// List ready task IDs in a shard.
    ///
    /// # Arguments
    ///
    /// * `shard` - The shard to list (single hex character, 0-f)
    /// * `limit` - Maximum number of task IDs to return (default: 100)
    ///
    /// # Returns
    ///
    /// A list of task UUID strings that are ready for claiming.
    ///
    /// # Example
    ///
    /// ```python
    /// ready_ids = await queue.list_ready("a", limit=10)
    /// for task_id in ready_ids:
    ///     task = await queue.get(task_id)
    /// ```
    #[pyo3(signature = (shard, limit=100))]
    fn list_ready<'py>(
        &self,
        py: Python<'py>,
        shard: String,
        limit: i32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let queue = self.inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let task_ids = queue
                .list_ready(&shard, limit)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

            let id_strings: Vec<String> = task_ids.iter().map(ToString::to_string).collect();
            Ok(id_strings)
        })
    }

    /// Get the version history of a task.
    ///
    /// # Arguments
    ///
    /// * `task_id` - The UUID of the task as a string
    ///
    /// # Returns
    ///
    /// A list of task versions, from newest to oldest.
    ///
    /// # Example
    ///
    /// ```python
    /// history = await queue.get_history("550e8400-e29b-41d4-a716-446655440000")
    /// for version in history:
    ///     print(f"Status: {version.status}, Updated: {version.updated_at}")
    /// ```
    fn get_history<'py>(&self, py: Python<'py>, task_id: String) -> PyResult<Bound<'py, PyAny>> {
        let queue = self.inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let task_id = uuid::Uuid::parse_str(&task_id)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

            let tasks = queue
                .get_history(task_id)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

            let py_tasks: Vec<PyTask> = tasks.into_iter().map(|t| PyTask::new(t, None)).collect();

            Ok(py_tasks)
        })
    }

    /// Get the current authoritative time from S3 (RFC 3339).
    fn now<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let queue = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let now = queue
                .now()
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            Ok(now.to_rfc3339())
        })
    }

    /// Submit multiple tasks to the queue in parallel.
    ///
    /// # Arguments
    ///
    /// * `tasks` - A list of (`task_type`, input) tuples
    /// * `timeout_seconds` - Optional timeout in seconds for all tasks (default: 300)
    /// * `max_retries` - Optional maximum retries for all tasks (default: 3)
    /// * `retry_policy` - Optional custom retry policy for all tasks
    ///
    /// # Returns
    ///
    /// A list of created tasks.
    ///
    /// # Example
    ///
    /// ```python
    /// tasks = await queue.submit_many([
    ///     ("send_email", {"to": "alice@example.com", "subject": "Hello"}),
    ///     ("send_email", {"to": "bob@example.com", "subject": "Hi"}),
    ///     ("process_order", {"order_id": "ORD-123"}),
    /// ])
    /// ```
    #[allow(
        clippy::too_many_arguments,
        clippy::needless_pass_by_value,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    #[pyo3(signature = (tasks, timeout_seconds=None, max_retries=None, retry_policy=None, max_reschedules=None, ttl_seconds=None, expires_at=None))]
    fn submit_many<'py>(
        &self,
        py: Python<'py>,
        tasks: Bound<'py, PyList>,
        timeout_seconds: Option<u64>,
        max_retries: Option<u32>,
        retry_policy: Option<PyRetryPolicy>,
        max_reschedules: Option<u32>,
        ttl_seconds: Option<u64>,
        expires_at: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        use chrono::{DateTime, TimeZone, Utc};

        // Parse expires_at from Python datetime
        let expires_at_chrono: Option<DateTime<Utc>> = if let Some(dt) = expires_at {
            let timestamp: f64 = dt.call_method0("timestamp")?.extract()?;
            let secs = timestamp.trunc() as i64;
            let nsecs = ((timestamp.fract()) * 1_000_000_000.0) as u32;
            Some(Utc.timestamp_opt(secs, nsecs).single().ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid expires_at datetime")
            })?)
        } else {
            None
        };

        // Extract task data from Python list before entering async context
        let mut task_data: Vec<(String, Value)> = Vec::with_capacity(tasks.len());
        for item in tasks.iter() {
            let tuple = item.cast::<pyo3::types::PyTuple>().map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "Each item must be a tuple of (task_type, input)",
                )
            })?;

            if tuple.len() != 2 {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Each tuple must have exactly 2 elements: (task_type, input)",
                ));
            }

            let task_type: String = tuple.get_item(0)?.extract()?;
            let input_json = py_to_json(py, &tuple.get_item(1)?)?;
            task_data.push((task_type, input_json));
        }

        let queue = self.inner.clone();
        let retry_policy_rust = retry_policy.map(Into::into);

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let options = SubmitOptions {
                timeout_seconds,
                max_retries,
                max_reschedules,
                retry_policy: retry_policy_rust,
                schedule_at: None,
                use_payload_refs: false,
                payload_ref_threshold_bytes: None,
                idempotency_key: None,
                idempotency_ttl_days: None,
                idempotency_scope: None,
                ttl_seconds,
                expires_at: expires_at_chrono,
            };

            // Create futures for all task submissions
            let futures: Vec<_> = task_data
                .into_iter()
                .map(|(task_type, input)| {
                    let queue = queue.clone();
                    let opts = options.clone();
                    async move { queue.submit(task_type, input, opts).await }
                })
                .collect();

            // Execute all submissions in parallel
            let results = join_all(futures).await;

            // Collect results, failing if any submission failed
            let mut py_tasks = Vec::with_capacity(results.len());
            for result in results {
                let task = result.map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
                })?;
                py_tasks.push(PyTask::new(task, None));
            }

            Ok(py_tasks)
        })
    }

    /// Publish a schema for a task type.
    ///
    /// # Arguments
    ///
    /// * `task_type` - The task type name
    /// * `schema` - Dict with optional 'input' and 'output' JSON Schema objects
    ///
    /// # Example
    ///
    /// ```python
    /// await queue.publish_schema("send_email", {
    ///     "input": {"type": "object", "properties": {"to": {"type": "string"}}},
    ///     "output": {"type": "object", "properties": {"sent": {"type": "boolean"}}}
    /// })
    /// ```
    #[allow(clippy::needless_pass_by_value)]
    fn publish_schema<'py>(
        &self,
        py: Python<'py>,
        task_type: String,
        schema: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let queue = self.inner.clone();

        // Convert Python dict to TaskSchema
        let schema_value: Value = pythonize::depythonize(&schema)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

        let task_schema: TaskSchema = serde_json::from_value(schema_value)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            queue
                .publish_schema(&task_type, &task_schema)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            Ok(())
        })
    }

    /// Get a schema for a task type.
    ///
    /// # Arguments
    ///
    /// * `task_type` - The task type name
    ///
    /// # Returns
    ///
    /// Dict with 'input' and/or 'output' keys, or None if not found.
    ///
    /// # Example
    ///
    /// ```python
    /// schema = await queue.get_schema("send_email")
    /// if schema:
    ///     print(schema["input"])
    /// ```
    fn get_schema<'py>(&self, py: Python<'py>, task_type: String) -> PyResult<Bound<'py, PyAny>> {
        let queue = self.inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let schema_opt = queue
                .get_schema(&task_type)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

            match schema_opt {
                Some(schema) => {
                    let value = serde_json::to_value(&schema).map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
                    })?;
                    Python::attach(|py| {
                        pythonize::pythonize(py, &value)
                            .map_err(|e| {
                                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
                            })
                            .map(Bound::unbind)
                    })
                }
                None => Python::attach(|py| Ok(py.None())),
            }
        })
    }

    /// List all task types that have schemas.
    ///
    /// # Returns
    ///
    /// List of task type names.
    ///
    /// # Example
    ///
    /// ```python
    /// schemas = await queue.list_schemas()
    /// # Returns: ["send_email", "process_order"]
    /// ```
    fn list_schemas<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let queue = self.inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let schemas = queue
                .list_schemas()
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            Ok(schemas)
        })
    }

    /// Delete a schema for a task type.
    ///
    /// # Arguments
    ///
    /// * `task_type` - The task type name
    ///
    /// # Example
    ///
    /// ```python
    /// await queue.delete_schema("send_email")
    /// ```
    fn delete_schema<'py>(
        &self,
        py: Python<'py>,
        task_type: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let queue = self.inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            queue
                .delete_schema(&task_type)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            Ok(())
        })
    }

    /// Validate input data against a schema's input specification.
    ///
    /// This is synchronous - schema should be fetched first with `get_schema`.
    ///
    /// # Arguments
    ///
    /// * `schema` - Schema dict from `get_schema()`
    /// * `data` - Data to validate
    ///
    /// # Raises
    ///
    /// `ValueError`: If validation fails.
    ///
    /// # Example
    ///
    /// ```python
    /// schema = await queue.get_schema("send_email")
    /// queue.validate_input(schema, {"to": "user@example.com"})
    /// ```
    #[allow(clippy::needless_pass_by_value)]
    fn validate_input(
        &self,
        _py: Python<'_>,
        schema: Bound<'_, PyAny>,
        data: Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let schema_value: Value = pythonize::depythonize(&schema)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
        let task_schema: TaskSchema = serde_json::from_value(schema_value)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

        let data_value: Value = pythonize::depythonize(&data)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

        self.inner
            .validate_input(&task_schema, &data_value)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))
    }

    /// Validate output data against a schema's output specification.
    ///
    /// This is synchronous - schema should be fetched first with `get_schema`.
    ///
    /// # Arguments
    ///
    /// * `schema` - Schema dict from `get_schema()`
    /// * `data` - Data to validate
    ///
    /// # Raises
    ///
    /// `ValueError`: If validation fails.
    ///
    /// # Example
    ///
    /// ```python
    /// schema = await queue.get_schema("send_email")
    /// queue.validate_output(schema, {"sent": True})
    /// ```
    #[allow(clippy::needless_pass_by_value)]
    fn validate_output(
        &self,
        _py: Python<'_>,
        schema: Bound<'_, PyAny>,
        data: Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let schema_value: Value = pythonize::depythonize(&schema)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
        let task_schema: TaskSchema = serde_json::from_value(schema_value)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

        let data_value: Value = pythonize::depythonize(&data)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

        self.inner
            .validate_output(&task_schema, &data_value)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))
    }

    /// Returns the shard prefix length for this queue.
    #[getter]
    fn shard_prefix_len(&self) -> usize {
        self.inner.shard_prefix_len()
    }

    /// Returns a low-level storage client for direct S3 operations.
    ///
    /// Use this for advanced operations like workflow state management
    /// that need to read/write arbitrary objects in the queue's bucket.
    ///
    /// # Example
    ///
    /// ```python
    /// # Read/write workflow state
    /// data, etag = await queue.storage.get("workflow/wf-123/state.json")
    /// new_etag = await queue.storage.put("workflow/wf-123/state.json", data, if_match=etag)
    ///
    /// # List signals
    /// keys = await queue.storage.list("workflow/wf-123/signals/")
    /// ```
    #[getter]
    fn storage(&self) -> PyStorageClient {
        PyStorageClient::new(std::sync::Arc::new(self.inner.client().clone()))
    }

    /// Returns all shard prefixes for this queue's configuration.
    ///
    /// # Returns
    ///
    /// A list of shard prefix strings.
    ///
    /// # Example
    ///
    /// ```python
    /// queue = await qo.connect(shard_prefix_len=2)
    /// shards = queue.all_shards()  # ["00", "01", ..., "ff"]
    /// ```
    fn all_shards(&self) -> Vec<String> {
        self.inner.all_shards()
    }

    // ========== Schedule Management Methods ==========

    /// Create a new recurring schedule.
    ///
    /// # Arguments
    ///
    /// * `id` - Unique schedule identifier
    /// * `task_type` - The type of task to submit when triggered
    /// * `input` - The input data for the task as a dict
    /// * `cron` - Cron expression (5-field standard format, e.g., "0 9 * * *")
    /// * `timeout_seconds` - Optional task timeout in seconds
    /// * `max_retries` - Optional maximum retries for submitted tasks
    ///
    /// # Returns
    ///
    /// The created schedule.
    ///
    /// # Example
    ///
    /// ```python
    /// # Run every day at 9 AM UTC
    /// schedule = await queue.create_schedule(
    ///     "daily-report",
    ///     "generate_report",
    ///     {"report_type": "daily"},
    ///     "0 9 * * *"
    /// )
    ///
    /// ```
    #[pyo3(signature = (id, task_type, input, cron, timeout_seconds=None, max_retries=None))]
    #[allow(clippy::too_many_arguments, clippy::needless_pass_by_value)]
    fn create_schedule<'py>(
        &self,
        py: Python<'py>,
        id: String,
        task_type: String,
        input: Bound<'py, PyAny>,
        cron: String,
        timeout_seconds: Option<u64>,
        max_retries: Option<u32>,
    ) -> PyResult<Bound<'py, PyAny>> {
        use crate::queue::{Schedule, ScheduleManager};

        let input_json = py_to_json(py, &input)?;
        let client = self.inner.client().clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut schedule = Schedule::new(id, task_type, input_json, cron);
            schedule.timeout_seconds = timeout_seconds;
            schedule.max_retries = max_retries;

            let manager = ScheduleManager::new(client);
            manager
                .create(&schedule)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

            Ok(super::schedule::PySchedule::new(schedule))
        })
    }

    /// Get a schedule by ID.
    ///
    /// # Arguments
    ///
    /// * `id` - The schedule ID
    ///
    /// # Returns
    ///
    /// The schedule if found, None otherwise.
    ///
    /// # Example
    ///
    /// ```python
    /// schedule = await queue.get_schedule("daily-report")
    /// if schedule:
    ///     print(f"Schedule cron: {schedule.cron}")
    /// ```
    fn get_schedule<'py>(&self, py: Python<'py>, id: String) -> PyResult<Bound<'py, PyAny>> {
        use crate::queue::ScheduleManager;

        let client = self.inner.client().clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let manager = ScheduleManager::new(client);
            let result = manager
                .get(&id)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

            Ok(result.map(|(schedule, _etag)| super::schedule::PySchedule::new(schedule)))
        })
    }

    /// List all schedules.
    ///
    /// # Returns
    ///
    /// A list of all schedules.
    ///
    /// # Example
    ///
    /// ```python
    /// schedules = await queue.list_schedules()
    /// for schedule in schedules:
    ///     print(f"{schedule.id}: {schedule.cron} (enabled={schedule.enabled})")
    /// ```
    fn list_schedules<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        use crate::queue::ScheduleManager;

        let client = self.inner.client().clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let manager = ScheduleManager::new(client);
            let schedules = manager
                .list()
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

            let py_schedules: Vec<super::schedule::PySchedule> = schedules
                .into_iter()
                .map(super::schedule::PySchedule::new)
                .collect();

            Ok(py_schedules)
        })
    }

    /// Delete a schedule.
    ///
    /// # Arguments
    ///
    /// * `id` - The schedule ID to delete
    ///
    /// # Example
    ///
    /// ```python
    /// await queue.delete_schedule("daily-report")
    /// ```
    fn delete_schedule<'py>(&self, py: Python<'py>, id: String) -> PyResult<Bound<'py, PyAny>> {
        use crate::queue::ScheduleManager;

        let client = self.inner.client().clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let manager = ScheduleManager::new(client);
            manager
                .delete(&id)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

            Ok(())
        })
    }

    /// Enable a schedule.
    ///
    /// # Arguments
    ///
    /// * `id` - The schedule ID to enable
    ///
    /// # Example
    ///
    /// ```python
    /// await queue.enable_schedule("daily-report")
    /// ```
    fn enable_schedule<'py>(&self, py: Python<'py>, id: String) -> PyResult<Bound<'py, PyAny>> {
        use crate::queue::ScheduleManager;

        let client = self.inner.client().clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let manager = ScheduleManager::new(client);
            manager
                .enable(&id)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

            Ok(())
        })
    }

    /// Disable a schedule.
    ///
    /// # Arguments
    ///
    /// * `id` - The schedule ID to disable
    ///
    /// # Example
    ///
    /// ```python
    /// await queue.disable_schedule("daily-report")
    /// ```
    fn disable_schedule<'py>(&self, py: Python<'py>, id: String) -> PyResult<Bound<'py, PyAny>> {
        use crate::queue::ScheduleManager;

        let client = self.inner.client().clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let manager = ScheduleManager::new(client);
            manager
                .disable(&id)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

            Ok(())
        })
    }

    /// Trigger a schedule immediately, submitting a task from its configuration.
    ///
    /// This submits a task using the schedule's configured `task_type`, input,
    /// timeout, and retries. The schedule's cron timing is not affected.
    ///
    /// # Arguments
    ///
    /// * `id` - The schedule ID to trigger
    ///
    /// # Returns
    ///
    /// The submitted task.
    ///
    /// # Raises
    ///
    /// `RuntimeError`: If the schedule is not found.
    ///
    /// # Example
    ///
    /// ```python
    /// # Manually trigger a scheduled task
    /// task = await queue.trigger_schedule("daily-report")
    /// print(f"Triggered task: {task.id}")
    /// ```
    fn trigger_schedule<'py>(&self, py: Python<'py>, id: String) -> PyResult<Bound<'py, PyAny>> {
        use crate::queue::{ScheduleManager, SubmitOptions};

        let queue = self.inner.clone();
        let client = self.inner.client().clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let manager = ScheduleManager::new(client);
            let (schedule, _etag) = manager
                .get(&id)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?
                .ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                        "Schedule not found: {id}"
                    ))
                })?;

            let options = SubmitOptions {
                timeout_seconds: schedule.timeout_seconds,
                max_retries: schedule.max_retries,
                ..Default::default()
            };

            let task = queue
                .submit(schedule.task_type, schedule.input, options)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

            Ok(super::task::PyTask::new(task, None))
        })
    }

    /// Get the last run information for a schedule.
    ///
    /// # Arguments
    ///
    /// * `id` - The schedule ID
    ///
    /// # Returns
    ///
    /// The last run info if available, None otherwise.
    ///
    /// # Example
    ///
    /// ```python
    /// last_run = await queue.get_schedule_last_run("daily-report")
    /// if last_run:
    ///     print(f"Last run: {last_run.last_run_at}")
    ///     print(f"Next run: {last_run.next_run_at}")
    /// ```
    fn get_schedule_last_run<'py>(
        &self,
        py: Python<'py>,
        id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        use crate::queue::ScheduleManager;

        let client = self.inner.client().clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let manager = ScheduleManager::new(client);
            let result = manager
                .get_last_run(&id)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

            Ok(result.map(|(last_run, _etag)| super::schedule::PyScheduleLastRun::new(last_run)))
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "Queue(bucket='{}', shard_prefix_len={})",
            self.inner.client().bucket(),
            self.inner.shard_prefix_len()
        )
    }

    /// Mark a pending task as expired.
    ///
    /// Only tasks in `Pending` status can be marked as expired. Running tasks
    /// will continue to completion (expiration only affects pending tasks).
    ///
    /// # Arguments
    ///
    /// * `task_id` - The UUID of the task as a string
    ///
    /// # Returns
    ///
    /// The expired task on success.
    ///
    /// # Raises
    ///
    /// `RuntimeError`: If the task doesn't exist or cannot be marked as expired.
    ///
    /// # Example
    ///
    /// ```python
    /// # Mark a task as expired
    /// task = await queue.mark_expired("550e8400-e29b-41d4-a716-446655440000")
    /// assert task.status == TaskStatus.Expired
    /// ```
    fn mark_expired<'py>(&self, py: Python<'py>, task_id: String) -> PyResult<Bound<'py, PyAny>> {
        let queue = self.inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let task_id = uuid::Uuid::parse_str(&task_id)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

            let task = queue
                .mark_expired(task_id)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

            Ok(PyTask::new(task, None))
        })
    }

    /// Cancel a pending task.
    ///
    /// Only tasks in `Pending` status can be cancelled. Running tasks require
    /// cooperative cancellation through the `cancel_requested` flag.
    ///
    /// # Arguments
    ///
    /// * `task_id` - The UUID of the task as a string
    /// * `reason` - Optional reason for cancellation (stored in `last_error`)
    /// * `cancelled_by` - Optional identifier for who/what cancelled the task
    ///
    /// # Returns
    ///
    /// The cancelled task on success.
    ///
    /// # Raises
    ///
    /// `RuntimeError`: If the task doesn't exist or cannot be cancelled.
    ///
    /// # Example
    ///
    /// ```python
    /// # Cancel a pending task
    /// task = await queue.cancel("550e8400-e29b-41d4-a716-446655440000")
    ///
    /// # Cancel with reason
    /// task = await queue.cancel(
    ///     "550e8400-e29b-41d4-a716-446655440000",
    ///     reason="User requested cancellation",
    ///     cancelled_by="user:alice",
    /// )
    /// ```
    #[pyo3(signature = (task_id, reason=None, cancelled_by=None))]
    fn cancel<'py>(
        &self,
        py: Python<'py>,
        task_id: String,
        reason: Option<String>,
        cancelled_by: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let queue = self.inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let task_id = uuid::Uuid::parse_str(&task_id)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

            let task = queue
                .cancel_with_reason(task_id, reason.as_deref(), cancelled_by.as_deref())
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

            Ok(PyTask::new(task, None))
        })
    }

    /// Cancel multiple pending tasks by their IDs.
    ///
    /// Only tasks in `Pending` status can be cancelled. Tasks are cancelled in parallel.
    ///
    /// # Arguments
    ///
    /// * `task_ids` - List of task UUIDs as strings
    /// * `reason` - Optional reason for cancellation (stored in `last_error`)
    /// * `cancelled_by` - Optional identifier for who/what cancelled the tasks
    ///
    /// # Returns
    ///
    /// A list of cancelled tasks.
    ///
    /// # Raises
    ///
    /// `RuntimeError`: If any task doesn't exist or cannot be cancelled.
    ///
    /// # Example
    ///
    /// ```python
    /// # Cancel multiple tasks
    /// tasks = await queue.cancel_many([
    ///     "550e8400-e29b-41d4-a716-446655440000",
    ///     "550e8400-e29b-41d4-a716-446655440001",
    /// ])
    ///
    /// # Cancel with reason
    /// tasks = await queue.cancel_many(
    ///     task_ids,
    ///     reason="Batch cancellation",
    ///     cancelled_by="admin:cleanup",
    /// )
    /// ```
    #[pyo3(signature = (task_ids, reason=None, cancelled_by=None))]
    #[allow(clippy::needless_pass_by_value)]
    fn cancel_many<'py>(
        &self,
        py: Python<'py>,
        task_ids: Vec<String>,
        reason: Option<String>,
        cancelled_by: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let queue = self.inner.clone();

        // Parse all UUIDs before entering async context
        let parsed_ids: Vec<uuid::Uuid> = task_ids
            .iter()
            .map(|id| {
                uuid::Uuid::parse_str(id)
                    .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))
            })
            .collect::<PyResult<Vec<_>>>()?;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            // Create futures for all cancel operations
            let futures: Vec<_> = parsed_ids
                .into_iter()
                .map(|task_id| {
                    let queue = queue.clone();
                    let reason = reason.clone();
                    let cancelled_by = cancelled_by.clone();
                    async move {
                        queue
                            .cancel_with_reason(task_id, reason.as_deref(), cancelled_by.as_deref())
                            .await
                    }
                })
                .collect();

            // Execute all cancellations in parallel
            let results = join_all(futures).await;

            // Collect only successful cancellations (ignoring failures)
            let py_tasks: Vec<PyTask> = results
                .into_iter()
                .filter_map(Result::ok)
                .map(|task| PyTask::new(task, None))
                .collect();

            Ok(py_tasks)
        })
    }

    /// Cancel all pending tasks of a specific type.
    ///
    /// Iterates through all shards to find and cancel pending tasks matching
    /// the given task type. Tasks are cancelled in parallel.
    ///
    /// # Arguments
    ///
    /// * `task_type` - The task type to cancel
    /// * `reason` - Optional reason for cancellation (stored in `last_error`)
    /// * `cancelled_by` - Optional identifier for who/what cancelled the tasks
    ///
    /// # Returns
    ///
    /// A `CancelByTypeResult` containing cancelled tasks and failures.
    ///
    /// # Example
    ///
    /// ```python
    /// # Cancel all pending tasks of type "send_email"
    /// result = await queue.cancel_by_type("send_email")
    /// print(f"Cancelled {len(result.cancelled)} tasks")
    ///
    /// # Cancel with reason
    /// result = await queue.cancel_by_type(
    ///     "send_email",
    ///     reason="Feature deprecated",
    ///     cancelled_by="system:migration",
    /// )
    /// ```
    #[pyo3(signature = (task_type, reason=None, cancelled_by=None))]
    #[allow(clippy::needless_collect)]
    fn cancel_by_type<'py>(
        &self,
        py: Python<'py>,
        task_type: String,
        reason: Option<String>,
        cancelled_by: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        use crate::TaskStatus;

        let queue = self.inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let shards = queue.all_shards();
            let mut cancelled: Vec<PyTask> = Vec::new();
            let mut failed: Vec<(String, String)> = Vec::new();

            // Iterate through all shards
            for shard in shards {
                // List pending tasks in this shard (use a reasonable limit)
                let tasks = queue
                    .list(&shard, Some(TaskStatus::Pending), 10000)
                    .await
                    .map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
                    })?;

                // Filter tasks by type
                let matching_tasks: Vec<_> = tasks
                    .into_iter()
                    .filter(|t| t.task_type == task_type)
                    .collect();

                if matching_tasks.is_empty() {
                    continue;
                }

                // Create pairs of (task_id, future) for all cancel operations in this shard
                let task_futures: Vec<_> = matching_tasks
                    .into_iter()
                    .map(|task| {
                        let task_id = task.id;
                        let queue = queue.clone();
                        let reason = reason.clone();
                        let cancelled_by = cancelled_by.clone();
                        let future = async move {
                            queue
                                .cancel_with_reason(
                                    task.id,
                                    reason.as_deref(),
                                    cancelled_by.as_deref(),
                                )
                                .await
                        };
                        (task_id, future)
                    })
                    .collect();

                // Separate task_ids and futures for parallel execution
                let (task_ids, futures): (Vec<_>, Vec<_>) = task_futures.into_iter().unzip();

                // Execute all cancellations in parallel
                let results = join_all(futures).await;

                // Collect successful cancellations and failures
                for (task_id, result) in task_ids.into_iter().zip(results) {
                    match result {
                        Ok(task) => cancelled.push(PyTask::new(task, None)),
                        Err(e) => failed.push((task_id.to_string(), e.to_string())),
                    }
                }
            }

            Ok(PyCancelByTypeResult { cancelled, failed })
        })
    }

    /// Request cooperative cancellation of a running task.
    ///
    /// Sets the `cancel_requested` flag on the task. The worker can check this
    /// via `context.is_cancellation_requested()` and gracefully shut down.
    ///
    /// Only works on tasks in `Running` status.
    ///
    /// # Arguments
    ///
    /// * `task_id` - The UUID of the running task as a string
    ///
    /// # Returns
    ///
    /// The updated Task object with `cancel_requested=True`.
    ///
    /// # Raises
    ///
    /// * `RuntimeError` - If the task doesn't exist or is not Running.
    ///
    /// # Example
    ///
    /// ```python
    /// # Request cancellation of a running task
    /// task = await queue.request_cancellation("550e8400-e29b-41d4-a716-446655440000")
    /// assert task.cancel_requested is True
    /// ```
    fn request_cancellation<'py>(
        &self,
        py: Python<'py>,
        task_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let queue = self.inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let uuid = uuid::Uuid::parse_str(&task_id).map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid UUID: {e}"))
            })?;

            let task = queue
                .request_cancellation(uuid)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

            Ok(PyTask::new(task, None))
        })
    }
}
