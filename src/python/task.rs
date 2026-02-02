//! Python bindings for task types.
//!
//! This module provides Python wrappers for Task, `TaskStatus`, and `RetryPolicy`.

use pyo3::prelude::*;
use pyo3::types::PyDict;
use serde_json::Value;

use crate::models::{RetryPolicy, Task, TaskStatus};

/// Task status enum exposed to Python.
#[pyclass(name = "TaskStatus", eq, eq_int)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PyTaskStatus {
    /// Task is waiting to be claimed by a worker.
    Pending,
    /// Task has been claimed and is being executed.
    Running,
    /// Task completed successfully.
    Completed,
    /// Task failed after exhausting all retries.
    Failed,
    /// Task was cancelled before completion.
    Cancelled,
    /// Task has been soft-deleted/archived.
    Archived,
    /// Task expired before being claimed (exceeded TTL).
    Expired,
}

#[pymethods]
#[allow(clippy::missing_const_for_fn)]
impl PyTaskStatus {
    /// Returns the string representation of the status.
    #[allow(clippy::trivially_copy_pass_by_ref)]
    fn __str__(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Archived => "archived",
            Self::Expired => "expired",
        }
    }

    /// Returns a debug representation.
    #[allow(clippy::trivially_copy_pass_by_ref)]
    fn __repr__(&self) -> String {
        format!("TaskStatus.{}", self.__str__().to_uppercase())
    }
}

impl From<TaskStatus> for PyTaskStatus {
    fn from(status: TaskStatus) -> Self {
        match status {
            TaskStatus::Pending => Self::Pending,
            TaskStatus::Running => Self::Running,
            TaskStatus::Completed => Self::Completed,
            TaskStatus::Failed => Self::Failed,
            TaskStatus::Cancelled => Self::Cancelled,
            TaskStatus::Archived => Self::Archived,
            TaskStatus::Expired => Self::Expired,
        }
    }
}

impl From<PyTaskStatus> for TaskStatus {
    fn from(status: PyTaskStatus) -> Self {
        match status {
            PyTaskStatus::Pending => Self::Pending,
            PyTaskStatus::Running => Self::Running,
            PyTaskStatus::Completed => Self::Completed,
            PyTaskStatus::Failed => Self::Failed,
            PyTaskStatus::Cancelled => Self::Cancelled,
            PyTaskStatus::Archived => Self::Archived,
            PyTaskStatus::Expired => Self::Expired,
        }
    }
}

/// Retry policy configuration for exponential backoff with jitter.
#[pyclass(name = "RetryPolicy")]
#[derive(Debug, Clone)]
pub struct PyRetryPolicy {
    inner: RetryPolicy,
}

#[pymethods]
#[allow(clippy::missing_const_for_fn)]
impl PyRetryPolicy {
    /// Creates a new retry policy.
    ///
    /// # Arguments
    ///
    /// * `initial_interval_ms` - Initial delay between retries in milliseconds (default: 1000)
    /// * `max_interval_ms` - Maximum delay between retries in milliseconds (default: 60000)
    /// * `multiplier` - Multiplier for exponential backoff (default: 2.0)
    /// * `jitter_percent` - Jitter percentage 0.0-1.0 (default: 0.25)
    #[new]
    #[pyo3(signature = (initial_interval_ms=1000, max_interval_ms=60000, multiplier=2.0, jitter_percent=0.25))]
    fn new(
        initial_interval_ms: u32,
        max_interval_ms: u32,
        multiplier: f64,
        jitter_percent: f64,
    ) -> Self {
        Self {
            inner: RetryPolicy::new(
                initial_interval_ms,
                max_interval_ms,
                multiplier,
                jitter_percent,
            ),
        }
    }

    /// Initial delay between retries in milliseconds.
    #[getter]
    fn initial_interval_ms(&self) -> u32 {
        self.inner.initial_interval_ms
    }

    /// Maximum delay between retries in milliseconds.
    #[getter]
    fn max_interval_ms(&self) -> u32 {
        self.inner.max_interval_ms
    }

    /// Multiplier for exponential backoff.
    #[getter]
    fn multiplier(&self) -> f64 {
        self.inner.multiplier
    }

    /// Jitter percentage (0.0 to 1.0).
    #[getter]
    fn jitter_percent(&self) -> f64 {
        self.inner.jitter_percent
    }

    /// Calculates the delay for a given retry attempt in milliseconds.
    #[allow(clippy::cast_possible_truncation)]
    fn calculate_delay_ms(&self, attempt: u32) -> u64 {
        self.inner.calculate_delay(attempt).as_millis() as u64
    }

    fn __repr__(&self) -> String {
        format!(
            "RetryPolicy(initial_interval_ms={}, max_interval_ms={}, multiplier={}, jitter_percent={})",
            self.inner.initial_interval_ms,
            self.inner.max_interval_ms,
            self.inner.multiplier,
            self.inner.jitter_percent
        )
    }
}

impl From<RetryPolicy> for PyRetryPolicy {
    fn from(policy: RetryPolicy) -> Self {
        Self { inner: policy }
    }
}

impl From<PyRetryPolicy> for RetryPolicy {
    fn from(policy: PyRetryPolicy) -> Self {
        policy.inner
    }
}

impl From<&PyRetryPolicy> for RetryPolicy {
    fn from(policy: &PyRetryPolicy) -> Self {
        policy.inner.clone()
    }
}

/// A task in the distributed task queue.
#[pyclass(name = "Task")]
#[derive(Debug, Clone)]
pub struct PyTask {
    pub(crate) inner: Task,
    #[allow(dead_code)]
    pub(crate) etag: Option<String>,
}

impl PyTask {
    /// Creates a new `PyTask` from a Rust Task.
    #[must_use]
    pub const fn new(task: Task, etag: Option<String>) -> Self {
        Self { inner: task, etag }
    }
}

#[pymethods]
#[allow(clippy::missing_const_for_fn)]
impl PyTask {
    /// Unique identifier for the task.
    #[getter]
    fn id(&self) -> String {
        self.inner.id.to_string()
    }

    /// Shard identifier (first hex character of UUID).
    #[getter]
    fn shard(&self) -> String {
        self.inner.shard.clone()
    }

    /// Type of task (e.g., `send_email`, `process_image`).
    #[getter]
    fn task_type(&self) -> String {
        self.inner.task_type.clone()
    }

    /// Input data for the task as a Python dict.
    #[getter]
    fn input<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        pythonize::pythonize(py, &self.inner.input)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))
    }

    /// Output data from task execution (None if not completed).
    #[getter]
    fn output<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        match &self.inner.output {
            Some(output) => {
                let py_output = pythonize::pythonize(py, output)
                    .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
                Ok(Some(py_output))
            }
            None => Ok(None),
        }
    }

    /// Current status of the task.
    #[getter]
    fn status(&self) -> PyTaskStatus {
        self.inner.status.into()
    }

    /// Maximum time in seconds a task can run before timeout.
    #[getter]
    fn timeout_seconds(&self) -> u64 {
        self.inner.timeout_seconds
    }

    /// Maximum number of retry attempts.
    #[getter]
    fn max_retries(&self) -> u32 {
        self.inner.max_retries
    }

    /// Current retry attempt count.
    #[getter]
    fn retry_count(&self) -> u32 {
        self.inner.retry_count
    }

    /// Retry policy configuration.
    #[getter]
    fn retry_policy(&self) -> PyRetryPolicy {
        self.inner.retry_policy.clone().into()
    }

    /// Timestamp when the task was created (ISO 8601 string).
    #[getter]
    fn created_at(&self) -> String {
        self.inner.created_at.to_rfc3339()
    }

    /// Timestamp when the task becomes available for claiming (ISO 8601 string).
    #[getter]
    fn available_at(&self) -> String {
        self.inner.available_at.to_rfc3339()
    }

    /// Timestamp when the lease expires (ISO 8601 string, None if no lease).
    #[getter]
    fn lease_expires_at(&self) -> Option<String> {
        self.inner.lease_expires_at.map(|dt| dt.to_rfc3339())
    }

    /// Lease ID for current attempt (None if not claimed).
    #[getter]
    fn lease_id(&self) -> Option<String> {
        self.inner.lease_id.map(|id| id.to_string())
    }

    /// Number of times this task has been claimed.
    #[getter]
    fn attempt(&self) -> u32 {
        self.inner.attempt
    }

    /// Last modification timestamp (ISO 8601 string).
    #[getter]
    fn updated_at(&self) -> String {
        self.inner.updated_at.to_rfc3339()
    }

    /// Timestamp when the task completed (ISO 8601 string, None if not completed).
    #[getter]
    fn completed_at(&self) -> Option<String> {
        self.inner.completed_at.map(|dt| dt.to_rfc3339())
    }

    /// ID of the worker currently processing this task.
    #[getter]
    fn worker_id(&self) -> Option<String> {
        self.inner.worker_id.clone()
    }

    /// Error message from the last failed execution attempt.
    #[getter]
    fn last_error(&self) -> Option<String> {
        self.inner.last_error.clone()
    }

    /// S3 key for externally stored input payload, or None if inline.
    #[getter]
    fn input_ref(&self) -> Option<String> {
        self.inner.input_ref.clone()
    }

    /// S3 key for externally stored output payload, or None if inline.
    #[getter]
    fn output_ref(&self) -> Option<String> {
        self.inner.output_ref.clone()
    }

    /// Current reschedule count.
    #[getter]
    fn reschedule_count(&self) -> u32 {
        self.inner.reschedule_count
    }

    /// Maximum number of reschedules allowed (None = unlimited).
    #[getter]
    fn max_reschedules(&self) -> Option<u32> {
        self.inner.max_reschedules
    }

    /// Whether cancellation has been requested for this task.
    #[getter]
    fn cancel_requested(&self) -> bool {
        self.inner.cancel_requested
    }

    /// Timestamp when the task was cancelled (ISO 8601 string, None if not cancelled).
    #[getter]
    fn cancelled_at(&self) -> Option<String> {
        self.inner.cancelled_at.map(|dt| dt.to_rfc3339())
    }

    /// Who/what cancelled the task (e.g., "user:alice", "system").
    #[getter]
    fn cancelled_by(&self) -> Option<String> {
        self.inner.cancelled_by.clone()
    }

    /// When the task expires (ISO 8601 string, None if no expiration).
    #[getter]
    fn expires_at(&self) -> Option<String> {
        self.inner.expires_at.map(|dt| dt.to_rfc3339())
    }

    /// Timestamp when the task was marked as expired (ISO 8601 string, None if not expired).
    #[getter]
    fn expired_at(&self) -> Option<String> {
        self.inner.expired_at.map(|dt| dt.to_rfc3339())
    }

    /// Returns True if the task can be retried.
    fn can_retry(&self) -> bool {
        self.inner.can_retry()
    }

    /// Returns True if the task is available for claiming, using an authoritative time.
    #[allow(clippy::needless_pass_by_value)]
    fn is_available_at(&self, now_rfc3339: String) -> PyResult<bool> {
        let now = parse_datetime(&now_rfc3339)?;
        Ok(self.inner.is_available_at(now))
    }

    /// Returns True if the task's lease has expired, using an authoritative time.
    #[allow(clippy::needless_pass_by_value)]
    fn is_lease_expired_at(&self, now_rfc3339: String) -> PyResult<bool> {
        let now = parse_datetime(&now_rfc3339)?;
        Ok(self.inner.is_lease_expired_at(now))
    }

    /// Returns True if the task has timed out, using an authoritative time.
    #[allow(clippy::needless_pass_by_value)]
    fn is_timed_out_at(&self, now_rfc3339: String) -> PyResult<bool> {
        let now = parse_datetime(&now_rfc3339)?;
        Ok(self.inner.is_timed_out_at(now))
    }

    /// Returns True if the task has expired (exceeded TTL), using an authoritative time.
    ///
    /// # Arguments
    ///
    /// * `now_rfc3339` - Current time as RFC 3339 string (get from `queue.now()`)
    ///
    /// # Example
    ///
    /// ```python
    /// now = await queue.now()
    /// if task.is_expired_at(now):
    ///     print("Task has expired")
    /// ```
    #[allow(clippy::needless_pass_by_value)]
    fn is_expired_at(&self, now_rfc3339: String) -> PyResult<bool> {
        let now = parse_datetime(&now_rfc3339)?;
        Ok(self.inner.is_expired_at(now))
    }

    /// Returns a dict representation of the task.
    fn to_dict<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        dict.set_item("id", self.id())?;
        dict.set_item("shard", self.shard())?;
        dict.set_item("task_type", self.task_type())?;
        dict.set_item("input", self.input(py)?)?;
        dict.set_item("output", self.output(py)?)?;
        dict.set_item("status", self.status().__str__())?;
        dict.set_item("timeout_seconds", self.timeout_seconds())?;
        dict.set_item("max_retries", self.max_retries())?;
        dict.set_item("retry_count", self.retry_count())?;
        dict.set_item("created_at", self.created_at())?;
        dict.set_item("available_at", self.available_at())?;
        dict.set_item("lease_expires_at", self.lease_expires_at())?;
        dict.set_item("lease_id", self.lease_id())?;
        dict.set_item("attempt", self.attempt())?;
        dict.set_item("updated_at", self.updated_at())?;
        dict.set_item("completed_at", self.completed_at())?;
        dict.set_item("worker_id", self.worker_id())?;
        dict.set_item("last_error", self.last_error())?;
        dict.set_item("input_ref", self.input_ref())?;
        dict.set_item("output_ref", self.output_ref())?;
        dict.set_item("reschedule_count", self.reschedule_count())?;
        dict.set_item("max_reschedules", self.max_reschedules())?;
        dict.set_item("cancel_requested", self.cancel_requested())?;
        dict.set_item("cancelled_at", self.cancelled_at())?;
        dict.set_item("cancelled_by", self.cancelled_by())?;
        dict.set_item("expires_at", self.expires_at())?;
        dict.set_item("expired_at", self.expired_at())?;
        Ok(dict)
    }

    fn __repr__(&self) -> String {
        format!(
            "Task(id='{}', type='{}', status={})",
            self.inner.id,
            self.inner.task_type,
            PyTaskStatus::from(self.inner.status).__str__()
        )
    }
}

/// Helper to convert a Python object to `serde_json::Value`
pub fn py_to_json(_py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<Value> {
    pythonize::depythonize(obj)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))
}

fn parse_datetime(value: &str) -> PyResult<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::parse_from_rfc3339(value)
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))
}
