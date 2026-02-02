//! Python bindings for worker operations.
//!
//! This module provides the `PyWorker` class that wraps the Rust `Worker`
//! and allows Python functions to be registered as task handlers.

use pyo3::prelude::*;
use std::collections::HashMap;
use std::ffi::CString;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::models::{TaskError, WorkerInfo};
use crate::queue::Queue;
use crate::storage::PutCondition;
use crate::worker::{
    complete_task, fail_task, reschedule_task, retry_task, shutdown_signal, try_claim_for_handler,
    ClaimResult, MonitorConfig, PollingStrategy, TaskContext, TimeoutMonitor, TransitionError,
};

use super::queue::PyQueue;
use super::task::PyTask;

/// Convert a Python exception to a `TaskError`.
///
/// Inspects the exception type and message to determine the appropriate
/// `TaskError` variant:
/// - `RescheduleError` -> `TaskError::Reschedule` with delay extracted from message
/// - `RetryableError` or `ConnectionError` -> `TaskError::Retryable`
/// - Other exceptions -> `TaskError::Permanent`
#[allow(clippy::needless_pass_by_value)] // PyErr ownership is needed for conversion
fn python_error_to_task_error(err: pyo3::PyErr) -> TaskError {
    let err_str = err.to_string();

    // Check for RescheduleError - format is "RescheduleError: Reschedule after N seconds"
    if err_str.contains("RescheduleError") {
        // Try to extract the delay from the message
        // Format: "Reschedule after {N} seconds"
        if let Some(delay) = extract_reschedule_delay(&err_str) {
            return TaskError::Reschedule(delay);
        }
        // If we can't parse the delay, default to 60 seconds
        return TaskError::Reschedule(60);
    }

    // Check for RetryableError
    if err_str.contains("RetryableError") || err_str.contains("ConnectionError") {
        return TaskError::Retryable(err_str);
    }

    // Default to permanent error
    TaskError::Permanent(err_str)
}

/// Extract the delay in seconds from a `RescheduleError` message.
///
/// Looks for patterns like "Reschedule after 60 seconds" or "after 60 seconds".
fn extract_reschedule_delay(err_str: &str) -> Option<u64> {
    // Look for "after N seconds" pattern
    let after_idx = err_str.find("after ")?;
    let remaining = &err_str[after_idx + 6..];

    // Find the number
    let num_end = remaining.find(|c: char| !c.is_ascii_digit())?;
    let num_str = &remaining[..num_end];

    num_str.parse().ok()
}

/// A task handler registration.
struct PyTaskHandler {
    /// The Python callable (async function).
    callback: Py<PyAny>,
}

/// Lifecycle hooks for worker events.
#[derive(Default)]
#[allow(clippy::struct_field_names)] // "on_" prefix is intentional for API clarity
struct PyWorkerHooks {
    /// Called once when worker starts, before processing any tasks.
    on_startup: Option<Py<PyAny>>,
    /// Called when worker receives shutdown signal.
    on_shutdown: Option<Py<PyAny>>,
    /// Called after each successful task completion.
    on_success: Option<Py<PyAny>>,
    /// Called after task failure (before potential retry).
    on_error: Option<Py<PyAny>>,
}

/// A worker that polls for and executes tasks.
///
/// The worker polls S3 for ready tasks, claims them atomically,
/// and executes them using registered Python handlers.
///
/// # Example
///
/// ```python
/// import asyncio
/// import buquet
///
/// async def main():
///     queue = await buquet.connect()
///     worker = buquet.Worker(queue, "worker-1", ["0", "1", "2", "3"])
///
///     @worker.task("send_email")
///     async def send_email(input):
///         # Process the task
///         return {"status": "sent", "to": input["to"]}
///
///     await worker.run()
///
/// asyncio.run(main())
/// ```
#[pyclass(name = "Worker")]
pub struct PyWorker {
    queue: Arc<Queue>,
    worker_id: String,
    shards: Vec<String>,
    handlers: Arc<RwLock<HashMap<String, PyTaskHandler>>>,
    hooks: Arc<RwLock<PyWorkerHooks>>,
}

/// Polling strategy configuration.
///
/// Controls how the worker polls for tasks:
/// - `fixed`: Constant interval polling (predictable latency, higher cost)
/// - `adaptive`: Exponential backoff when idle (lower cost, variable latency)
///
/// # Example
///
/// ```python
/// # Fixed polling every 500ms
/// strategy = PollingStrategy.fixed(interval_ms=500)
///
/// # Adaptive polling (recommended)
/// strategy = PollingStrategy.adaptive(
///     min_interval_ms=100,
///     max_interval_ms=30000,
///     backoff_multiplier=2.0
/// )
/// ```
#[pyclass(name = "PollingStrategy")]
#[derive(Clone)]
pub struct PyPollingStrategy {
    inner: PollingStrategy,
}

#[pymethods]
#[allow(clippy::missing_const_for_fn)] // PyO3 methods cannot be const
impl PyPollingStrategy {
    /// Creates a fixed polling strategy.
    ///
    /// The worker will poll at a constant interval.
    ///
    /// # Arguments
    ///
    /// * `interval_ms` - Poll interval in milliseconds
    #[staticmethod]
    #[pyo3(signature = (interval_ms=500))]
    fn fixed(interval_ms: u64) -> Self {
        Self {
            inner: PollingStrategy::Fixed { interval_ms },
        }
    }

    /// Creates an adaptive polling strategy.
    ///
    /// The worker backs off exponentially when no tasks are found,
    /// and resets to the minimum interval when tasks are found.
    ///
    /// # Arguments
    ///
    /// * `min_interval_ms` - Minimum interval when tasks are available (default: 100)
    /// * `max_interval_ms` - Maximum interval when idle (default: 5000)
    /// * `backoff_multiplier` - How fast to back off (default: 2.0)
    #[staticmethod]
    #[pyo3(signature = (min_interval_ms=100, max_interval_ms=5000, backoff_multiplier=2.0))]
    fn adaptive(min_interval_ms: u64, max_interval_ms: u64, backoff_multiplier: f64) -> Self {
        Self {
            inner: PollingStrategy::Adaptive {
                min_interval_ms,
                max_interval_ms,
                backoff_multiplier,
            },
        }
    }

    fn __repr__(&self) -> String {
        match &self.inner {
            PollingStrategy::Fixed { interval_ms } => {
                format!("PollingStrategy.fixed(interval_ms={interval_ms})")
            }
            PollingStrategy::Adaptive {
                min_interval_ms,
                max_interval_ms,
                backoff_multiplier,
            } => {
                format!(
                    "PollingStrategy.adaptive(min_interval_ms={min_interval_ms}, max_interval_ms={max_interval_ms}, backoff_multiplier={backoff_multiplier})"
                )
            }
        }
    }
}

#[allow(clippy::missing_const_for_fn)] // PyO3 methods cannot be const
impl PyPollingStrategy {
    /// Returns the inner polling strategy.
    #[must_use]
    pub fn inner(&self) -> &PollingStrategy {
        &self.inner
    }
}

/// Shard lease configuration.
///
/// Controls dynamic shard ownership for workers:
/// - `disabled`: Shard leasing disabled (default)
/// - `enabled`: Shard leasing enabled with default settings
/// - `custom`: Shard leasing with custom settings
///
/// # Example
///
/// ```python
/// # Disable shard leasing (default)
/// config = ShardLeaseConfig.disabled()
///
/// # Enable with defaults
/// config = ShardLeaseConfig.enabled()
///
/// # Custom configuration
/// config = ShardLeaseConfig.custom(
///     shards_per_worker=8,
///     lease_ttl_secs=60,
///     renewal_interval_secs=20
/// )
/// ```
#[pyclass(name = "ShardLeaseConfig")]
#[derive(Clone)]
pub struct PyShardLeaseConfig {
    inner: crate::worker::ShardLeaseConfig,
}

#[pymethods]
#[allow(clippy::missing_const_for_fn)] // PyO3 methods cannot be const
impl PyShardLeaseConfig {
    /// Creates a disabled shard lease configuration.
    ///
    /// This is the default. Workers will poll their statically assigned shards.
    #[staticmethod]
    fn disabled() -> Self {
        Self {
            inner: crate::worker::ShardLeaseConfig::default(),
        }
    }

    /// Creates an enabled shard lease configuration with default settings.
    ///
    /// Default settings:
    /// - `shards_per_worker`: 16
    /// - `lease_ttl_secs`: 30
    /// - `renewal_interval_secs`: 10
    #[staticmethod]
    fn enabled() -> Self {
        Self {
            inner: crate::worker::ShardLeaseConfig::enabled(),
        }
    }

    /// Creates a custom shard lease configuration.
    ///
    /// # Arguments
    ///
    /// * `shards_per_worker` - Number of shards each worker should try to lease (default: 16)
    /// * `lease_ttl_secs` - Lease duration in seconds (default: 30)
    /// * `renewal_interval_secs` - How often to renew leases in seconds (default: 10)
    #[staticmethod]
    #[pyo3(signature = (shards_per_worker=16, lease_ttl_secs=30, renewal_interval_secs=10))]
    fn custom(shards_per_worker: usize, lease_ttl_secs: u64, renewal_interval_secs: u64) -> Self {
        Self {
            inner: crate::worker::ShardLeaseConfig {
                enabled: true,
                shards_per_worker,
                lease_ttl: std::time::Duration::from_secs(lease_ttl_secs),
                renewal_interval: std::time::Duration::from_secs(renewal_interval_secs),
            },
        }
    }

    /// Whether shard leasing is enabled.
    #[getter]
    fn is_enabled(&self) -> bool {
        self.inner.enabled
    }

    /// Number of shards each worker should try to lease.
    #[getter]
    fn shards_per_worker(&self) -> usize {
        self.inner.shards_per_worker
    }

    /// Lease duration in seconds.
    #[getter]
    fn lease_ttl_secs(&self) -> u64 {
        self.inner.lease_ttl.as_secs()
    }

    /// How often to renew leases in seconds.
    #[getter]
    fn renewal_interval_secs(&self) -> u64 {
        self.inner.renewal_interval.as_secs()
    }

    fn __repr__(&self) -> String {
        if !self.inner.enabled {
            "ShardLeaseConfig.disabled()".to_string()
        } else if self.inner.shards_per_worker == 16
            && self.inner.lease_ttl.as_secs() == 30
            && self.inner.renewal_interval.as_secs() == 10
        {
            "ShardLeaseConfig.enabled()".to_string()
        } else {
            let shards_per_worker = self.inner.shards_per_worker;
            let lease_ttl_secs = self.inner.lease_ttl.as_secs();
            let renewal_interval_secs = self.inner.renewal_interval.as_secs();
            format!(
                "ShardLeaseConfig.custom(shards_per_worker={shards_per_worker}, lease_ttl_secs={lease_ttl_secs}, renewal_interval_secs={renewal_interval_secs})"
            )
        }
    }
}

#[allow(clippy::missing_const_for_fn)] // PyO3 methods cannot be const
impl PyShardLeaseConfig {
    /// Returns the inner shard lease configuration.
    #[must_use]
    pub fn inner(&self) -> &crate::worker::ShardLeaseConfig {
        &self.inner
    }
}

/// Options for running a worker loop.
#[pyclass(name = "WorkerRunOptions")]
#[derive(Clone)]
pub struct PyWorkerRunOptions {
    /// Polling strategy (fixed or adaptive).
    #[pyo3(get, set)]
    pub polling: Option<PyPollingStrategy>,
    /// Polling interval in milliseconds (deprecated, use `polling` instead).
    #[pyo3(get, set)]
    pub poll_interval_ms: u64,
    /// Maximum tasks to process before stopping (None for unlimited).
    #[pyo3(get, set)]
    pub max_tasks: Option<usize>,
    /// Heartbeat interval in seconds.
    #[pyo3(get, set)]
    pub heartbeat_interval_s: u64,
    /// Whether to run the timeout monitor alongside the worker.
    #[pyo3(get, set)]
    pub with_monitor: bool,
    /// Monitor check interval in seconds.
    #[pyo3(get, set)]
    pub monitor_check_interval_s: u64,
    /// Worker health threshold in seconds.
    #[pyo3(get, set)]
    pub monitor_worker_health_threshold_s: i64,
    /// Monitor sweep interval in seconds.
    #[pyo3(get, set)]
    pub monitor_sweep_interval_s: u64,
    /// Monitor sweep page size.
    #[pyo3(get, set)]
    pub monitor_sweep_page_size: i32,
    /// Shard lease configuration (None uses legacy `shard_leasing_*` fields).
    #[pyo3(get, set)]
    pub shard_lease_config: Option<PyShardLeaseConfig>,
    /// Enable shard leasing for dynamic shard ownership (deprecated, use `shard_lease_config`).
    #[pyo3(get, set)]
    pub shard_leasing_enabled: bool,
    /// Number of shards per worker when shard leasing is enabled (deprecated, use `shard_lease_config`).
    #[pyo3(get, set)]
    pub shard_leasing_shards_per_worker: usize,
    /// Shard lease TTL in seconds (deprecated, use `shard_lease_config`).
    #[pyo3(get, set)]
    pub shard_leasing_ttl_s: u64,
    /// Shard lease renewal interval in seconds (deprecated, use `shard_lease_config`).
    #[pyo3(get, set)]
    pub shard_leasing_renewal_interval_s: u64,
}

impl Default for PyWorkerRunOptions {
    fn default() -> Self {
        Self {
            polling: None, // Will use adaptive by default
            poll_interval_ms: 1000,
            max_tasks: None,
            heartbeat_interval_s: 10,
            with_monitor: true,
            monitor_check_interval_s: 30,
            monitor_worker_health_threshold_s: 60,
            monitor_sweep_interval_s: 300,
            monitor_sweep_page_size: 1000,
            shard_lease_config: None,
            shard_leasing_enabled: false,
            shard_leasing_shards_per_worker: 16,
            shard_leasing_ttl_s: 30,
            shard_leasing_renewal_interval_s: 10,
        }
    }
}

#[pymethods]
#[allow(clippy::missing_const_for_fn)] // PyO3 methods cannot be const
impl PyWorkerRunOptions {
    /// Creates a new set of worker run options.
    #[new]
    #[pyo3(signature = (
        polling=None,
        poll_interval_ms=1000,
        max_tasks=None,
        heartbeat_interval_s=10,
        with_monitor=true,
        monitor_check_interval_s=30,
        monitor_worker_health_threshold_s=60,
        monitor_sweep_interval_s=300,
        monitor_sweep_page_size=1000,
        shard_lease_config=None,
        shard_leasing_enabled=false,
        shard_leasing_shards_per_worker=16,
        shard_leasing_ttl_s=30,
        shard_leasing_renewal_interval_s=10
    ))]
    #[allow(clippy::too_many_arguments)] // kwargs with defaults is idiomatic Python
    fn new(
        polling: Option<PyPollingStrategy>,
        poll_interval_ms: u64,
        max_tasks: Option<usize>,
        heartbeat_interval_s: u64,
        with_monitor: bool,
        monitor_check_interval_s: u64,
        monitor_worker_health_threshold_s: i64,
        monitor_sweep_interval_s: u64,
        monitor_sweep_page_size: i32,
        shard_lease_config: Option<PyShardLeaseConfig>,
        shard_leasing_enabled: bool,
        shard_leasing_shards_per_worker: usize,
        shard_leasing_ttl_s: u64,
        shard_leasing_renewal_interval_s: u64,
    ) -> Self {
        Self {
            polling,
            poll_interval_ms,
            max_tasks,
            heartbeat_interval_s,
            with_monitor,
            monitor_check_interval_s,
            monitor_worker_health_threshold_s,
            monitor_sweep_interval_s,
            monitor_sweep_page_size,
            shard_lease_config,
            shard_leasing_enabled,
            shard_leasing_shards_per_worker,
            shard_leasing_ttl_s,
            shard_leasing_renewal_interval_s,
        }
    }

    fn __repr__(&self) -> String {
        let polling_str = self
            .polling
            .as_ref()
            .map_or_else(|| "None".to_string(), PyPollingStrategy::__repr__);
        let shard_lease_config_str = self
            .shard_lease_config
            .as_ref()
            .map_or_else(|| "None".to_string(), PyShardLeaseConfig::__repr__);
        format!(
            "WorkerRunOptions(polling={}, poll_interval_ms={}, max_tasks={:?}, heartbeat_interval_s={}, with_monitor={}, monitor_check_interval_s={}, monitor_worker_health_threshold_s={}, monitor_sweep_interval_s={}, monitor_sweep_page_size={}, shard_lease_config={}, shard_leasing_enabled={}, shard_leasing_shards_per_worker={}, shard_leasing_ttl_s={}, shard_leasing_renewal_interval_s={})",
            polling_str,
            self.poll_interval_ms,
            self.max_tasks,
            self.heartbeat_interval_s,
            self.with_monitor,
            self.monitor_check_interval_s,
            self.monitor_worker_health_threshold_s,
            self.monitor_sweep_interval_s,
            self.monitor_sweep_page_size,
            shard_lease_config_str,
            self.shard_leasing_enabled,
            self.shard_leasing_shards_per_worker,
            self.shard_leasing_ttl_s,
            self.shard_leasing_renewal_interval_s
        )
    }
}

/// Context for a running task that allows lease extension from Python.
///
/// This object is passed to task handlers (as an optional second argument)
/// to allow them to extend their lease for long-running operations.
///
/// # Example
///
/// ```python
/// @worker.task("long_running")
/// async def long_running(input, ctx):
///     for chunk in chunks:
///         await process_chunk(chunk)
///         # Extend lease by 60 seconds
///         await ctx.extend_lease(60)
///     return {"status": "done"}
/// ```
#[pyclass(name = "TaskContext")]
#[derive(Clone)]
pub struct PyTaskContext {
    inner: Arc<TaskContext>,
}

impl PyTaskContext {
    /// Creates a new `PyTaskContext` wrapping a `TaskContext`.
    #[must_use]
    pub fn new(ctx: TaskContext) -> Self {
        Self {
            inner: Arc::new(ctx),
        }
    }
}

#[pymethods]
impl PyTaskContext {
    /// Returns the task ID.
    #[getter]
    fn task_id(&self) -> String {
        self.inner.task_id().to_string()
    }

    /// Extends the lease on the current task.
    ///
    /// Call this periodically during long-running task execution to prevent
    /// the timeout monitor from reclaiming the task.
    ///
    /// # Arguments
    ///
    /// * `additional_secs` - How many seconds to add to the lease
    ///
    /// # Raises
    ///
    /// `RuntimeError` if the lease extension fails, which indicates the task
    /// may have been taken by another worker. In this case, the handler should
    /// stop processing immediately to avoid duplicate work.
    ///
    /// # Example
    ///
    /// ```python
    /// @worker.task("long_running")
    /// async def long_running(input, ctx):
    ///     for chunk in chunks:
    ///         await process_chunk(chunk)
    ///         try:
    ///             await ctx.extend_lease(60)  # Add 60 seconds
    ///         except RuntimeError as e:
    ///             print(f"Lease extension failed: {e}")
    ///             return  # Stop processing, another worker has the task
    ///     return {"status": "done"}
    /// ```
    fn extend_lease<'py>(
        &self,
        py: Python<'py>,
        additional_secs: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ctx = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            ctx.extend_lease_secs(additional_secs)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            Ok(())
        })
    }

    /// Returns the current lease expiration time as an ISO 8601 string.
    fn lease_expires_at<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let ctx = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let task = ctx.task().await;
            Ok(task.lease_expires_at.map(|dt| dt.to_rfc3339()))
        })
    }

    /// Refreshes the task state from S3.
    ///
    /// Call this to get the latest task state, including any cancellation
    /// requests that may have been made since the task was claimed.
    ///
    /// # Example
    ///
    /// ```python
    /// @worker.task("long_running")
    /// async def long_running(input, ctx):
    ///     for chunk in chunks:
    ///         await process_chunk(chunk)
    ///         # Check for cancellation with fresh state
    ///         await ctx.refresh()
    ///         if await ctx.is_cancellation_requested():
    ///             return {"status": "cancelled"}
    ///     return {"status": "done"}
    /// ```
    fn refresh<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let ctx = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            ctx.refresh()
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            Ok(())
        })
    }

    /// Check if cancellation has been requested for this task.
    ///
    /// This reads from the cached task state. For real-time cancellation
    /// detection, call `refresh()` first to fetch the latest state from S3.
    ///
    /// # Example
    ///
    /// ```python
    /// # Check cached state (fast, may be stale)
    /// if await ctx.is_cancellation_requested():
    ///     return {"status": "cancelled"}
    ///
    /// # Or refresh first for latest state
    /// await ctx.refresh()
    /// if await ctx.is_cancellation_requested():
    ///     return {"status": "cancelled"}
    /// ```
    fn is_cancellation_requested<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let ctx = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            Ok(ctx.is_cancellation_requested().await)
        })
    }

    fn __repr__(&self) -> String {
        let task_id = self.inner.task_id();
        format!("TaskContext(task_id='{task_id}')")
    }
}

#[pymethods]
#[allow(clippy::elidable_lifetime_names)] // PyO3 requires explicit lifetimes
#[allow(clippy::needless_pass_by_value)] // PyO3 requires owned values
#[allow(clippy::unnecessary_wraps)] // PyO3 requires PyResult return types
impl PyWorker {
    /// Creates a new worker.
    ///
    /// # Arguments
    ///
    /// * `queue` - The queue to poll for tasks
    /// * `worker_id` - A unique identifier for this worker
    /// * `shards` - The shards this worker will poll (e.g., `["0", "1", "a", "b"]`)
    #[new]
    fn new(queue: &PyQueue, worker_id: String, shards: Vec<String>) -> Self {
        Self {
            queue: Arc::new((*queue.inner()).clone()),
            worker_id,
            shards,
            handlers: Arc::new(RwLock::new(HashMap::new())),
            hooks: Arc::new(RwLock::new(PyWorkerHooks::default())),
        }
    }

    /// Decorator to register a task handler.
    ///
    /// # Arguments
    ///
    /// * `task_type` - The type of task this handler processes
    ///
    /// # Returns
    ///
    /// A decorator function that registers the handler.
    ///
    /// # Example
    ///
    /// ```python
    /// @worker.task("process_image")
    /// async def process_image(input):
    ///     # input is a dict with the task's input data
    ///     url = input["url"]
    ///     # ... process the image ...
    ///     return {"processed": True, "size": 1024}
    /// ```
    fn task<'py>(slf: Py<Self>, py: Python<'py>, task_type: String) -> PyResult<Bound<'py, PyAny>> {
        // Create a decorator function that captures the task_type and worker
        let code = CString::new(
            r"
def make_decorator(worker, task_type):
    def decorator(func):
        worker._register_handler(task_type, func)
        return func
    return decorator
",
        )
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

        let filename = CString::new("decorator.py")
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
        let module_name = CString::new("decorator")
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

        let decorator = PyModule::from_code(py, &code, &filename, &module_name)?;

        let make_decorator = decorator.getattr("make_decorator")?;
        make_decorator.call1((slf, task_type))
    }

    /// Internal method to register a handler.
    fn _register_handler(
        &self,
        _py: Python<'_>,
        task_type: String,
        callback: Py<PyAny>,
    ) -> PyResult<()> {
        let handlers = self.handlers.clone();

        // Use a sync block to register since we're called from Python
        // This is safe because we're just inserting into the HashMap
        pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            let mut handlers = handlers.write().await;
            handlers.insert(task_type, PyTaskHandler { callback });
        });

        Ok(())
    }

    /// Decorator to register a startup hook.
    ///
    /// The startup hook is called once when the worker starts, before processing any tasks.
    ///
    /// # Example
    ///
    /// ```python
    /// @worker.on_startup
    /// async def startup():
    ///     print("Worker starting up!")
    /// ```
    fn on_startup<'py>(slf: Py<Self>, py: Python<'py>, callback: Py<PyAny>) -> PyResult<Py<PyAny>> {
        let hooks = slf.borrow(py).hooks.clone();
        let callback_clone = callback.clone_ref(py);
        pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            let mut hooks = hooks.write().await;
            hooks.on_startup = Some(callback_clone);
        });
        Ok(callback)
    }

    /// Decorator to register a shutdown hook.
    ///
    /// The shutdown hook is called when the worker receives a shutdown signal.
    ///
    /// # Example
    ///
    /// ```python
    /// @worker.on_shutdown
    /// async def shutdown():
    ///     print("Worker shutting down!")
    /// ```
    fn on_shutdown<'py>(
        slf: Py<Self>,
        py: Python<'py>,
        callback: Py<PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let hooks = slf.borrow(py).hooks.clone();
        let callback_clone = callback.clone_ref(py);
        pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            let mut hooks = hooks.write().await;
            hooks.on_shutdown = Some(callback_clone);
        });
        Ok(callback)
    }

    /// Decorator to register a success hook.
    ///
    /// The success hook is called after each successful task completion.
    ///
    /// # Arguments
    ///
    /// The callback receives the completed task as its argument.
    ///
    /// # Example
    ///
    /// ```python
    /// @worker.on_success
    /// async def on_success(task):
    ///     print(f"Task {task.id} completed successfully!")
    /// ```
    fn on_success<'py>(slf: Py<Self>, py: Python<'py>, callback: Py<PyAny>) -> PyResult<Py<PyAny>> {
        let hooks = slf.borrow(py).hooks.clone();
        let callback_clone = callback.clone_ref(py);
        pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            let mut hooks = hooks.write().await;
            hooks.on_success = Some(callback_clone);
        });
        Ok(callback)
    }

    /// Decorator to register an error hook.
    ///
    /// The error hook is called after task failure (before potential retry).
    ///
    /// # Arguments
    ///
    /// The callback receives the failed task and error message as arguments.
    ///
    /// # Example
    ///
    /// ```python
    /// @worker.on_error
    /// async def on_error(task, error):
    ///     print(f"Task {task.id} failed with error: {error}")
    /// ```
    fn on_error<'py>(slf: Py<Self>, py: Python<'py>, callback: Py<PyAny>) -> PyResult<Py<PyAny>> {
        let hooks = slf.borrow(py).hooks.clone();
        let callback_clone = callback.clone_ref(py);
        pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            let mut hooks = hooks.write().await;
            hooks.on_error = Some(callback_clone);
        });
        Ok(callback)
    }

    /// Run the worker loop.
    ///
    /// This method polls for tasks and executes them using registered handlers.
    /// It runs indefinitely until cancelled.
    ///
    /// # Arguments
    ///
    /// * `options` - Optional `WorkerRunOptions` (defaults applied if None)
    ///
    /// # Example
    ///
    /// ```python
    /// # Run indefinitely
    /// await worker.run()
    ///
    /// # Run with custom poll interval
    /// await worker.run(poll_interval_ms=500)
    ///
    /// # Process exactly 10 tasks then stop
    /// await worker.run(max_tasks=10)
    /// ```
    #[pyo3(signature = (options=None))]
    #[allow(clippy::too_many_lines)] // Worker run loop has many branches
    fn run<'py>(
        &self,
        py: Python<'py>,
        options: Option<PyWorkerRunOptions>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let options = options.unwrap_or_default();
        let queue = self.queue.clone();
        let worker_id = self.worker_id.clone();
        let shards = self.shards.clone();
        let handlers = self.handlers.clone();
        let hooks = self.hooks.clone();

        #[allow(clippy::large_stack_frames)] // Async state machine requires large stack
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            // Create and register worker info
            let now = queue.now().await.unwrap_or_else(|_| chrono::Utc::now());
            let mut worker_info = WorkerInfo {
                worker_id: worker_id.clone(),
                started_at: now,
                last_heartbeat: now,
                shards: shards.clone(),
                current_task: None,
                tasks_completed: 0,
                tasks_failed: 0,
            };

            // Register worker in S3
            let worker_key = worker_info.key();
            if let Ok(body) = serde_json::to_vec(&worker_info) {
                if let Err(e) = queue
                    .client()
                    .put_object(&worker_key, body, PutCondition::None)
                    .await
                {
                    tracing::warn!("Failed to register worker: {}", e);
                } else {
                    tracing::info!(worker_id = %worker_id, "Worker registered");
                }
            }

            // Call startup hook before processing any tasks
            {
                let hooks = hooks.read().await;
                if let Some(ref callback) = hooks.on_startup {
                    if let Err(e) = call_hook(callback).await {
                        tracing::warn!("Startup hook failed: {}", e);
                    }
                }
            }

            let (monitor_shutdown_tx, monitor_shutdown_rx) = shutdown_signal();
            let monitor_handle = if options.with_monitor {
                let mut monitor = TimeoutMonitor::new(
                    (*queue).clone(),
                    MonitorConfig {
                        check_interval: std::time::Duration::from_secs(
                            options.monitor_check_interval_s,
                        ),
                        worker_health_threshold: chrono::Duration::seconds(
                            options.monitor_worker_health_threshold_s,
                        ),
                        sweep_interval: if options.monitor_sweep_interval_s == 0 {
                            None
                        } else {
                            Some(std::time::Duration::from_secs(
                                options.monitor_sweep_interval_s,
                            ))
                        },
                        sweep_page_size: options.monitor_sweep_page_size,
                        max_tasks_per_scan: 100,
                    },
                );
                Some(tokio::spawn(async move {
                    if let Err(e) = monitor.run(monitor_shutdown_rx).await {
                        tracing::warn!("Python worker monitor exited with error: {}", e);
                    }
                }))
            } else {
                tracing::warn!(
                    "Embedded monitor disabled. Ensure `buquet monitor` is running to prevent task stalls."
                );
                None
            };

            let mut tasks_processed: usize = 0;
            // Use the polling strategy if provided, otherwise fall back to legacy poll_interval_ms
            let polling_strategy = options.polling.as_ref().map_or_else(
                || PollingStrategy::Fixed {
                    interval_ms: options.poll_interval_ms,
                },
                |p| p.inner().clone(),
            );
            let mut current_poll_interval = polling_strategy.initial_interval();
            let heartbeat_interval = std::time::Duration::from_secs(options.heartbeat_interval_s);
            let mut last_heartbeat = std::time::Instant::now();

            loop {
                // Check if we've hit the max tasks limit
                if let Some(max) = options.max_tasks {
                    if tasks_processed >= max {
                        break;
                    }
                }

                // Update heartbeat if needed
                if last_heartbeat.elapsed() >= heartbeat_interval {
                    if let Ok(now) = queue.now().await {
                        worker_info.last_heartbeat = now;
                        if let Ok(body) = serde_json::to_vec(&worker_info) {
                            if let Err(e) = queue
                                .client()
                                .put_object(&worker_key, body, PutCondition::None)
                                .await
                            {
                                tracing::warn!("Failed to update heartbeat: {}", e);
                            }
                        }
                    }
                    last_heartbeat = std::time::Instant::now();
                }

                // Poll each shard for ready tasks
                let mut found_task = false;

                // Snapshot registered handler types once per poll cycle.
                // This allows us to use the shared try_claim_for_handler function
                // which requires a sync closure for the handler check.
                let registered_types: std::collections::HashSet<String> = {
                    let handlers_guard = handlers.read().await;
                    handlers_guard.keys().cloned().collect()
                };

                for shard in &shards {
                    // List ready tasks in this shard
                    let ready_tasks = match queue.list_ready(shard, 10).await {
                        Ok(tasks) => tasks,
                        Err(e) => {
                            // Log error and continue to next shard
                            tracing::warn!("Error listing ready tasks in shard {}: {}", shard, e);
                            continue;
                        }
                    };

                    // Try to claim each ready task using the shared Rust function
                    // that checks for handler before claiming
                    for task_id in ready_tasks {
                        // Use the canonical try_claim_for_handler function from Rust core.
                        // This is the single source of truth for the "check handler before claim" logic.
                        match try_claim_for_handler(&queue, task_id, &worker_id, |task_type| {
                            registered_types.contains(task_type)
                        })
                        .await
                        {
                            Ok(ClaimResult::Claimed { task, etag }) => {
                                found_task = true;
                                let lease_id = task.lease_id;
                                let etag_for_reschedule = etag.clone();

                                // Update current task in worker info
                                worker_info.current_task = Some(task.id);

                                // Create TaskContext for lease extension
                                let rust_ctx =
                                    TaskContext::new((*queue).clone(), (*task).clone(), etag);
                                let py_ctx = PyTaskContext::new(rust_ctx);

                                // Execute the task with context
                                let result = execute_task_with_handler_and_context(
                                    &handlers,
                                    &task,
                                    Some(py_ctx),
                                )
                                .await;

                                // Clear current task
                                worker_info.current_task = None;

                                // Handle the result
                                match result {
                                    Ok(output) => {
                                        // Task completed successfully
                                        worker_info.tasks_completed += 1;
                                        if let Some(lease_id) = lease_id {
                                            if let Err(e) =
                                                complete_task(&queue, task.id, lease_id, output)
                                                    .await
                                            {
                                                tracing::error!(
                                                    "Failed to complete task {}: {}",
                                                    task.id,
                                                    e
                                                );
                                            }
                                        }
                                        // Call success hook
                                        let hooks = hooks.read().await;
                                        if let Some(ref callback) = hooks.on_success {
                                            if let Err(e) = call_success_hook(callback, &task).await
                                            {
                                                tracing::warn!(
                                                    "Success hook failed for task {}: {}",
                                                    task.id,
                                                    e
                                                );
                                            }
                                        }
                                    }
                                    Err(TaskError::Retryable(ref msg)) => {
                                        // Call error hook before retry decision
                                        {
                                            let hooks = hooks.read().await;
                                            if let Some(ref callback) = hooks.on_error {
                                                if let Err(e) =
                                                    call_error_hook(callback, &task, msg).await
                                                {
                                                    tracing::warn!(
                                                        "Error hook failed for task {}: {}",
                                                        task.id,
                                                        e
                                                    );
                                                }
                                            }
                                        }
                                        // Retryable error
                                        if let Some(lease_id) = lease_id {
                                            if task.can_retry() {
                                                if let Err(e) =
                                                    retry_task(&queue, task.id, lease_id, msg).await
                                                {
                                                    tracing::error!(
                                                        "Failed to retry task {}: {}",
                                                        task.id,
                                                        e
                                                    );
                                                }
                                            } else {
                                                // Exhausted retries, fail the task
                                                worker_info.tasks_failed += 1;
                                                if let Err(e) =
                                                    fail_task(&queue, task.id, lease_id, msg).await
                                                {
                                                    tracing::error!(
                                                        "Failed to fail task {}: {}",
                                                        task.id,
                                                        e
                                                    );
                                                }
                                            }
                                        }
                                    }
                                    Err(TaskError::Permanent(ref msg)) => {
                                        // Call error hook
                                        {
                                            let hooks = hooks.read().await;
                                            if let Some(ref callback) = hooks.on_error {
                                                if let Err(e) =
                                                    call_error_hook(callback, &task, msg).await
                                                {
                                                    tracing::warn!(
                                                        "Error hook failed for task {}: {}",
                                                        task.id,
                                                        e
                                                    );
                                                }
                                            }
                                        }
                                        // Permanent error - fail immediately
                                        worker_info.tasks_failed += 1;
                                        if let Some(lease_id) = lease_id {
                                            if let Err(e) =
                                                fail_task(&queue, task.id, lease_id, msg).await
                                            {
                                                tracing::error!(
                                                    "Failed to fail task {}: {}",
                                                    task.id,
                                                    e
                                                );
                                            }
                                        }
                                    }
                                    Err(TaskError::Timeout) => {
                                        // Call error hook
                                        {
                                            let hooks = hooks.read().await;
                                            if let Some(ref callback) = hooks.on_error {
                                                if let Err(e) = call_error_hook(
                                                    callback,
                                                    &task,
                                                    "Task timed out",
                                                )
                                                .await
                                                {
                                                    tracing::warn!(
                                                        "Error hook failed for task {}: {}",
                                                        task.id,
                                                        e
                                                    );
                                                }
                                            }
                                        }
                                        // Timeout error - fail immediately
                                        worker_info.tasks_failed += 1;
                                        if let Some(lease_id) = lease_id {
                                            if let Err(e) = fail_task(
                                                &queue,
                                                task.id,
                                                lease_id,
                                                "Task timed out",
                                            )
                                            .await
                                            {
                                                tracing::error!(
                                                    "Failed to fail task {}: {}",
                                                    task.id,
                                                    e
                                                );
                                            }
                                        }
                                    }
                                    Err(TaskError::Reschedule(delay_seconds)) => {
                                        // Reschedule - task wants to run later
                                        if let Some(lease_id) = lease_id {
                                            match reschedule_task(
                                                &queue,
                                                &task,
                                                &etag_for_reschedule,
                                                lease_id,
                                                delay_seconds,
                                            )
                                            .await
                                            {
                                                Ok(updated) => {
                                                    tracing::info!(
                                                        "Task {} rescheduled for {} seconds, count: {}",
                                                        task.id,
                                                        delay_seconds,
                                                        updated.reschedule_count
                                                    );
                                                }
                                                Err(TransitionError::MaxReschedulesExceeded(
                                                    max,
                                                )) => {
                                                    // Max reschedules exceeded - fail the task
                                                    worker_info.tasks_failed += 1;
                                                    let error_msg =
                                                        format!("Max reschedules ({max}) exceeded");
                                                    if let Err(e) = fail_task(
                                                        &queue, task.id, lease_id, &error_msg,
                                                    )
                                                    .await
                                                    {
                                                        tracing::error!(
                                                            "Failed to fail task {} after max reschedules: {}",
                                                            task.id,
                                                            e
                                                        );
                                                    }
                                                }
                                                Err(e) => {
                                                    tracing::error!(
                                                        "Failed to reschedule task {}: {}",
                                                        task.id,
                                                        e
                                                    );
                                                }
                                            }
                                        }
                                    }
                                }

                                tasks_processed += 1;

                                // Check max_tasks after each task
                                if let Some(max) = options.max_tasks {
                                    if tasks_processed >= max {
                                        // Call shutdown hook before returning
                                        {
                                            let hooks = hooks.read().await;
                                            if let Some(ref callback) = hooks.on_shutdown {
                                                if let Err(e) = call_hook(callback).await {
                                                    tracing::warn!("Shutdown hook failed: {}", e);
                                                }
                                            }
                                        }
                                        // Deregister worker
                                        if let Err(e) =
                                            queue.client().delete_object(&worker_key).await
                                        {
                                            tracing::warn!("Failed to deregister worker: {}", e);
                                        }
                                        if options.with_monitor {
                                            let _ = monitor_shutdown_tx.send(true);
                                            if let Some(handle) = monitor_handle {
                                                let _ = handle.await;
                                            }
                                        }
                                        return Ok(tasks_processed);
                                    }
                                }
                            }
                            Ok(
                                ClaimResult::AlreadyClaimed
                                | ClaimResult::NotAvailable
                                | ClaimResult::NotFound,
                            ) => {
                                // Task claimed by another worker or not available, try next
                            }
                            Ok(ClaimResult::Expired) => {
                                // Task expired, mark it and try next
                                let _ = queue.mark_expired(task_id).await;
                            }
                            Err(e) => {
                                tracing::warn!("Error claiming task {}: {}", task_id, e);
                            }
                        }
                    }
                }

                // Update polling interval and sleep if no tasks found
                if found_task {
                    // Reset to fast polling
                    current_poll_interval =
                        polling_strategy.next_interval(current_poll_interval, true);
                } else {
                    // Apply jitter and sleep
                    let sleep_interval = polling_strategy.apply_jitter(current_poll_interval);
                    tokio::time::sleep(sleep_interval).await;
                    // Back off (or stay fixed for Fixed strategy)
                    current_poll_interval =
                        polling_strategy.next_interval(current_poll_interval, false);
                }
            }

            // Call shutdown hook before exiting
            {
                let hooks = hooks.read().await;
                if let Some(ref callback) = hooks.on_shutdown {
                    if let Err(e) = call_hook(callback).await {
                        tracing::warn!("Shutdown hook failed: {}", e);
                    }
                }
            }

            // Deregister worker from S3
            if let Err(e) = queue.client().delete_object(&worker_key).await {
                tracing::warn!("Failed to deregister worker: {}", e);
            } else {
                tracing::info!(worker_id = %worker_info.worker_id, "Worker deregistered");
            }

            if options.with_monitor {
                let _ = monitor_shutdown_tx.send(true);
                if let Some(handle) = monitor_handle {
                    let _ = handle.await;
                }
            }

            Ok(tasks_processed)
        })
    }

    /// Returns the worker ID.
    #[getter]
    fn worker_id(&self) -> &str {
        &self.worker_id
    }

    /// Returns the shards this worker polls.
    #[getter]
    fn shards(&self) -> Vec<String> {
        self.shards.clone()
    }

    /// Returns the list of registered task types.
    fn registered_task_types(&self, _py: Python<'_>) -> PyResult<Vec<String>> {
        let handlers = self.handlers.clone();

        pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            let handlers = handlers.read().await;
            Ok(handlers.keys().cloned().collect())
        })
    }

    fn __repr__(&self) -> String {
        let worker_id = &self.worker_id;
        let shards = &self.shards;
        format!("Worker(id='{worker_id}', shards={shards:?})")
    }
}

/// Execute a task using the registered Python handler.
#[allow(dead_code)]
async fn execute_task_with_handler(
    handlers: &Arc<RwLock<HashMap<String, PyTaskHandler>>>,
    task: &crate::models::Task,
) -> Result<serde_json::Value, TaskError> {
    execute_task_with_handler_and_context(handlers, task, None).await
}

/// Execute a task using the registered Python handler with optional `TaskContext`.
async fn execute_task_with_handler_and_context(
    handlers: &Arc<RwLock<HashMap<String, PyTaskHandler>>>,
    task: &crate::models::Task,
    ctx: Option<PyTaskContext>,
) -> Result<serde_json::Value, TaskError> {
    let task_type = task.task_type.clone();
    let input = task.input.clone();

    // Get the handler
    let handler = {
        let handlers = handlers.read().await;
        handlers
            .get(&task_type)
            .map(|h| Python::attach(|py| h.callback.clone_ref(py)))
    };

    let Some(callback) = handler else {
        return Err(TaskError::Permanent(format!(
            "No handler registered for task type: {task_type}"
        )));
    };

    // Call the Python handler
    let result = Python::attach(|py| {
        // Convert input to Python dict
        let py_input = pythonize::pythonize(py, &input)
            .map_err(|e| TaskError::Permanent(format!("Failed to convert input: {e}")))?;

        // Call the handler with input and optional context
        // Try calling with (input, ctx) first, fall back to (input,) if that fails
        let coro = if let Some(ref ctx) = ctx {
            // Try with context first
            match callback.call1(py, (py_input.clone(), ctx.clone())) {
                Ok(result) => result,
                Err(e) => {
                    // If it failed because of wrong number of arguments, try without context
                    let err_str = e.to_string();
                    if err_str.contains("takes 1 positional argument")
                        || err_str.contains("got 2")
                        || err_str.contains("unexpected keyword argument")
                    {
                        callback
                            .call1(py, (py_input,))
                            .map_err(python_error_to_task_error)?
                    } else {
                        // Different error, propagate it
                        return Err(python_error_to_task_error(e));
                    }
                }
            }
        } else {
            callback
                .call1(py, (py_input,))
                .map_err(python_error_to_task_error)?
        };

        Ok::<Py<PyAny>, TaskError>(coro)
    })?;

    // Check if it's a coroutine and get the future if so
    let (is_coro, future_opt) = Python::attach(|py| {
        let result = result.bind(py);

        // Check if it's a coroutine
        let asyncio = py
            .import("asyncio")
            .map_err(|e| TaskError::Permanent(format!("Failed to import asyncio: {e}")))?;

        let is_coro = asyncio
            .call_method1("iscoroutine", (result,))
            .map_err(|e| TaskError::Permanent(e.to_string()))?
            .is_truthy()
            .map_err(|e| TaskError::Permanent(e.to_string()))?;

        if is_coro {
            // Create a future from the coroutine
            let future = pyo3_async_runtimes::tokio::into_future(result.clone())
                .map_err(|e| TaskError::Permanent(format!("Failed to create future: {e}")))?;
            Ok::<_, TaskError>((true, Some(future)))
        } else {
            // Synchronous result - convert now
            Ok((false, None))
        }
    })?;

    let output: serde_json::Value = if is_coro {
        // Await the Python coroutine (we're already in an async context)
        // SAFETY: future_opt is always Some when is_coro is true (set together above)
        let future = future_opt.ok_or_else(|| {
            TaskError::Permanent("Internal error: coroutine flag set but no future".to_string())
        })?;
        let py_result = future.await.map_err(python_error_to_task_error)?;

        // Convert back to JSON
        Python::attach(|py| {
            let bound = py_result.bind(py);
            pythonize::depythonize(bound)
                .map_err(|e| TaskError::Permanent(format!("Failed to convert output: {e}")))
        })?
    } else {
        // Synchronous result - convert now
        Python::attach(|py| {
            let bound = result.bind(py);
            pythonize::depythonize(bound)
                .map_err(|e| TaskError::Permanent(format!("Failed to convert output: {e}")))
        })?
    };

    Ok(output)
}

/// Call a hook callback, handling both sync and async functions.
async fn call_hook(callback: &Py<PyAny>) -> Result<(), String> {
    // Call the callback
    let result = Python::attach(|py| {
        let coro = callback.call0(py).map_err(|e| e.to_string())?;
        Ok::<Py<PyAny>, String>(coro)
    })?;

    // Check if it's a coroutine and await it if so
    let future_opt = Python::attach(|py| {
        let result = result.bind(py);
        let asyncio = py
            .import("asyncio")
            .map_err(|e| format!("Failed to import asyncio: {e}"))?;

        let is_coro = asyncio
            .call_method1("iscoroutine", (result,))
            .map_err(|e| e.to_string())?
            .is_truthy()
            .map_err(|e| e.to_string())?;

        if is_coro {
            let future = pyo3_async_runtimes::tokio::into_future(result.clone())
                .map_err(|e| format!("Failed to create future: {e}"))?;
            Ok::<Option<_>, String>(Some(future))
        } else {
            Ok(None)
        }
    })?;

    if let Some(future) = future_opt {
        future.await.map_err(|e| e.to_string())?;
    }

    Ok(())
}

/// Call a success hook callback with task argument.
async fn call_success_hook(callback: &Py<PyAny>, task: &crate::models::Task) -> Result<(), String> {
    let py_task = PyTask::new(task.clone(), None);

    // Call the callback with task argument
    let result = Python::attach(|py| {
        let coro = callback.call1(py, (py_task,)).map_err(|e| e.to_string())?;
        Ok::<Py<PyAny>, String>(coro)
    })?;

    // Check if it's a coroutine and await it if so
    let future_opt = Python::attach(|py| {
        let result = result.bind(py);
        let asyncio = py
            .import("asyncio")
            .map_err(|e| format!("Failed to import asyncio: {e}"))?;

        let is_coro = asyncio
            .call_method1("iscoroutine", (result,))
            .map_err(|e| e.to_string())?
            .is_truthy()
            .map_err(|e| e.to_string())?;

        if is_coro {
            let future = pyo3_async_runtimes::tokio::into_future(result.clone())
                .map_err(|e| format!("Failed to create future: {e}"))?;
            Ok::<Option<_>, String>(Some(future))
        } else {
            Ok(None)
        }
    })?;

    if let Some(future) = future_opt {
        future.await.map_err(|e| e.to_string())?;
    }

    Ok(())
}

/// Call an error hook callback with task and error arguments.
async fn call_error_hook(
    callback: &Py<PyAny>,
    task: &crate::models::Task,
    error: &str,
) -> Result<(), String> {
    let py_task = PyTask::new(task.clone(), None);
    let error_msg = error.to_string();

    // Call the callback with task and error arguments
    let result = Python::attach(|py| {
        let coro = callback
            .call1(py, (py_task, error_msg))
            .map_err(|e| e.to_string())?;
        Ok::<Py<PyAny>, String>(coro)
    })?;

    // Check if it's a coroutine and await it if so
    let future_opt = Python::attach(|py| {
        let result = result.bind(py);
        let asyncio = py
            .import("asyncio")
            .map_err(|e| format!("Failed to import asyncio: {e}"))?;

        let is_coro = asyncio
            .call_method1("iscoroutine", (result,))
            .map_err(|e| e.to_string())?
            .is_truthy()
            .map_err(|e| e.to_string())?;

        if is_coro {
            let future = pyo3_async_runtimes::tokio::into_future(result.clone())
                .map_err(|e| format!("Failed to create future: {e}"))?;
            Ok::<Option<_>, String>(Some(future))
        } else {
            Ok(None)
        }
    })?;

    if let Some(future) = future_opt {
        future.await.map_err(|e| e.to_string())?;
    }

    Ok(())
}
