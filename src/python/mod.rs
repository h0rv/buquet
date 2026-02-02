//! Python bindings for the buquet task queue.
//!
//! This module provides `PyO3` bindings to expose the Rust task queue
//! functionality to Python. All async operations are exposed as
//! Python awaitables using pyo3-async-runtimes.

mod config;
mod metrics;
mod queue;
mod schedule;
mod storage;
mod task;
mod worker;

use pyo3::prelude::*;

pub use config::{PyConfig, PyMonitorConfig, PyWorkerConfig};
pub use queue::{PyCancelByTypeResult, PyQueue};
pub use schedule::{PySchedule, PyScheduleLastRun};
pub use storage::PyStorageClient;
pub use task::{PyRetryPolicy, PyTask, PyTaskStatus};
pub use worker::{
    PyPollingStrategy, PyShardLeaseConfig, PyTaskContext, PyWorker, PyWorkerRunOptions,
};

/// The buquet Python module.
///
/// Provides access to the S3-based task queue from Python.
#[pymodule]
#[pyo3(name = "_buquet")]
pub fn buquet(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyTaskStatus>()?;
    m.add_class::<PyRetryPolicy>()?;
    m.add_class::<PyTask>()?;
    m.add_class::<PyQueue>()?;
    m.add_class::<PyWorker>()?;
    m.add_class::<PyWorkerRunOptions>()?;
    m.add_class::<PyPollingStrategy>()?;
    m.add_class::<PyShardLeaseConfig>()?;
    m.add_class::<PyTaskContext>()?;
    m.add_class::<PySchedule>()?;
    m.add_class::<PyScheduleLastRun>()?;
    m.add_class::<PyCancelByTypeResult>()?;
    m.add_class::<PyStorageClient>()?;

    // Add config classes
    m.add_class::<PyConfig>()?;
    m.add_class::<PyWorkerConfig>()?;
    m.add_class::<PyMonitorConfig>()?;

    // Add the connect function at module level
    m.add_function(wrap_pyfunction!(connect, m)?)?;

    // Add the load_config function at module level
    m.add_function(wrap_pyfunction!(config::py_load_config, m)?)?;

    // Add the metrics submodule
    metrics::register_metrics_module(m)?;

    // Add the workflow submodule
    let workflow_module = PyModule::new(m.py(), "workflow")?;
    crate::workflow::python::register(&workflow_module)?;
    m.add_submodule(&workflow_module)?;

    Ok(())
}

/// Connect to an S3-based task queue.
///
/// Creates a new queue connection using environment variables for configuration:
/// - `S3_ENDPOINT` (optional): Custom S3 endpoint URL
/// - `S3_BUCKET` (required): The bucket name
/// - `S3_REGION` (required): The AWS region
/// - `AWS_ACCESS_KEY_ID` (required): AWS access key
/// - `AWS_SECRET_ACCESS_KEY` (required): AWS secret key
/// - `BUQUET_SHARD_PREFIX_LEN` (optional): Number of hex characters for shard prefix (1-4)
///
/// Optional overrides can be provided as keyword arguments.
///
/// # Arguments
///
/// * `endpoint` - Optional custom S3 endpoint URL (overrides `S3_ENDPOINT`)
/// * `bucket` - Optional bucket name (overrides `S3_BUCKET`)
/// * `region` - Optional region (overrides `S3_REGION`)
/// * `shard_prefix_len` - Optional shard prefix length 1-4 (overrides `BUQUET_SHARD_PREFIX_LEN`)
///   - 1 = 16 shards (default)
///   - 2 = 256 shards
///   - 3 = 4096 shards
///   - 4 = 65536 shards
///
/// # Returns
///
/// A `PyQueue` instance connected to the S3 bucket.
///
/// # Example
///
/// ```python
/// import asyncio
/// import buquet
///
/// async def main():
///     # Connect with default 16 shards
///     queue = await buquet.connect()
///
///     # Connect with 256 shards for high-volume queues
///     queue = await buquet.connect(shard_prefix_len=2)
///
///     task = await queue.submit("my_task", {"data": "value"})
///     print(f"Submitted task: {task.id}")
///
/// asyncio.run(main())
/// ```
#[pyfunction]
#[pyo3(signature = (endpoint=None, bucket=None, region=None, shard_prefix_len=None))]
fn connect(
    py: Python<'_>,
    endpoint: Option<String>,
    bucket: Option<String>,
    region: Option<String>,
    shard_prefix_len: Option<usize>,
) -> PyResult<Bound<'_, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        queue::connect_impl(endpoint, bucket, region, shard_prefix_len).await
    })
}
