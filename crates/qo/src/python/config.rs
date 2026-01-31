//! Python bindings for configuration loading.
//!
//! This module provides Python wrappers for `QoConfig`, `WorkerConfig`, and `MonitorConfig`.

use pyo3::prelude::*;
use std::path::PathBuf;

use crate::config::{load_config, MonitorConfig, QoConfig, WorkerConfig};

/// Worker-specific configuration.
#[pyclass(name = "WorkerConfig")]
#[derive(Debug, Clone)]
pub struct PyWorkerConfig {
    inner: WorkerConfig,
}

#[pymethods]
#[allow(clippy::missing_const_for_fn)]
impl PyWorkerConfig {
    /// Polling interval in milliseconds.
    #[getter]
    fn poll_interval_ms(&self) -> Option<u64> {
        self.inner.poll_interval_ms
    }

    /// Index mode for ready task discovery ("hybrid", "index", "scan").
    #[getter]
    fn index_mode(&self) -> Option<String> {
        self.inner.index_mode.clone()
    }

    /// Shards to process (e.g., `["0", "1", "2", "3"]`).
    #[getter]
    fn shards(&self) -> Option<Vec<String>> {
        self.inner.shards.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "WorkerConfig(poll_interval_ms={:?}, index_mode={:?}, shards={:?})",
            self.inner.poll_interval_ms, self.inner.index_mode, self.inner.shards
        )
    }
}

impl From<WorkerConfig> for PyWorkerConfig {
    fn from(config: WorkerConfig) -> Self {
        Self { inner: config }
    }
}

/// Monitor-specific configuration.
#[pyclass(name = "MonitorConfig")]
#[derive(Debug, Clone)]
pub struct PyMonitorConfig {
    inner: MonitorConfig,
}

#[pymethods]
#[allow(clippy::missing_const_for_fn)]
impl PyMonitorConfig {
    /// Interval in seconds between health checks.
    #[getter]
    fn check_interval_secs(&self) -> Option<u64> {
        self.inner.check_interval_secs
    }

    /// Interval in seconds between sweep operations.
    #[getter]
    fn sweep_interval_secs(&self) -> Option<u64> {
        self.inner.sweep_interval_secs
    }

    fn __repr__(&self) -> String {
        format!(
            "MonitorConfig(check_interval_secs={:?}, sweep_interval_secs={:?})",
            self.inner.check_interval_secs, self.inner.sweep_interval_secs
        )
    }
}

impl From<MonitorConfig> for PyMonitorConfig {
    fn from(config: MonitorConfig) -> Self {
        Self { inner: config }
    }
}

/// Main configuration for qo.
#[pyclass(name = "Config")]
#[derive(Debug, Clone)]
pub struct PyConfig {
    inner: QoConfig,
}

#[pymethods]
impl PyConfig {
    /// S3 bucket name for the queue.
    #[getter]
    fn bucket(&self) -> String {
        self.inner.bucket.clone()
    }

    /// Optional custom S3 endpoint (e.g., for local development with LocalStack/MinIO).
    #[getter]
    fn endpoint(&self) -> Option<String> {
        self.inner.endpoint.clone()
    }

    /// AWS region (defaults to "us-east-1").
    #[getter]
    fn region(&self) -> String {
        self.inner.region.clone()
    }

    /// Worker-specific configuration.
    #[getter]
    fn worker(&self) -> PyWorkerConfig {
        self.inner.worker.clone().into()
    }

    /// Monitor-specific configuration.
    #[getter]
    fn monitor(&self) -> PyMonitorConfig {
        self.inner.monitor.clone().into()
    }

    fn __repr__(&self) -> String {
        format!(
            "Config(bucket='{}', endpoint={:?}, region='{}')",
            self.inner.bucket, self.inner.endpoint, self.inner.region
        )
    }
}

impl From<QoConfig> for PyConfig {
    fn from(config: QoConfig) -> Self {
        Self { inner: config }
    }
}

/// Load configuration from files and environment variables.
///
/// Resolution order (highest to lowest priority):
/// 1. Environment variables (`S3_BUCKET`, `S3_ENDPOINT`, `S3_REGION`, `QO_*`)
/// 2. Profile-specific settings (if profile specified)
/// 3. Project-level `.qo.toml`
/// 4. User-level `~/.config/qo/config.toml`
/// 5. Built-in defaults
///
/// # Arguments
///
/// * `profile` - Optional profile name to use
///
/// # Returns
///
/// A Config object with the resolved configuration.
///
/// # Example
///
/// ```python
/// import qo
///
/// # Load default config
/// config = qo.load_config()
///
/// # Load with a specific profile
/// config = qo.load_config(profile="dev")
///
/// print(f"Bucket: {config.bucket}")
/// print(f"Region: {config.region}")
/// ```
#[pyfunction]
#[pyo3(signature = (profile=None))]
#[allow(clippy::needless_pass_by_value)]
pub fn py_load_config(profile: Option<String>) -> PyResult<PyConfig> {
    let config = load_config(profile.as_deref(), None::<&PathBuf>)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
    Ok(config.into())
}
