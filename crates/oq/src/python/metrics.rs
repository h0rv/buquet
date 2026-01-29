//! Python bindings for metrics configuration.
//!
//! This module provides functions to configure metrics exporters from Python.
//! Metrics are emitted using the `metrics` crate facade, and these functions
//! install the appropriate recorder/exporter.

use pyo3::prelude::*;
use std::env;
use std::sync::OnceLock;

/// Track whether a metrics exporter has been installed.
/// Only one exporter can be installed per process.
static METRICS_INSTALLED: OnceLock<String> = OnceLock::new();

/// Check if metrics are already configured, return error if so.
fn check_not_configured() -> PyResult<()> {
    if let Some(exporter) = METRICS_INSTALLED.get() {
        return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
            "Metrics exporter already configured: {exporter}. Only one exporter can be installed per process.",
        )));
    }
    Ok(())
}

/// Mark metrics as configured with the given exporter name.
fn mark_configured(name: &str) -> PyResult<()> {
    METRICS_INSTALLED.set(name.to_string()).map_err(|_| {
        pyo3::exceptions::PyRuntimeError::new_err(
            "Metrics exporter already configured (race condition)",
        )
    })
}

/// Enable Prometheus metrics exporter.
///
/// Starts an HTTP server that exposes metrics at `http://0.0.0.0:{port}/metrics`.
///
/// # Arguments
///
/// * `port` - The port to listen on (default: 9000)
///
/// # Example
///
/// ```python
/// import oq
///
/// # Start Prometheus exporter on :9000/metrics
/// oq.metrics.enable_prometheus(port=9000)
///
/// # Now run your worker
/// ```
#[pyfunction]
#[pyo3(signature = (port=9000))]
pub fn enable_prometheus(port: u16) -> PyResult<()> {
    check_not_configured()?;

    metrics_exporter_prometheus::PrometheusBuilder::new()
        .with_http_listener(([0, 0, 0, 0], port))
        .install()
        .map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!(
                "Failed to install Prometheus exporter: {e}",
            ))
        })?;

    mark_configured("prometheus")?;
    tracing::info!(port, "Prometheus metrics exporter started");
    Ok(())
}

/// Enable `StatsD` metrics exporter (also works with Datadog `DogStatsD`).
///
/// Sends metrics to a StatsD-compatible server via UDP.
/// Note: oq metrics are already namespaced with "oq." prefix.
///
/// # Arguments
///
/// * `host` - The `StatsD` server host (default: "127.0.0.1")
/// * `port` - The `StatsD` server port (default: 8125)
///
/// # Example
///
/// ```python
/// import oq
///
/// # Send to local Datadog agent
/// oq.metrics.enable_statsd(host="127.0.0.1", port=8125)
/// ```
#[pyfunction]
#[pyo3(signature = (host="127.0.0.1", port=8125))]
pub fn enable_statsd(host: &str, port: u16) -> PyResult<()> {
    check_not_configured()?;

    let recorder = metrics_exporter_statsd::StatsdBuilder::from(host, port)
        .with_queue_size(5000)
        .with_buffer_size(1024)
        .build(None) // No prefix - oq metrics already use "oq." namespace
        .map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!(
                "Failed to build StatsD recorder: {e}",
            ))
        })?;

    metrics::set_global_recorder(recorder).map_err(|e| {
        pyo3::exceptions::PyRuntimeError::new_err(format!(
            "Failed to set global metrics recorder: {e}",
        ))
    })?;

    mark_configured("statsd")?;
    tracing::info!(host, port, "StatsD metrics exporter started");
    Ok(())
}

/// Enable OpenTelemetry OTLP metrics exporter.
///
/// Sends metrics to an OpenTelemetry collector via gRPC.
///
/// # Arguments
///
/// * `endpoint` - The OTLP endpoint (default: `http://localhost:4317`)
///
/// # Example
///
/// ```python
/// import oq
///
/// # Send to local OTEL collector
/// oq.metrics.enable_opentelemetry(endpoint="http://localhost:4317")
///
/// # Or to Datadog OTLP endpoint
/// oq.metrics.enable_opentelemetry(endpoint="http://localhost:4317")
/// ```
#[pyfunction]
#[pyo3(signature = (endpoint="http://localhost:4317"))]
pub fn enable_opentelemetry(endpoint: &str) -> PyResult<()> {
    use opentelemetry::metrics::MeterProvider;
    use opentelemetry_otlp::WithExportConfig;

    check_not_configured()?;

    // Build the OTLP exporter
    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()
        .map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(
                format!("Failed to build OTLP exporter: {e}",),
            )
        })?;

    // Build the meter provider with the exporter
    let meter_provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_periodic_exporter(exporter)
        .build();

    // Get a meter from the provider
    let meter = meter_provider.meter("oq");

    // Create the bridge from metrics crate to OpenTelemetry
    let otel_metrics = metrics_opentelemetry::OpenTelemetryMetrics::new(meter);
    let otel_recorder = metrics_opentelemetry::OpenTelemetryRecorder::new(otel_metrics);

    // Install as global recorder
    metrics::set_global_recorder(otel_recorder).map_err(|e| {
        pyo3::exceptions::PyRuntimeError::new_err(format!(
            "Failed to set global metrics recorder: {e}",
        ))
    })?;

    mark_configured("opentelemetry")?;
    tracing::info!(endpoint, "OpenTelemetry metrics exporter started");
    Ok(())
}

/// Auto-configure metrics from environment variables.
///
/// Checks the following environment variables:
///
/// - `OQ_METRICS_EXPORTER`: The exporter to use ("prometheus", "statsd", "datadog", "opentelemetry", "otlp")
/// - `OQ_METRICS_PROMETHEUS_PORT`: Port for Prometheus (default: 9000)
/// - `OQ_METRICS_STATSD_HOST`: StatsD/Datadog host (default: "127.0.0.1")
/// - `OQ_METRICS_STATSD_PORT`: StatsD/Datadog port (default: 8125)
/// - `OQ_METRICS_OTLP_ENDPOINT`: OTLP endpoint (default: `http://localhost:4317`)
///
/// # Returns
///
/// True if an exporter was configured, False if `OQ_METRICS_EXPORTER` was not set.
///
/// # Example
///
/// ```python
/// import oq
///
/// # Configure from environment
/// if oq.metrics.auto_configure():
///     print("Metrics configured from environment")
/// else:
///     print("No metrics exporter configured")
/// ```
#[pyfunction]
pub fn auto_configure() -> PyResult<bool> {
    let exporter = match env::var("OQ_METRICS_EXPORTER") {
        Ok(v) => v.to_lowercase(),
        Err(_) => return Ok(false),
    };

    match exporter.as_str() {
        "prometheus" => {
            let port: u16 = env::var("OQ_METRICS_PROMETHEUS_PORT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(9000);
            enable_prometheus(port)?;
        }
        "statsd" | "datadog" | "dogstatsd" => {
            let host = env::var("OQ_METRICS_STATSD_HOST").unwrap_or_else(|_| "127.0.0.1".into());
            let port: u16 = env::var("OQ_METRICS_STATSD_PORT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(8125);
            enable_statsd(&host, port)?;
        }
        "opentelemetry" | "otlp" | "otel" => {
            let endpoint = env::var("OQ_METRICS_OTLP_ENDPOINT")
                .unwrap_or_else(|_| "http://localhost:4317".into());
            enable_opentelemetry(&endpoint)?;
        }
        other => {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "Unknown metrics exporter: '{other}'. Valid options: prometheus, statsd, datadog, opentelemetry, otlp",
            )));
        }
    }

    Ok(true)
}

/// Check if a metrics exporter is currently configured.
///
/// # Returns
///
/// The name of the configured exporter, or None if not configured.
#[pyfunction]
pub fn current_exporter() -> Option<String> {
    METRICS_INSTALLED.get().cloned()
}

/// Python submodule for metrics configuration.
pub fn register_metrics_module(parent: &Bound<'_, PyModule>) -> PyResult<()> {
    let m = PyModule::new(parent.py(), "metrics")?;
    m.add_function(wrap_pyfunction!(enable_prometheus, &m)?)?;
    m.add_function(wrap_pyfunction!(enable_statsd, &m)?)?;
    m.add_function(wrap_pyfunction!(enable_opentelemetry, &m)?)?;
    m.add_function(wrap_pyfunction!(auto_configure, &m)?)?;
    m.add_function(wrap_pyfunction!(current_exporter, &m)?)?;
    parent.add_submodule(&m)?;
    Ok(())
}
