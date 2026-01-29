//! Python bindings for the workflow engine.

use pyo3::prelude::*;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::engine::{OrchestrateResult, WorkflowEngine};
use crate::types::StepDef;
use oq::storage::S3Client;

use super::signals::PySignalManager;
use super::state::PyStateManager;
use super::types::{PySignal, PyStepDef, PyWorkflowState};

/// Python wrapper for `OrchestrateResult`.
#[pyclass(name = "OrchestrateResult")]
#[derive(Clone)]
pub struct PyOrchestrateResult {
    /// Result type: "noop", `"version_mismatch"`, "completed", "failed", "paused", "waiting", "scheduled"
    #[pyo3(get)]
    pub result_type: String,
    /// Workflow ID
    #[pyo3(get)]
    pub workflow_id: String,
    /// Current status (for noop)
    #[pyo3(get)]
    pub status: Option<String>,
    /// Expected hash (for `version_mismatch`)
    #[pyo3(get)]
    pub expected_hash: Option<String>,
    /// Actual hash (for `version_mismatch`)
    #[pyo3(get)]
    pub actual_hash: Option<String>,
    /// Reason (for paused)
    #[pyo3(get)]
    pub reason: Option<String>,
    /// Current steps
    #[pyo3(get)]
    pub current_steps: Option<Vec<String>>,
    /// Submitted steps (for scheduled)
    #[pyo3(get)]
    pub submitted_steps: Option<Vec<String>>,
}

impl From<OrchestrateResult> for PyOrchestrateResult {
    fn from(result: OrchestrateResult) -> Self {
        match result {
            OrchestrateResult::Noop {
                workflow_id,
                status,
            } => Self {
                result_type: "noop".to_string(),
                workflow_id,
                status: Some(format!("{status:?}").to_lowercase()),
                expected_hash: None,
                actual_hash: None,
                reason: None,
                current_steps: None,
                submitted_steps: None,
            },
            OrchestrateResult::VersionMismatch {
                workflow_id,
                expected,
                actual,
            } => Self {
                result_type: "version_mismatch".to_string(),
                workflow_id,
                status: None,
                expected_hash: Some(expected),
                actual_hash: Some(actual),
                reason: None,
                current_steps: None,
                submitted_steps: None,
            },
            OrchestrateResult::Completed { workflow_id } => Self {
                result_type: "completed".to_string(),
                workflow_id,
                status: None,
                expected_hash: None,
                actual_hash: None,
                reason: None,
                current_steps: None,
                submitted_steps: None,
            },
            OrchestrateResult::Failed { workflow_id } => Self {
                result_type: "failed".to_string(),
                workflow_id,
                status: None,
                expected_hash: None,
                actual_hash: None,
                reason: None,
                current_steps: None,
                submitted_steps: None,
            },
            OrchestrateResult::Paused {
                workflow_id,
                reason,
            } => Self {
                result_type: "paused".to_string(),
                workflow_id,
                status: None,
                expected_hash: None,
                actual_hash: None,
                reason: Some(reason),
                current_steps: None,
                submitted_steps: None,
            },
            OrchestrateResult::Waiting {
                workflow_id,
                current_steps,
            } => Self {
                result_type: "waiting".to_string(),
                workflow_id,
                status: None,
                expected_hash: None,
                actual_hash: None,
                reason: None,
                current_steps: Some(current_steps),
                submitted_steps: None,
            },
            OrchestrateResult::Scheduled {
                workflow_id,
                submitted_steps,
                current_steps,
            } => Self {
                result_type: "scheduled".to_string(),
                workflow_id,
                status: None,
                expected_hash: None,
                actual_hash: None,
                reason: None,
                current_steps: Some(current_steps),
                submitted_steps: Some(submitted_steps),
            },
        }
    }
}

#[pymethods]
impl PyOrchestrateResult {
    #[allow(clippy::missing_const_for_fn)]
    fn __repr__(&self) -> String {
        format!(
            "OrchestrateResult(type={:?}, workflow_id={:?})",
            self.result_type, self.workflow_id
        )
    }
}

/// Workflow execution engine.
#[pyclass(name = "WorkflowEngine")]
#[derive(Clone)]
pub struct PyWorkflowEngine {
    inner: WorkflowEngine,
}

impl PyWorkflowEngine {
    /// Create a new engine from an S3 client.
    #[must_use]
    pub fn new(client: Arc<S3Client>) -> Self {
        Self {
            inner: WorkflowEngine::new(client),
        }
    }
}

#[pymethods]
impl PyWorkflowEngine {
    /// Create a new `WorkflowEngine`.
    ///
    /// # Arguments
    ///
    /// * `bucket` - S3 bucket name
    /// * `endpoint` - Optional S3 endpoint URL (for S3-compatible services)
    /// * `region` - AWS region (default: us-east-1)
    ///
    /// # Example
    ///
    /// ```python
    /// engine = await WorkflowEngine.create(
    ///     bucket="my-bucket",
    ///     endpoint="http://localhost:4566",  # LocalStack
    ///     region="us-east-1"
    /// )
    /// ```
    #[staticmethod]
    #[pyo3(signature = (bucket, endpoint=None, region="us-east-1".to_string()))]
    fn create(
        py: Python<'_>,
        bucket: String,
        endpoint: Option<String>,
        region: String,
    ) -> PyResult<Bound<'_, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let config = oq::storage::S3Config::new(endpoint, bucket, region);
            let client = oq::storage::S3Client::new(config)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

            Ok(Self {
                inner: WorkflowEngine::new(Arc::new(client)),
            })
        })
    }

    /// Get the state manager.
    #[getter]
    #[must_use]
    fn state(&self) -> PyStateManager {
        PyStateManager::from_manager(self.inner.state().clone())
    }

    /// Get the signal manager.
    #[getter]
    #[must_use]
    fn signals(&self) -> PySignalManager {
        PySignalManager::from_manager(self.inner.signals().clone())
    }

    /// Start a workflow with idempotency support.
    ///
    /// Creates the initial workflow state if it doesn't exist. If the workflow
    /// already exists, returns the existing state without modification.
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The workflow ID
    /// * `workflow_type` - The workflow type name
    /// * `definition_hash` - Hash of the workflow definition
    /// * `data` - Initial workflow data (Python dict)
    /// * `now` - Current timestamp
    ///
    /// # Returns
    ///
    /// Tuple of (created: bool, state: `WorkflowState`). If created is False,
    /// the workflow already existed.
    #[allow(clippy::needless_pass_by_value)]
    fn start_workflow<'py>(
        &self,
        py: Python<'py>,
        workflow_id: String,
        workflow_type: String,
        definition_hash: String,
        data: PyObject,
        now: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let data_json: serde_json::Value = pythonize::depythonize(data.bind(py))
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
        let engine = self.inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let (created, state) = engine
                .start_workflow(
                    &workflow_id,
                    &workflow_type,
                    &definition_hash,
                    data_json,
                    &now,
                )
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

            Ok((created, PyWorkflowState::from(state)))
        })
    }

    /// Run one orchestration tick.
    ///
    /// Returns (`OrchestrateResult`, `Optional[WorkflowState]`).
    fn orchestrate<'py>(
        &self,
        py: Python<'py>,
        workflow_id: String,
        steps: Vec<PyStepDef>,
        definition_hash: String,
        now: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.inner.clone();
        let steps_map: BTreeMap<String, StepDef> = steps
            .into_iter()
            .map(|s| (s.inner.name.clone(), s.inner))
            .collect();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let (result, state) = engine
                .orchestrate(&workflow_id, &steps_map, &definition_hash, &now)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

            let py_result = PyOrchestrateResult::from(result);
            let py_state: Option<PyWorkflowState> = state.map(Into::into);

            Ok((py_result, py_state))
        })
    }

    /// Mark steps as running after tasks have been submitted.
    fn mark_steps_running<'py>(
        &self,
        py: Python<'py>,
        workflow_id: String,
        step_names: Vec<String>,
        now: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            engine
                .mark_steps_running(&workflow_id, &step_names, &now)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            Ok(())
        })
    }

    /// Mark a step as completed.
    #[allow(clippy::needless_pass_by_value)]
    fn complete_step<'py>(
        &self,
        py: Python<'py>,
        workflow_id: String,
        step_name: String,
        result: PyObject,
        now: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let result_json: serde_json::Value = pythonize::depythonize(result.bind(py))
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
        let engine = self.inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            engine
                .complete_step(&workflow_id, &step_name, result_json, &now)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            Ok(())
        })
    }

    /// Mark a step as failed (only call after retries exhausted).
    fn fail_step<'py>(
        &self,
        py: Python<'py>,
        workflow_id: String,
        step_name: String,
        error: String,
        now: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            engine
                .fail_step(&workflow_id, &step_name, &error, &now)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            Ok(())
        })
    }

    /// Mark a step as cancelled.
    fn cancel_step<'py>(
        &self,
        py: Python<'py>,
        workflow_id: String,
        step_name: String,
        reason: String,
        now: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            engine
                .cancel_step(&workflow_id, &step_name, &reason, &now)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            Ok(())
        })
    }

    /// Consume the next signal and advance the cursor atomically.
    fn consume_signal<'py>(
        &self,
        py: Python<'py>,
        workflow_id: String,
        signal_name: String,
        now: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let signal = engine
                .consume_signal(&workflow_id, &signal_name, &now)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

            Ok(signal.map(PySignal::from))
        })
    }

    #[allow(clippy::missing_const_for_fn, clippy::unused_self)]
    fn __repr__(&self) -> String {
        "WorkflowEngine()".to_string()
    }
}
