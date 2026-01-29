//! Python bindings for the workflow sweeper.

use pyo3::prelude::*;

use crate::state::StateManager;
use crate::sweeper::{RecoveryReason, SweepResult, WorkflowNeedsRecovery, WorkflowSweeper};

use super::state::PyStateManager;
use super::types::PyWorkflowState;

/// Python wrapper for `RecoveryReason`.
#[pyclass(name = "RecoveryReason")]
#[derive(Clone)]
pub struct PyRecoveryReason {
    inner: RecoveryReason,
}

impl From<RecoveryReason> for PyRecoveryReason {
    fn from(inner: RecoveryReason) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl PyRecoveryReason {
    /// Get the reason as a string.
    #[getter]
    #[allow(clippy::missing_const_for_fn)]
    fn reason(&self) -> &'static str {
        match &self.inner {
            RecoveryReason::NoOrchestratorTask => "no_orchestrator_task",
            RecoveryReason::OrchestratorTaskTerminal => "orchestrator_task_terminal",
            RecoveryReason::OrchestratorLeaseExpired => "orchestrator_lease_expired",
        }
    }

    fn __repr__(&self) -> String {
        format!("RecoveryReason({:?})", self.reason())
    }
}

/// Python wrapper for `WorkflowNeedsRecovery`.
#[pyclass(name = "WorkflowNeedsRecovery")]
#[derive(Clone)]
pub struct PyWorkflowNeedsRecovery {
    /// Workflow ID.
    #[pyo3(get)]
    pub workflow_id: String,
    /// The workflow state.
    #[pyo3(get)]
    pub state: PyWorkflowState,
    /// Reason for needing recovery.
    #[pyo3(get)]
    pub reason: PyRecoveryReason,
}

impl From<WorkflowNeedsRecovery> for PyWorkflowNeedsRecovery {
    fn from(w: WorkflowNeedsRecovery) -> Self {
        Self {
            workflow_id: w.workflow_id,
            state: PyWorkflowState::from(w.state),
            reason: PyRecoveryReason::from(w.reason),
        }
    }
}

#[pymethods]
impl PyWorkflowNeedsRecovery {
    fn __repr__(&self) -> String {
        format!(
            "WorkflowNeedsRecovery(workflow_id={:?}, reason={:?})",
            self.workflow_id,
            self.reason.reason()
        )
    }
}

/// Python wrapper for `SweepResult`.
#[pyclass(name = "SweepResult")]
#[derive(Clone)]
pub struct PySweepResult {
    /// Number of workflows scanned.
    #[pyo3(get)]
    pub scanned: usize,
    /// Workflows that need recovery.
    #[pyo3(get)]
    pub needs_recovery: Vec<PyWorkflowNeedsRecovery>,
    /// Number of terminal workflows.
    #[pyo3(get)]
    pub terminal: usize,
    /// Number of healthy workflows.
    #[pyo3(get)]
    pub healthy: usize,
}

impl From<SweepResult> for PySweepResult {
    fn from(r: SweepResult) -> Self {
        Self {
            scanned: r.scanned,
            needs_recovery: r.needs_recovery.into_iter().map(Into::into).collect(),
            terminal: r.terminal,
            healthy: r.healthy,
        }
    }
}

#[pymethods]
impl PySweepResult {
    fn __repr__(&self) -> String {
        format!(
            "SweepResult(scanned={}, needs_recovery={}, terminal={}, healthy={})",
            self.scanned,
            self.needs_recovery.len(),
            self.terminal,
            self.healthy
        )
    }
}

/// Python wrapper for the `WorkflowSweeper`.
#[pyclass(name = "WorkflowSweeper")]
#[derive(Clone)]
pub struct PyWorkflowSweeper {
    inner: WorkflowSweeper,
}

impl PyWorkflowSweeper {
    /// Create from a `StateManager`.
    #[must_use]
    pub const fn new(state_manager: StateManager) -> Self {
        Self {
            inner: WorkflowSweeper::new(state_manager),
        }
    }

    /// Create from a `PyStateManager`.
    #[must_use]
    pub fn from_py_state_manager(state_manager: &PyStateManager) -> Self {
        Self {
            inner: WorkflowSweeper::new(state_manager.inner().clone()),
        }
    }
}

#[pymethods]
impl PyWorkflowSweeper {
    /// Scan for workflows needing recovery (simple version without task checking).
    ///
    /// This only checks for missing `orchestrator_task_id`. For full checking
    /// that includes task status, use the Python wrapper that provides a
    /// `task_checker` callback.
    ///
    /// # Arguments
    ///
    /// * `prefix` - Optional prefix to filter workflows
    /// * `limit` - Maximum number of workflows to scan
    ///
    /// # Returns
    ///
    /// `SweepResult` with scan statistics and list of workflows needing recovery.
    #[pyo3(signature = (prefix="", limit=1000))]
    fn scan_simple<'py>(
        &self,
        py: Python<'py>,
        prefix: &str,
        limit: i32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let sweeper = self.inner.clone();
        let prefix = prefix.to_string();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = sweeper
                .scan_simple(&prefix, limit)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

            Ok(PySweepResult::from(result))
        })
    }

    #[allow(clippy::unused_self, clippy::missing_const_for_fn)]
    fn __repr__(&self) -> &'static str {
        "WorkflowSweeper()"
    }
}
