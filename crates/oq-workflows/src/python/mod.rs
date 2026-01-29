//! Python bindings for oq-workflows.
//!
//! This module provides `PyO3` bindings to expose the Rust workflow
//! functionality to Python.

mod dag;
mod engine;
mod signals;
mod state;
mod sweeper;
mod types;

use pyo3::prelude::*;

pub use dag::register_dag_functions;
pub use engine::{PyOrchestrateResult, PyWorkflowEngine};
pub use signals::PySignalManager;
pub use state::PyStateManager;
pub use sweeper::{PyRecoveryReason, PySweepResult, PyWorkflowNeedsRecovery, PyWorkflowSweeper};
pub use types::{
    PyOnFailure, PySignal, PySignalCursor, PyStepDef, PyStepState, PyStepStatus,
    PyWorkflowErrorInfo, PyWorkflowRun, PyWorkflowState, PyWorkflowStatus,
};

/// The `oq_workflows` Python module.
///
/// Provides workflow orchestration functionality built on oq.
#[pymodule]
#[pyo3(name = "_oq_workflows")]
pub fn oq_workflows(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Type classes
    m.add_class::<PyWorkflowStatus>()?;
    m.add_class::<PyStepStatus>()?;
    m.add_class::<PyOnFailure>()?;
    m.add_class::<PyWorkflowErrorInfo>()?;
    m.add_class::<PySignalCursor>()?;
    m.add_class::<PyStepState>()?;
    m.add_class::<PyWorkflowState>()?;
    m.add_class::<PySignal>()?;
    m.add_class::<PyStepDef>()?;
    m.add_class::<PyWorkflowRun>()?;

    // Manager classes
    m.add_class::<PyStateManager>()?;
    m.add_class::<PySignalManager>()?;

    // Engine classes
    m.add_class::<PyWorkflowEngine>()?;
    m.add_class::<PyOrchestrateResult>()?;

    // Sweeper classes
    m.add_class::<PyWorkflowSweeper>()?;
    m.add_class::<PySweepResult>()?;
    m.add_class::<PyWorkflowNeedsRecovery>()?;
    m.add_class::<PyRecoveryReason>()?;

    // DAG functions
    register_dag_functions(m)?;

    // Signal parsing function
    m.add_function(wrap_pyfunction!(signals::parse_signal_key, m)?)?;

    Ok(())
}
