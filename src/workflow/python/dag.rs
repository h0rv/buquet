//! Python bindings for DAG utility functions.
//!
//! This module exposes pure DAG computation functions to Python.
//! These are synchronous functions since they don't do any I/O.

use pyo3::prelude::*;
use std::collections::BTreeMap;

use super::super::dag::{self, DagError};
use super::super::types::{compute_definition_hash as rust_compute_definition_hash, StepDef};

use super::types::{PyStepDef, PyWorkflowState};

/// Convert a list of `PyStepDef` to a `BTreeMap` keyed by step name.
fn convert_steps(steps: Vec<PyStepDef>) -> BTreeMap<String, StepDef> {
    steps
        .into_iter()
        .map(|s| (s.inner.name.clone(), s.inner))
        .collect()
}

/// Convert `DagError` to `PyErr`.
fn dag_error_to_py_err(e: &DagError) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string())
}

/// Compute which steps are ready to run.
///
/// A step is ready if:
/// 1. It's not already completed, running, or failed
/// 2. All its dependencies are completed
///
/// # Arguments
///
/// * `steps` - List of step definitions from the workflow
/// * `state` - Current workflow state
///
/// # Returns
///
/// List of step names that are ready to run
#[pyfunction]
#[pyo3(name = "compute_ready_steps")]
pub fn py_compute_ready_steps(steps: Vec<PyStepDef>, state: &PyWorkflowState) -> Vec<String> {
    let steps_map = convert_steps(steps);
    dag::compute_ready_steps(&steps_map, &state.inner)
}

/// Check if all steps in the workflow are completed.
///
/// # Arguments
///
/// * `steps` - List of step definitions from the workflow
/// * `state` - Current workflow state
///
/// # Returns
///
/// True if all steps are completed
#[pyfunction]
#[pyo3(name = "is_workflow_complete")]
pub fn py_is_workflow_complete(steps: Vec<PyStepDef>, state: &PyWorkflowState) -> bool {
    let steps_map = convert_steps(steps);
    dag::is_workflow_complete(&steps_map, &state.inner)
}

/// Check if there's a failed step that blocks further progress.
///
/// A failure is unrecoverable if:
/// 1. A step has failed
/// 2. That step's `on_failure` is `FAIL_WORKFLOW` or COMPENSATE
/// 3. Or other steps depend on it when `on_failure` is CONTINUE
///
/// # Arguments
///
/// * `steps` - List of step definitions from the workflow
/// * `state` - Current workflow state
///
/// # Returns
///
/// True if workflow cannot make further progress due to failure
#[pyfunction]
#[pyo3(name = "has_unrecoverable_failure")]
pub fn py_has_unrecoverable_failure(steps: Vec<PyStepDef>, state: &PyWorkflowState) -> bool {
    let steps_map = convert_steps(steps);
    dag::has_unrecoverable_failure(&steps_map, &state.inner)
}

/// Check if workflow should pause due to step failure with `PAUSE_WORKFLOW` policy.
///
/// # Arguments
///
/// * `steps` - List of step definitions from the workflow
/// * `state` - Current workflow state
///
/// # Returns
///
/// True if workflow should transition to paused state
#[pyfunction]
#[pyo3(name = "should_pause_workflow")]
pub fn py_should_pause_workflow(steps: Vec<PyStepDef>, state: &PyWorkflowState) -> bool {
    let steps_map = convert_steps(steps);
    dag::should_pause_workflow(&steps_map, &state.inner)
}

/// Get the compensation chain for completed steps in reverse order.
///
/// When a step fails with COMPENSATE policy, we need to run compensation
/// handlers for all previously completed steps in reverse order.
///
/// # Arguments
///
/// * `steps` - List of step definitions from the workflow
/// * `state` - Current workflow state
///
/// # Returns
///
/// List of step names that need compensation, in reverse completion order
#[pyfunction]
#[pyo3(name = "get_compensation_chain")]
pub fn py_get_compensation_chain(steps: Vec<PyStepDef>, state: &PyWorkflowState) -> Vec<String> {
    let steps_map = convert_steps(steps);
    dag::get_compensation_chain(&steps_map, &state.inner)
}

/// Get steps that are blocked by failed dependencies.
///
/// # Arguments
///
/// * `steps` - List of step definitions from the workflow
/// * `state` - Current workflow state
///
/// # Returns
///
/// List of step names that cannot run due to failed dependencies
#[pyfunction]
#[pyo3(name = "get_blocked_steps")]
pub fn py_get_blocked_steps(steps: Vec<PyStepDef>, state: &PyWorkflowState) -> Vec<String> {
    let steps_map = convert_steps(steps);
    dag::get_blocked_steps(&steps_map, &state.inner)
}

/// Sort steps in topological order (dependencies before dependents).
///
/// # Arguments
///
/// * `steps` - List of step definitions from the workflow
///
/// # Returns
///
/// List of step names in execution order
///
/// # Raises
///
/// `ValueError`: If the DAG is invalid (unknown dependency, cycle, etc.)
#[pyfunction]
#[pyo3(name = "topological_sort")]
pub fn py_topological_sort(steps: Vec<PyStepDef>) -> PyResult<Vec<String>> {
    let steps_map = convert_steps(steps);
    dag::topological_sort(&steps_map).map_err(|e| dag_error_to_py_err(&e))
}

/// Validate the workflow DAG structure.
///
/// Checks for:
/// - Missing dependencies (steps that depend on unknown steps)
/// - Self-dependencies
/// - Cycles in the dependency graph
///
/// # Arguments
///
/// * `steps` - List of step definitions from the workflow
///
/// # Returns
///
/// List of validation error strings (empty if valid)
#[pyfunction]
#[pyo3(name = "validate_dag")]
pub fn py_validate_dag(steps: Vec<PyStepDef>) -> Vec<String> {
    let steps_map = convert_steps(steps);
    dag::validate_dag(&steps_map)
}

/// Compute a hash of the workflow definition structure.
///
/// The hash is based on step names and their dependencies only,
/// not on handler code, timeouts, or retry policies. This allows
/// detecting structural changes to a workflow.
///
/// # Arguments
///
/// * `steps` - List of step definitions from the workflow
///
/// # Returns
///
/// A hash string in the format "sha256:XXXX..."
#[pyfunction]
#[pyo3(name = "compute_definition_hash")]
pub fn py_compute_definition_hash(steps: Vec<PyStepDef>) -> String {
    let steps_map = convert_steps(steps);
    rust_compute_definition_hash(&steps_map)
}

/// Register DAG functions in a Python module.
pub fn register_dag_functions(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_compute_ready_steps, m)?)?;
    m.add_function(wrap_pyfunction!(py_is_workflow_complete, m)?)?;
    m.add_function(wrap_pyfunction!(py_has_unrecoverable_failure, m)?)?;
    m.add_function(wrap_pyfunction!(py_should_pause_workflow, m)?)?;
    m.add_function(wrap_pyfunction!(py_get_compensation_chain, m)?)?;
    m.add_function(wrap_pyfunction!(py_get_blocked_steps, m)?)?;
    m.add_function(wrap_pyfunction!(py_topological_sort, m)?)?;
    m.add_function(wrap_pyfunction!(py_validate_dag, m)?)?;
    m.add_function(wrap_pyfunction!(py_compute_definition_hash, m)?)?;
    Ok(())
}
