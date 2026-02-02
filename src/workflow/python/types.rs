//! Python bindings for workflow types.
//!
//! This module provides Python wrappers for workflow-related types.

use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::collections::BTreeMap;

use super::super::types::{
    OnFailure, Signal, SignalCursor, StepDef, StepState, StepStatus, WorkflowErrorInfo,
    WorkflowRun, WorkflowState, WorkflowStatus,
};

/// Workflow status enum exposed to Python.
#[pyclass(name = "WorkflowStatus", eq, eq_int)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PyWorkflowStatus {
    /// Workflow created but not yet started.
    Pending,
    /// Workflow is actively executing steps.
    Running,
    /// Workflow is waiting for a signal.
    WaitingSignal,
    /// Workflow is running compensation handlers.
    Compensating,
    /// Workflow is paused (can be resumed).
    Paused,
    /// Workflow completed successfully.
    Completed,
    /// Workflow failed.
    Failed,
    /// Workflow was cancelled.
    Cancelled,
}

#[pymethods]
#[allow(
    clippy::trivially_copy_pass_by_ref,
    clippy::missing_const_for_fn,
    clippy::unused_self
)]
impl PyWorkflowStatus {
    /// Returns the string representation of the status.
    fn __str__(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::WaitingSignal => "waiting_signal",
            Self::Compensating => "compensating",
            Self::Paused => "paused",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    /// Returns a debug representation.
    fn __repr__(&self) -> String {
        format!("WorkflowStatus.{}", self.__str__().to_uppercase())
    }

    /// Check if this is a terminal state.
    fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }

    /// Check if this is an active state (workflow is processing).
    fn is_active(&self) -> bool {
        matches!(
            self,
            Self::Running | Self::WaitingSignal | Self::Compensating
        )
    }
}

impl From<WorkflowStatus> for PyWorkflowStatus {
    fn from(status: WorkflowStatus) -> Self {
        match status {
            WorkflowStatus::Pending => Self::Pending,
            WorkflowStatus::Running => Self::Running,
            WorkflowStatus::WaitingSignal => Self::WaitingSignal,
            WorkflowStatus::Compensating => Self::Compensating,
            WorkflowStatus::Paused => Self::Paused,
            WorkflowStatus::Completed => Self::Completed,
            WorkflowStatus::Failed => Self::Failed,
            WorkflowStatus::Cancelled => Self::Cancelled,
        }
    }
}

impl From<PyWorkflowStatus> for WorkflowStatus {
    fn from(status: PyWorkflowStatus) -> Self {
        match status {
            PyWorkflowStatus::Pending => Self::Pending,
            PyWorkflowStatus::Running => Self::Running,
            PyWorkflowStatus::WaitingSignal => Self::WaitingSignal,
            PyWorkflowStatus::Compensating => Self::Compensating,
            PyWorkflowStatus::Paused => Self::Paused,
            PyWorkflowStatus::Completed => Self::Completed,
            PyWorkflowStatus::Failed => Self::Failed,
            PyWorkflowStatus::Cancelled => Self::Cancelled,
        }
    }
}

/// Step status enum exposed to Python.
#[pyclass(name = "StepStatus", eq, eq_int)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PyStepStatus {
    /// Step has not started.
    Pending,
    /// Step is currently executing.
    Running,
    /// Step completed successfully.
    Completed,
    /// Step failed.
    Failed,
    /// Step was cancelled.
    Cancelled,
}

#[pymethods]
#[allow(
    clippy::trivially_copy_pass_by_ref,
    clippy::missing_const_for_fn,
    clippy::unused_self
)]
impl PyStepStatus {
    /// Returns the string representation of the status.
    fn __str__(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    /// Returns a debug representation.
    fn __repr__(&self) -> String {
        format!("StepStatus.{}", self.__str__().to_uppercase())
    }
}

impl From<StepStatus> for PyStepStatus {
    fn from(status: StepStatus) -> Self {
        match status {
            StepStatus::Pending => Self::Pending,
            StepStatus::Running => Self::Running,
            StepStatus::Completed => Self::Completed,
            StepStatus::Failed => Self::Failed,
            StepStatus::Cancelled => Self::Cancelled,
        }
    }
}

impl From<PyStepStatus> for StepStatus {
    fn from(status: PyStepStatus) -> Self {
        match status {
            PyStepStatus::Pending => Self::Pending,
            PyStepStatus::Running => Self::Running,
            PyStepStatus::Completed => Self::Completed,
            PyStepStatus::Failed => Self::Failed,
            PyStepStatus::Cancelled => Self::Cancelled,
        }
    }
}

/// Action to take when a step fails.
#[pyclass(name = "OnFailure", eq, eq_int)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PyOnFailure {
    /// Fail the entire workflow.
    FailWorkflow,
    /// Pause the workflow for manual intervention.
    PauseWorkflow,
    /// Continue with other steps (if possible).
    Continue,
    /// Run compensation handlers.
    Compensate,
}

#[pymethods]
#[allow(
    clippy::trivially_copy_pass_by_ref,
    clippy::missing_const_for_fn,
    clippy::unused_self
)]
impl PyOnFailure {
    /// Returns the string representation of the action.
    fn __str__(&self) -> &'static str {
        match self {
            Self::FailWorkflow => "fail_workflow",
            Self::PauseWorkflow => "pause_workflow",
            Self::Continue => "continue",
            Self::Compensate => "compensate",
        }
    }

    /// Returns a debug representation.
    fn __repr__(&self) -> String {
        format!("OnFailure.{}", self.__str__().to_uppercase())
    }
}

impl From<OnFailure> for PyOnFailure {
    fn from(action: OnFailure) -> Self {
        match action {
            OnFailure::FailWorkflow => Self::FailWorkflow,
            OnFailure::PauseWorkflow => Self::PauseWorkflow,
            OnFailure::Continue => Self::Continue,
            OnFailure::Compensate => Self::Compensate,
        }
    }
}

impl From<PyOnFailure> for OnFailure {
    fn from(action: PyOnFailure) -> Self {
        match action {
            PyOnFailure::FailWorkflow => Self::FailWorkflow,
            PyOnFailure::PauseWorkflow => Self::PauseWorkflow,
            PyOnFailure::Continue => Self::Continue,
            PyOnFailure::Compensate => Self::Compensate,
        }
    }
}

/// Error information for a workflow.
#[pyclass(name = "WorkflowErrorInfo")]
#[derive(Debug, Clone)]
pub struct PyWorkflowErrorInfo {
    inner: WorkflowErrorInfo,
}

#[pymethods]
#[allow(clippy::missing_const_for_fn)]
impl PyWorkflowErrorInfo {
    /// Creates a new workflow error info.
    #[new]
    #[pyo3(signature = (type_, message, step=None, attempt=None, at=None))]
    fn new(
        type_: String,
        message: String,
        step: Option<String>,
        attempt: Option<u32>,
        at: Option<String>,
    ) -> Self {
        Self {
            inner: WorkflowErrorInfo {
                error_type: type_,
                message,
                step,
                attempt,
                at,
            },
        }
    }

    /// Error type/category.
    #[getter]
    fn type_(&self) -> String {
        self.inner.error_type.clone()
    }

    /// Error message.
    #[getter]
    fn message(&self) -> String {
        self.inner.message.clone()
    }

    /// Step that caused the error (if applicable).
    #[getter]
    fn step(&self) -> Option<String> {
        self.inner.step.clone()
    }

    /// Attempt number when error occurred.
    #[getter]
    fn attempt(&self) -> Option<u32> {
        self.inner.attempt
    }

    /// Timestamp when error occurred.
    #[getter]
    fn at(&self) -> Option<String> {
        self.inner.at.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "WorkflowErrorInfo(type_='{}', message='{}', step={:?}, attempt={:?}, at={:?})",
            self.inner.error_type,
            self.inner.message,
            self.inner.step,
            self.inner.attempt,
            self.inner.at
        )
    }

    /// Returns a dict representation.
    fn to_dict<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        dict.set_item("type", self.type_())?;
        dict.set_item("message", self.message())?;
        dict.set_item("step", self.step())?;
        dict.set_item("attempt", self.attempt())?;
        dict.set_item("at", self.at())?;
        Ok(dict)
    }
}

impl From<WorkflowErrorInfo> for PyWorkflowErrorInfo {
    fn from(info: WorkflowErrorInfo) -> Self {
        Self { inner: info }
    }
}

impl From<PyWorkflowErrorInfo> for WorkflowErrorInfo {
    fn from(info: PyWorkflowErrorInfo) -> Self {
        info.inner
    }
}

impl From<&PyWorkflowErrorInfo> for WorkflowErrorInfo {
    fn from(info: &PyWorkflowErrorInfo) -> Self {
        info.inner.clone()
    }
}

/// Cursor for signal consumption.
#[pyclass(name = "SignalCursor")]
#[derive(Debug, Clone)]
pub struct PySignalCursor {
    inner: SignalCursor,
}

#[pymethods]
#[allow(clippy::missing_const_for_fn)]
impl PySignalCursor {
    /// Creates a new signal cursor.
    #[new]
    #[pyo3(signature = (cursor=None))]
    fn new(cursor: Option<String>) -> Self {
        Self {
            inner: SignalCursor { cursor },
        }
    }

    /// The cursor position (`timestamp_uuid` suffix).
    #[getter]
    fn cursor(&self) -> Option<String> {
        self.inner.cursor.clone()
    }

    fn __repr__(&self) -> String {
        format!("SignalCursor(cursor={:?})", self.inner.cursor)
    }

    /// Returns a dict representation.
    fn to_dict<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        dict.set_item("cursor", self.cursor())?;
        Ok(dict)
    }
}

impl From<SignalCursor> for PySignalCursor {
    fn from(cursor: SignalCursor) -> Self {
        Self { inner: cursor }
    }
}

impl From<PySignalCursor> for SignalCursor {
    fn from(cursor: PySignalCursor) -> Self {
        cursor.inner
    }
}

impl From<&PySignalCursor> for SignalCursor {
    fn from(cursor: &PySignalCursor) -> Self {
        cursor.inner.clone()
    }
}

/// State of a workflow step.
#[pyclass(name = "StepState")]
#[derive(Debug, Clone)]
pub struct PyStepState {
    inner: StepState,
}

#[pymethods]
#[allow(clippy::missing_const_for_fn, clippy::needless_pass_by_value)]
impl PyStepState {
    /// Creates a new step state.
    #[new]
    #[pyo3(signature = (status=PyStepStatus::Pending, started_at=None, completed_at=None, result=None, error=None, attempt=0, task_id=None))]
    fn new(
        status: PyStepStatus,
        started_at: Option<String>,
        completed_at: Option<String>,
        result: Option<Bound<'_, PyAny>>,
        error: Option<String>,
        attempt: u32,
        task_id: Option<String>,
    ) -> PyResult<Self> {
        let result_value = match result {
            Some(ref obj) => Some(py_to_json(obj)?),
            None => None,
        };
        Ok(Self {
            inner: StepState {
                status: status.into(),
                started_at,
                completed_at,
                result: result_value,
                error,
                attempt,
                task_id,
            },
        })
    }

    /// Current status.
    #[getter]
    fn status(&self) -> PyStepStatus {
        self.inner.status.into()
    }

    /// When the step started.
    #[getter]
    fn started_at(&self) -> Option<String> {
        self.inner.started_at.clone()
    }

    /// When the step completed (success or failure).
    #[getter]
    fn completed_at(&self) -> Option<String> {
        self.inner.completed_at.clone()
    }

    /// Step result (if completed successfully).
    #[getter]
    fn result<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        match &self.inner.result {
            Some(value) => {
                let py_value = pythonize::pythonize(py, value)
                    .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
                Ok(Some(py_value))
            }
            None => Ok(None),
        }
    }

    /// Error message (if failed).
    #[getter]
    fn error(&self) -> Option<String> {
        self.inner.error.clone()
    }

    /// Current attempt number.
    #[getter]
    fn attempt(&self) -> u32 {
        self.inner.attempt
    }

    /// Task ID for the current execution.
    #[getter]
    fn task_id(&self) -> Option<String> {
        self.inner.task_id.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "StepState(status={}, attempt={}, task_id={:?})",
            PyStepStatus::from(self.inner.status).__str__(),
            self.inner.attempt,
            self.inner.task_id
        )
    }

    /// Returns a dict representation.
    fn to_dict<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        dict.set_item("status", self.status().__str__())?;
        dict.set_item("started_at", self.started_at())?;
        dict.set_item("completed_at", self.completed_at())?;
        dict.set_item("result", self.result(py)?)?;
        dict.set_item("error", self.error())?;
        dict.set_item("attempt", self.attempt())?;
        dict.set_item("task_id", self.task_id())?;
        Ok(dict)
    }
}

impl From<StepState> for PyStepState {
    fn from(state: StepState) -> Self {
        Self { inner: state }
    }
}

impl From<PyStepState> for StepState {
    fn from(state: PyStepState) -> Self {
        state.inner
    }
}

impl From<&PyStepState> for StepState {
    fn from(state: &PyStepState) -> Self {
        state.inner.clone()
    }
}

/// Full state of a workflow instance.
#[pyclass(name = "WorkflowState")]
#[derive(Debug, Clone)]
pub struct PyWorkflowState {
    pub(crate) inner: WorkflowState,
}

#[pymethods]
#[allow(clippy::needless_pass_by_value)]
impl PyWorkflowState {
    /// Creates a new workflow state.
    #[new]
    #[pyo3(signature = (id, workflow_type, definition_hash, data, created_at, updated_at, status=PyWorkflowStatus::Pending, current_steps=None, error=None, orchestrator_task_id=None))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        id: String,
        workflow_type: String,
        definition_hash: String,
        data: Bound<'_, PyAny>,
        created_at: String,
        updated_at: String,
        status: PyWorkflowStatus,
        current_steps: Option<Vec<String>>,
        error: Option<PyWorkflowErrorInfo>,
        orchestrator_task_id: Option<String>,
    ) -> PyResult<Self> {
        let data_value = py_to_json(&data)?;
        Ok(Self {
            inner: WorkflowState {
                id,
                workflow_type,
                definition_hash,
                status: status.into(),
                current_steps: current_steps.unwrap_or_default(),
                data: data_value,
                steps: BTreeMap::new(),
                signals: BTreeMap::new(),
                error: error.map(Into::into),
                created_at,
                updated_at,
                orchestrator_task_id,
            },
        })
    }

    /// Unique workflow ID.
    #[getter]
    fn id(&self) -> String {
        self.inner.id.clone()
    }

    /// Workflow type name.
    #[getter]
    fn workflow_type(&self) -> String {
        self.inner.workflow_type.clone()
    }

    /// Hash of the workflow definition structure.
    #[getter]
    fn definition_hash(&self) -> String {
        self.inner.definition_hash.clone()
    }

    /// Current workflow status.
    #[getter]
    fn status(&self) -> PyWorkflowStatus {
        self.inner.status.into()
    }

    /// Currently executing steps.
    #[getter]
    fn current_steps(&self) -> Vec<String> {
        self.inner.current_steps.clone()
    }

    /// Workflow input data.
    #[getter]
    fn data<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        pythonize::pythonize(py, &self.inner.data)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))
    }

    /// Step states keyed by step name.
    #[getter]
    fn steps<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        for (name, state) in &self.inner.steps {
            dict.set_item(name, PyStepState::from(state.clone()).into_pyobject(py)?)?;
        }
        Ok(dict)
    }

    /// Signal cursors keyed by signal name.
    #[getter]
    fn signals<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        for (name, cursor) in &self.inner.signals {
            dict.set_item(
                name,
                PySignalCursor::from(cursor.clone()).into_pyobject(py)?,
            )?;
        }
        Ok(dict)
    }

    /// Error information (if failed/paused).
    #[getter]
    fn error(&self) -> Option<PyWorkflowErrorInfo> {
        self.inner.error.clone().map(Into::into)
    }

    /// When the workflow was created.
    #[getter]
    fn created_at(&self) -> String {
        self.inner.created_at.clone()
    }

    /// When the workflow was last updated.
    #[getter]
    fn updated_at(&self) -> String {
        self.inner.updated_at.clone()
    }

    /// Current orchestrator task ID (for stall detection).
    #[getter]
    fn orchestrator_task_id(&self) -> Option<String> {
        self.inner.orchestrator_task_id.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "WorkflowState(id='{}', type='{}', status={})",
            self.inner.id,
            self.inner.workflow_type,
            PyWorkflowStatus::from(self.inner.status).__str__()
        )
    }

    /// Set a step's state.
    fn set_step(&mut self, name: String, state: PyStepState) {
        self.inner.steps.insert(name, state.inner);
    }

    /// Set a signal cursor.
    #[pyo3(signature = (name, cursor=None))]
    fn set_signal(&mut self, name: String, cursor: Option<String>) {
        self.inner.signals.insert(name, SignalCursor { cursor });
    }

    /// Returns a dict representation.
    fn to_dict<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        dict.set_item("id", self.id())?;
        dict.set_item("workflow_type", self.workflow_type())?;
        dict.set_item("definition_hash", self.definition_hash())?;
        dict.set_item("status", self.status().__str__())?;
        dict.set_item("current_steps", self.current_steps())?;
        dict.set_item("data", self.data(py)?)?;

        // Convert steps to dict of dicts
        let steps_dict = PyDict::new(py);
        for (name, state) in &self.inner.steps {
            steps_dict.set_item(name, PyStepState::from(state.clone()).to_dict(py)?)?;
        }
        dict.set_item("steps", steps_dict)?;

        // Convert signals to dict of dicts
        let signals_dict = PyDict::new(py);
        for (name, cursor) in &self.inner.signals {
            signals_dict.set_item(name, PySignalCursor::from(cursor.clone()).to_dict(py)?)?;
        }
        dict.set_item("signals", signals_dict)?;

        dict.set_item("error", self.error().map(|e| e.to_dict(py)).transpose()?)?;
        dict.set_item("created_at", self.created_at())?;
        dict.set_item("updated_at", self.updated_at())?;
        dict.set_item("orchestrator_task_id", self.orchestrator_task_id())?;
        Ok(dict)
    }
}

impl From<WorkflowState> for PyWorkflowState {
    fn from(state: WorkflowState) -> Self {
        Self { inner: state }
    }
}

impl From<PyWorkflowState> for WorkflowState {
    fn from(state: PyWorkflowState) -> Self {
        state.inner
    }
}

impl From<&PyWorkflowState> for WorkflowState {
    fn from(state: &PyWorkflowState) -> Self {
        state.inner.clone()
    }
}

/// A signal sent to a workflow.
#[pyclass(name = "Signal")]
#[derive(Debug, Clone)]
pub struct PySignal {
    inner: Signal,
}

#[pymethods]
#[allow(clippy::needless_pass_by_value)]
impl PySignal {
    /// Creates a new signal.
    #[new]
    fn new(
        id: String,
        name: String,
        payload: Bound<'_, PyAny>,
        created_at: String,
    ) -> PyResult<Self> {
        let payload_value = py_to_json(&payload)?;
        Ok(Self {
            inner: Signal {
                id,
                name,
                payload: payload_value,
                created_at,
            },
        })
    }

    /// Unique signal ID.
    #[getter]
    fn id(&self) -> String {
        self.inner.id.clone()
    }

    /// Signal name.
    #[getter]
    fn name(&self) -> String {
        self.inner.name.clone()
    }

    /// Signal payload.
    #[getter]
    fn payload<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        pythonize::pythonize(py, &self.inner.payload)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))
    }

    /// When the signal was created.
    #[getter]
    fn created_at(&self) -> String {
        self.inner.created_at.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "Signal(id='{}', name='{}', created_at='{}')",
            self.inner.id, self.inner.name, self.inner.created_at
        )
    }

    /// Returns a dict representation.
    fn to_dict<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        dict.set_item("id", self.id())?;
        dict.set_item("name", self.name())?;
        dict.set_item("payload", self.payload(py)?)?;
        dict.set_item("created_at", self.created_at())?;
        Ok(dict)
    }
}

impl From<Signal> for PySignal {
    fn from(signal: Signal) -> Self {
        Self { inner: signal }
    }
}

impl From<PySignal> for Signal {
    fn from(signal: PySignal) -> Self {
        signal.inner
    }
}

impl From<&PySignal> for Signal {
    fn from(signal: &PySignal) -> Self {
        signal.inner.clone()
    }
}

/// Definition of a workflow step (for DAG computation).
#[pyclass(name = "StepDef")]
#[derive(Debug, Clone)]
pub struct PyStepDef {
    pub(crate) inner: StepDef,
}

#[pymethods]
#[allow(clippy::missing_const_for_fn)]
impl PyStepDef {
    /// Creates a new step definition.
    #[new]
    #[pyo3(signature = (name, depends_on=None, retries=3, timeout=300, on_failure=PyOnFailure::FailWorkflow, has_compensation=false))]
    fn new(
        name: String,
        depends_on: Option<Vec<String>>,
        retries: u32,
        timeout: u32,
        on_failure: PyOnFailure,
        has_compensation: bool,
    ) -> Self {
        Self {
            inner: StepDef {
                name,
                depends_on: depends_on.unwrap_or_default(),
                retries,
                timeout,
                on_failure: on_failure.into(),
                has_compensation,
            },
        }
    }

    /// Step name.
    #[getter]
    fn name(&self) -> String {
        self.inner.name.clone()
    }

    /// Steps that must complete before this one.
    #[getter]
    fn depends_on(&self) -> Vec<String> {
        self.inner.depends_on.clone()
    }

    /// Maximum retry attempts.
    #[getter]
    fn retries(&self) -> u32 {
        self.inner.retries
    }

    /// Timeout in seconds.
    #[getter]
    fn timeout(&self) -> u32 {
        self.inner.timeout
    }

    /// Action on failure.
    #[getter]
    fn on_failure(&self) -> PyOnFailure {
        self.inner.on_failure.into()
    }

    /// Whether this step has a compensation handler.
    #[getter]
    fn has_compensation(&self) -> bool {
        self.inner.has_compensation
    }

    fn __repr__(&self) -> String {
        format!(
            "StepDef(name='{}', depends_on={:?}, retries={}, timeout={}, on_failure={}, has_compensation={})",
            self.inner.name,
            self.inner.depends_on,
            self.inner.retries,
            self.inner.timeout,
            PyOnFailure::from(self.inner.on_failure).__str__(),
            self.inner.has_compensation
        )
    }

    /// Returns a dict representation.
    fn to_dict<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        dict.set_item("name", self.name())?;
        dict.set_item("depends_on", self.depends_on())?;
        dict.set_item("retries", self.retries())?;
        dict.set_item("timeout", self.timeout())?;
        dict.set_item("on_failure", self.on_failure().__str__())?;
        dict.set_item("has_compensation", self.has_compensation())?;
        Ok(dict)
    }
}

impl From<StepDef> for PyStepDef {
    fn from(def: StepDef) -> Self {
        Self { inner: def }
    }
}

impl From<PyStepDef> for StepDef {
    fn from(def: PyStepDef) -> Self {
        def.inner
    }
}

impl From<&PyStepDef> for StepDef {
    fn from(def: &PyStepDef) -> Self {
        def.inner.clone()
    }
}

/// A running workflow instance (client view).
#[pyclass(name = "WorkflowRun")]
#[derive(Debug, Clone)]
pub struct PyWorkflowRun {
    inner: WorkflowRun,
}

#[pymethods]
#[allow(clippy::needless_pass_by_value)]
impl PyWorkflowRun {
    /// Creates a new workflow run.
    #[new]
    #[pyo3(signature = (id, status, workflow_type=None, data=None, current_steps=None, error=None))]
    fn new(
        id: String,
        status: PyWorkflowStatus,
        workflow_type: Option<String>,
        data: Option<Bound<'_, PyAny>>,
        current_steps: Option<Vec<String>>,
        error: Option<PyWorkflowErrorInfo>,
    ) -> PyResult<Self> {
        let data_value = match data {
            Some(ref obj) => Some(py_to_json(obj)?),
            None => None,
        };
        Ok(Self {
            inner: WorkflowRun {
                id,
                status: status.into(),
                workflow_type,
                data: data_value,
                current_steps,
                error: error.map(Into::into),
            },
        })
    }

    /// Workflow ID.
    #[getter]
    fn id(&self) -> String {
        self.inner.id.clone()
    }

    /// Current status.
    #[getter]
    fn status(&self) -> PyWorkflowStatus {
        self.inner.status.into()
    }

    /// Workflow type.
    #[getter]
    fn workflow_type(&self) -> Option<String> {
        self.inner.workflow_type.clone()
    }

    /// Workflow data.
    #[getter]
    fn data<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        match &self.inner.data {
            Some(value) => {
                let py_value = pythonize::pythonize(py, value)
                    .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
                Ok(Some(py_value))
            }
            None => Ok(None),
        }
    }

    /// Currently executing steps.
    #[getter]
    fn current_steps(&self) -> Option<Vec<String>> {
        self.inner.current_steps.clone()
    }

    /// Error information.
    #[getter]
    fn error(&self) -> Option<PyWorkflowErrorInfo> {
        self.inner.error.clone().map(Into::into)
    }

    fn __repr__(&self) -> String {
        format!(
            "WorkflowRun(id='{}', status={})",
            self.inner.id,
            PyWorkflowStatus::from(self.inner.status).__str__()
        )
    }

    /// Returns a dict representation.
    fn to_dict<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        dict.set_item("id", self.id())?;
        dict.set_item("status", self.status().__str__())?;
        dict.set_item("workflow_type", self.workflow_type())?;
        dict.set_item("data", self.data(py)?)?;
        dict.set_item("current_steps", self.current_steps())?;
        dict.set_item("error", self.error().map(|e| e.to_dict(py)).transpose()?)?;
        Ok(dict)
    }

    /// Create from workflow state.
    #[staticmethod]
    fn from_state(state: &PyWorkflowState) -> Self {
        WorkflowRun::from_state(&state.inner).into()
    }
}

impl From<WorkflowRun> for PyWorkflowRun {
    fn from(run: WorkflowRun) -> Self {
        Self { inner: run }
    }
}

impl From<PyWorkflowRun> for WorkflowRun {
    fn from(run: PyWorkflowRun) -> Self {
        run.inner
    }
}

impl From<&PyWorkflowRun> for WorkflowRun {
    fn from(run: &PyWorkflowRun) -> Self {
        run.inner.clone()
    }
}

/// Helper to convert a Python object to `serde_json::Value`
pub fn py_to_json(obj: &Bound<'_, PyAny>) -> PyResult<serde_json::Value> {
    pythonize::depythonize(obj)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))
}
