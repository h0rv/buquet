//! Python bindings for schedule types.
//!
//! This module provides Python wrappers for `Schedule` and `ScheduleManager`.

use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::queue::{Schedule, ScheduleLastRun};

/// A recurring schedule for automatic task submission.
#[pyclass(name = "Schedule")]
#[derive(Debug, Clone)]
pub struct PySchedule {
    pub(crate) inner: Schedule,
}

impl PySchedule {
    /// Creates a new `PySchedule` from a Rust `Schedule`.
    #[must_use]
    pub const fn new(schedule: Schedule) -> Self {
        Self { inner: schedule }
    }
}

#[pymethods]
#[allow(clippy::missing_const_for_fn)]
impl PySchedule {
    /// Unique schedule identifier.
    #[getter]
    fn id(&self) -> &str {
        &self.inner.id
    }

    /// Task type to submit when triggered.
    #[getter]
    fn task_type(&self) -> &str {
        &self.inner.task_type
    }

    /// Input data for the task as a Python dict.
    #[getter]
    fn input<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        pythonize::pythonize(py, &self.inner.input)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))
    }

    /// Cron expression (5-field standard format).
    #[getter]
    fn cron(&self) -> &str {
        &self.inner.cron
    }

    /// Whether the schedule is enabled.
    #[getter]
    fn enabled(&self) -> bool {
        self.inner.enabled
    }

    /// Task timeout in seconds (None for default).
    #[getter]
    fn timeout_seconds(&self) -> Option<u64> {
        self.inner.timeout_seconds
    }

    /// Task max retries (None for default).
    #[getter]
    fn max_retries(&self) -> Option<u32> {
        self.inner.max_retries
    }

    /// When the schedule was created (ISO 8601 string).
    #[getter]
    fn created_at(&self) -> String {
        self.inner.created_at.to_rfc3339()
    }

    /// When the schedule was last updated (ISO 8601 string).
    #[getter]
    fn updated_at(&self) -> String {
        self.inner.updated_at.to_rfc3339()
    }

    /// Returns a dict representation of the schedule.
    fn to_dict<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        dict.set_item("id", self.id())?;
        dict.set_item("task_type", self.task_type())?;
        dict.set_item("input", self.input(py)?)?;
        dict.set_item("cron", self.cron())?;
        dict.set_item("enabled", self.enabled())?;
        dict.set_item("timeout_seconds", self.timeout_seconds())?;
        dict.set_item("max_retries", self.max_retries())?;
        dict.set_item("created_at", self.created_at())?;
        dict.set_item("updated_at", self.updated_at())?;
        Ok(dict)
    }

    fn __repr__(&self) -> String {
        format!(
            "Schedule(id='{}', task_type='{}', cron='{}', enabled={})",
            self.inner.id, self.inner.task_type, self.inner.cron, self.inner.enabled
        )
    }
}

/// Last run information for a schedule.
#[pyclass(name = "ScheduleLastRun")]
#[derive(Debug, Clone)]
pub struct PyScheduleLastRun {
    pub(crate) inner: ScheduleLastRun,
}

impl PyScheduleLastRun {
    /// Creates a new `PyScheduleLastRun` from a Rust `ScheduleLastRun`.
    #[must_use]
    pub const fn new(last_run: ScheduleLastRun) -> Self {
        Self { inner: last_run }
    }
}

#[pymethods]
impl PyScheduleLastRun {
    /// Schedule ID.
    #[getter]
    fn schedule_id(&self) -> &str {
        &self.inner.schedule_id
    }

    /// When the schedule was last triggered (ISO 8601 string).
    #[getter]
    fn last_run_at(&self) -> String {
        self.inner.last_run_at.to_rfc3339()
    }

    /// Task ID of the last submitted task.
    #[getter]
    fn last_task_id(&self) -> String {
        self.inner.last_task_id.to_string()
    }

    /// Next scheduled run time (ISO 8601 string).
    #[getter]
    fn next_run_at(&self) -> String {
        self.inner.next_run_at.to_rfc3339()
    }

    fn __repr__(&self) -> String {
        format!(
            "ScheduleLastRun(schedule_id='{}', last_run_at='{}', last_task_id='{}')",
            self.inner.schedule_id,
            self.inner.last_run_at.to_rfc3339(),
            self.inner.last_task_id
        )
    }
}
