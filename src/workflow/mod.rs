//! Workflow orchestration layer for buquet.
//!
//! This module provides durable workflow orchestration on top of the buquet task queue,
//! using S3 as the sole storage backend.

pub mod dag;
pub mod engine;
pub mod error;
pub mod signals;
pub mod state;
pub mod sweeper;
pub mod types;

#[cfg(feature = "python")]
pub mod python;

pub use engine::{OrchestrateResult, WorkflowEngine};
pub use error::{Result, WorkflowError};
pub use sweeper::{RecoveryReason, SweepResult, WorkflowNeedsRecovery, WorkflowSweeper};
pub use types::{
    compute_definition_hash, OnFailure, Signal, SignalCursor, StepDef, StepState, StepStatus,
    WorkflowErrorInfo, WorkflowRun, WorkflowState, WorkflowStatus,
};
