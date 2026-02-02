//! buquet - S3-Only Task Queue and Workflow Orchestration
//!
//! A minimal, production-ready distributed task queue and workflow orchestration
//! system built entirely on S3-compatible object storage. No databases, no message
//! brokers, no complex infrastructure. Just workers polling S3, claiming tasks
//! atomically via conditional writes, and executing work.

pub mod cli;
pub mod config;
pub mod migration;
pub mod models;
pub mod queue;
pub mod storage;
pub mod web;
pub mod worker;
pub mod workflow;

#[cfg(test)]
mod config_edge_case_tests;

#[cfg(feature = "python")]
pub mod python;

pub use models::{RetryPolicy, Task, TaskBuilder, TaskError, TaskStatus};
pub use queue::{
    connect, ConnectError, ConnectOptions, Queue, QueueConfig, RichError, Schedule, ScheduleError,
    ScheduleLastRun, ScheduleManager, SchemaValidationError, SubmitOptions, TaskOperationError,
    TaskSchema,
};
pub use worker::{
    claim_task, execute_task, ClaimResult, ExecutionResult, HandlerRegistry, IndexMode,
    IndexSweeper, SweepReport, SweeperConfig, TaskHandler, Worker,
};

// Workflow re-exports for convenience
pub use workflow::{
    compute_definition_hash, OnFailure, OrchestrateResult, RecoveryReason, Signal, SignalCursor,
    StepDef, StepState, StepStatus, SweepResult, WorkflowEngine, WorkflowError, WorkflowErrorInfo,
    WorkflowNeedsRecovery, WorkflowRun, WorkflowState, WorkflowStatus, WorkflowSweeper,
};

// Re-export the PyO3 module initialization function for Python builds
#[cfg(feature = "python")]
pub use python::buquet;
