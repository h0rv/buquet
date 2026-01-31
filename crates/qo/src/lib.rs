//! qo - S3-Only Task Queue
//!
//! A minimal, production-ready distributed task queue built entirely on
//! S3-compatible object storage. No databases, no message brokers, no complex
//! infrastructure. Just workers polling S3, claiming tasks atomically via
//! conditional writes, and executing work.

pub mod cli;
pub mod config;
pub mod migration;
pub mod models;
pub mod queue;
pub mod storage;
pub mod web;
pub mod worker;

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

// Re-export the PyO3 module initialization function for Python builds
#[cfg(feature = "python")]
pub use python::qo;
