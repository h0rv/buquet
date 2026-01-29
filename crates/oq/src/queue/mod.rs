//! Queue operations for the task queue system.
//!
//! This module provides the main interface for submitting, retrieving, and
//! managing tasks in the S3-backed distributed task queue.

mod config;
mod error;
mod idempotency;
#[cfg(test)]
mod idempotency_tests;
mod ops;
mod payload;
mod schedule;
mod schema;
#[cfg(test)]
mod schema_edge_case_tests;
#[cfg(test)]
mod schema_tests;

pub use config::QueueConfig;
pub use error::{RichError, TaskOperationError};
pub use idempotency::{
    compute_input_hash, compute_key_hash, idempotency_s3_key, IdempotencyConflictError,
    IdempotencyError, IdempotencyOptions, IdempotencyRecord, IdempotencyResult, IdempotencyScope,
    DEFAULT_IDEMPOTENCY_TTL_DAYS,
};
pub use ops::{Queue, SubmitOptions};
pub use payload::{
    load_payload, should_offload_payload, store_payload, PayloadRef, PayloadType,
    DEFAULT_PAYLOAD_REF_THRESHOLD_BYTES,
};
pub use schedule::{
    run_scheduler_tick, Schedule, ScheduleError, ScheduleLastRun, ScheduleManager, SchedulerRun,
    SchedulerTickError, SchedulerTickReport,
};
pub use schema::{SchemaValidationError, TaskSchema};
