//! Data models for the dex task queue.
//!
//! This module contains the core data structures used throughout the task queue:
//! - `Task` - Represents a task in the queue
//! - `TaskStatus` - The current state of a task
//! - `TaskError` - Errors that can occur during task execution
//! - `RetryPolicy` - Configuration for exponential backoff with jitter
//! - `WorkerInfo` - Information about a registered worker

/// Retry policy configuration.
pub mod retry;
/// Task model and related types.
pub mod task;
pub mod worker;

#[cfg(test)]
mod edge_case_tests;

pub use retry::RetryPolicy;
pub use task::{Task, TaskBuilder, TaskError, TaskStatus};
pub use worker::WorkerInfo;
