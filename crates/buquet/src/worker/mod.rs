//! Worker module for task claiming and execution.
//!
//! This module provides the core worker functionality for the S3-based task queue:
//! - `Worker` - The main worker struct that polls for and executes tasks
//! - `TaskHandler` - Trait for implementing task handlers
//! - `HandlerRegistry` - Registry for task handlers
//! - `ClaimResult` - Result type for claim operations
//! - `ExecutionResult` - Result type for task execution
//! - `TimeoutMonitor` - Background monitor for detecting and recovering timed-out tasks
//! - `MonitorConfig` - Configuration for the timeout monitor
//! - `IndexSweeper` - Background sweeper for repairing ready/lease indexes
//! - `SweeperConfig` - Configuration for the index sweeper
//! - `SweepReport` - Summary of sweep actions
//! - `PollingStrategy` - Configurable polling strategy (fixed or adaptive)
//! - `RunnerConfig` - Configuration for the worker runner loop
//! - `shutdown_signal` - Creates a shutdown signal channel
//! - `wait_for_shutdown_signal` - Waits for SIGTERM/SIGINT
//! - `ShardLease` - A shard ownership lease stored in S3
//! - `ShardLeaseConfig` - Configuration for shard leasing
//! - `ShardLeaseManager` - Manager for acquiring and maintaining shard leases

mod claim;
mod execute;
mod handler;
mod monitor;
mod polling;
mod runner;
mod shard_lease;
mod shard_lease_manager;
mod sweeper;
mod transitions;

#[cfg(test)]
mod edge_case_tests;
#[cfg(test)]
mod lease_extension_tests;
#[cfg(test)]
mod shard_lease_tests;

pub use claim::{claim_task, try_claim_for_handler, ClaimResult};
pub use execute::{create_task_context, execute_task, execute_task_with_context, ExecutionResult};
pub use handler::{HandlerRegistry, TaskContext, TaskHandler};
pub use monitor::{MonitorConfig, ShardCursor, TimeoutMonitor};
pub use polling::PollingStrategy;
pub use runner::{shutdown_signal, wait_for_shutdown_signal, IndexMode, RunnerConfig};
pub use shard_lease::{ShardLease, ShardLeaseConfig};
pub use shard_lease_manager::ShardLeaseManager;
pub use sweeper::{IndexSweeper, SweepMode, SweepReport, SweeperConfig, SweeperCursor};
pub use transitions::{
    archive_task, complete_task, complete_task_with_options, fail_task, replay_task,
    reschedule_task, retry_task, CompleteOptions, TransitionError,
};

use crate::models::WorkerInfo;
use crate::queue::Queue;
use std::collections::HashMap;

/// A worker that polls for and executes tasks.
///
/// The worker is responsible for:
/// - Polling S3 for ready tasks
/// - Claiming tasks atomically using conditional writes
/// - Executing tasks using registered handlers
/// - Updating task state based on execution results
#[derive(Debug)]
pub struct Worker {
    /// Queue for task operations.
    queue: Queue,
    /// This worker's info.
    info: WorkerInfo,
    /// Registered task handlers.
    handlers: HandlerRegistry,
    /// Bucket cursor + continuation token for ready index pagination per shard.
    ready_cursors: HashMap<String, ReadyBucketCursor>,
    /// Continuation tokens for task log pagination per shard.
    task_cursors: HashMap<String, Option<String>>,
    /// Starting shard cursor for fair polling.
    shard_cursor: usize,
}

#[derive(Debug, Clone, Default)]
struct ReadyBucketCursor {
    bucket: i64,
    token: Option<String>,
    initialized: bool,
}

impl Worker {
    /// Creates a new worker.
    ///
    /// # Arguments
    ///
    /// * `queue` - The queue to poll for tasks
    /// * `worker_id` - A unique identifier for this worker
    /// * `shards` - The shards this worker will poll (e.g., `["0", "1", "a", "b"]`)
    ///
    /// # Example
    ///
    /// ```ignore
    /// let worker = Worker::new(queue, "worker-1", vec!["0".to_string(), "1".to_string()]);
    /// ```
    pub fn new(queue: Queue, worker_id: impl Into<String>, shards: Vec<String>) -> Self {
        let mut ready_cursors = HashMap::new();
        let mut task_cursors = HashMap::new();
        for shard in &shards {
            ready_cursors.insert(shard.clone(), ReadyBucketCursor::default());
            task_cursors.insert(shard.clone(), None);
        }

        Self {
            queue,
            info: WorkerInfo::new(worker_id, shards),
            handlers: HandlerRegistry::new(),
            ready_cursors,
            task_cursors,
            shard_cursor: 0,
        }
    }

    /// Registers a task handler for processing tasks of a specific type.
    ///
    /// # Arguments
    ///
    /// * `handler` - The handler to register
    ///
    /// # Example
    ///
    /// ```ignore
    /// worker.register_handler(Box::new(MyTaskHandler));
    /// ```
    pub fn register_handler(&mut self, handler: Box<dyn TaskHandler>) {
        self.handlers.register(handler);
    }

    /// Returns the worker's unique identifier.
    #[must_use]
    pub fn worker_id(&self) -> &str {
        &self.info.worker_id
    }

    /// Returns the worker's info.
    #[must_use]
    pub const fn info(&self) -> &WorkerInfo {
        &self.info
    }

    /// Returns a mutable reference to the worker's info.
    ///
    /// This is useful for updating heartbeat timestamps and task counters.
    pub const fn info_mut(&mut self) -> &mut WorkerInfo {
        &mut self.info
    }

    /// Returns a reference to the queue.
    #[must_use]
    pub const fn queue(&self) -> &Queue {
        &self.queue
    }

    /// Returns a reference to the handler registry.
    #[must_use]
    pub const fn handlers(&self) -> &HandlerRegistry {
        &self.handlers
    }

    /// Returns the shards this worker is polling.
    #[must_use]
    pub fn shards(&self) -> &[String] {
        &self.info.shards
    }
}
