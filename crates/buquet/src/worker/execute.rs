//! Task execution with timeout handling.
//!
//! This module handles the actual execution of tasks using the appropriate
//! handler from the registry, with timeout enforcement.

use std::time::{Duration, Instant};

use metrics::{counter, histogram};
use serde_json::Value;
use tokio::time::timeout;

use crate::models::{Task, TaskError};
use crate::queue::Queue;
use crate::worker::{HandlerRegistry, TaskContext};

/// Result of task execution.
///
/// This enum captures the outcome of executing a task handler.
#[derive(Debug)]
pub enum ExecutionResult {
    /// Task completed successfully with the given output.
    Success(Value),
    /// Task failed with the given error.
    ///
    /// The error indicates whether the failure is retryable or permanent.
    Failed(TaskError),
}

impl ExecutionResult {
    /// Returns true if the execution was successful.
    #[must_use]
    pub const fn is_success(&self) -> bool {
        matches!(self, Self::Success(_))
    }

    /// Returns true if the execution failed.
    #[must_use]
    pub const fn is_failed(&self) -> bool {
        matches!(self, Self::Failed(_))
    }

    /// Returns the output value if successful, None otherwise.
    #[must_use]
    pub const fn output(&self) -> Option<&Value> {
        match self {
            Self::Success(v) => Some(v),
            Self::Failed(_) => None,
        }
    }

    /// Returns the error if failed, None otherwise.
    #[must_use]
    pub const fn error(&self) -> Option<&TaskError> {
        match self {
            Self::Success(_) => None,
            Self::Failed(e) => Some(e),
        }
    }
}

/// Executes a task using the appropriate handler from the registry.
///
/// This function:
/// 1. Looks up the handler for the task's type
/// 2. Executes the handler with timeout enforcement
/// 3. Returns the result (success or failure)
///
/// # Arguments
///
/// * `task` - The task to execute
/// * `handlers` - The registry of task handlers
///
/// # Returns
///
/// * `ExecutionResult::Success(output)` - Task completed successfully
/// * `ExecutionResult::Failed(TaskError::Permanent(_))` - No handler found
/// * `ExecutionResult::Failed(TaskError::Timeout)` - Handler timed out
/// * `ExecutionResult::Failed(error)` - Handler returned an error
///
/// # Example
///
/// ```ignore
/// let result = execute_task(&task, &handlers).await;
/// match result {
///     ExecutionResult::Success(output) => println!("Task completed: {:?}", output),
///     ExecutionResult::Failed(error) => println!("Task failed: {:?}", error),
/// }
/// ```
pub async fn execute_task(task: &Task, handlers: &HandlerRegistry) -> ExecutionResult {
    // Find the appropriate handler for this task type
    let Some(handler) = handlers.get(&task.task_type) else {
        counter!("buquet.tasks.failed", "task_type" => task.task_type.clone(), "reason" => "no_handler")
            .increment(1);
        return ExecutionResult::Failed(TaskError::Permanent(format!(
            "No handler registered for task type: {}",
            task.task_type
        )));
    };

    // Execute with timeout
    let timeout_duration = Duration::from_secs(task.timeout_seconds);
    let start = Instant::now();

    let result = match timeout(timeout_duration, handler.handle(task.input.clone())).await {
        // Handler completed within timeout
        Ok(Ok(output)) => ExecutionResult::Success(output),
        // Handler returned an error
        Ok(Err(e)) => ExecutionResult::Failed(e),
        // Handler timed out
        Err(_elapsed) => ExecutionResult::Failed(TaskError::Timeout),
    };

    let duration = start.elapsed();
    let task_type = task.task_type.clone();

    // Record metrics
    histogram!("buquet.task.duration_seconds", "task_type" => task_type.clone())
        .record(duration.as_secs_f64());

    match &result {
        ExecutionResult::Success(_) => {
            counter!("buquet.tasks.completed", "task_type" => task_type).increment(1);
        }
        ExecutionResult::Failed(TaskError::Timeout) => {
            counter!("buquet.tasks.failed", "task_type" => task_type, "reason" => "timeout")
                .increment(1);
        }
        ExecutionResult::Failed(TaskError::Retryable(_)) => {
            counter!("buquet.tasks.failed", "task_type" => task_type, "reason" => "retryable")
                .increment(1);
        }
        ExecutionResult::Failed(TaskError::Permanent(_)) => {
            counter!("buquet.tasks.failed", "task_type" => task_type, "reason" => "permanent")
                .increment(1);
        }
        ExecutionResult::Failed(TaskError::Reschedule(delay)) => {
            counter!("buquet.tasks.rescheduled", "task_type" => task_type).increment(1);
            #[allow(clippy::cast_precision_loss)]
            let delay_f64 = *delay as f64;
            histogram!("buquet.reschedule.delay_seconds", "task_type" => task.task_type.clone())
                .record(delay_f64);
        }
    }

    result
}

/// Executes a task with a `TaskContext` for lease extension support.
///
/// This function is similar to `execute_task` but provides a `TaskContext`
/// that handlers can use to extend their lease during long-running operations.
///
/// # Arguments
///
/// * `queue` - The queue for making lease extension calls
/// * `task` - The task to execute
/// * `etag` - The `ETag` from when the task was claimed
/// * `handlers` - The registry of task handlers
///
/// # Returns
///
/// * `ExecutionResult::Success(output)` - Task completed successfully
/// * `ExecutionResult::Failed(error)` - Task failed
/// * The updated `ETag` after any lease extensions
///
/// # Example
///
/// ```ignore
/// let (result, new_etag) = execute_task_with_context(&queue, &task, &etag, &handlers).await;
/// ```
pub async fn execute_task_with_context(
    queue: &Queue,
    task: &Task,
    etag: &str,
    handlers: &HandlerRegistry,
) -> (ExecutionResult, String) {
    // Create TaskContext for lease extension
    let ctx = TaskContext::new(queue.clone(), task.clone(), etag.to_string());

    // Find the appropriate handler for this task type
    let Some(handler) = handlers.get(&task.task_type) else {
        counter!("buquet.tasks.failed", "task_type" => task.task_type.clone(), "reason" => "no_handler")
            .increment(1);
        return (
            ExecutionResult::Failed(TaskError::Permanent(format!(
                "No handler registered for task type: {}",
                task.task_type
            ))),
            etag.to_string(),
        );
    };

    // Execute with timeout, passing context to handler
    let timeout_duration = Duration::from_secs(task.timeout_seconds);
    let start = Instant::now();

    let result = match timeout(
        timeout_duration,
        handler.handle_with_context(task.input.clone(), &ctx),
    )
    .await
    {
        // Handler completed within timeout
        Ok(Ok(output)) => ExecutionResult::Success(output),
        // Handler returned an error
        Ok(Err(e)) => ExecutionResult::Failed(e),
        // Handler timed out
        Err(_elapsed) => ExecutionResult::Failed(TaskError::Timeout),
    };

    let duration = start.elapsed();
    let task_type = task.task_type.clone();

    // Record metrics
    histogram!("buquet.task.duration_seconds", "task_type" => task_type.clone())
        .record(duration.as_secs_f64());

    match &result {
        ExecutionResult::Success(_) => {
            counter!("buquet.tasks.completed", "task_type" => task_type).increment(1);
        }
        ExecutionResult::Failed(TaskError::Timeout) => {
            counter!("buquet.tasks.failed", "task_type" => task_type, "reason" => "timeout")
                .increment(1);
        }
        ExecutionResult::Failed(TaskError::Retryable(_)) => {
            counter!("buquet.tasks.failed", "task_type" => task_type, "reason" => "retryable")
                .increment(1);
        }
        ExecutionResult::Failed(TaskError::Permanent(_)) => {
            counter!("buquet.tasks.failed", "task_type" => task_type, "reason" => "permanent")
                .increment(1);
        }
        ExecutionResult::Failed(TaskError::Reschedule(delay)) => {
            counter!("buquet.tasks.rescheduled", "task_type" => task_type).increment(1);
            #[allow(clippy::cast_precision_loss)]
            let delay_f64 = *delay as f64;
            histogram!("buquet.reschedule.delay_seconds", "task_type" => task.task_type.clone())
                .record(delay_f64);
        }
    }

    // Get the final etag after any lease extensions
    let final_etag = ctx.etag().await;

    (result, final_etag)
}

/// Returns a `TaskContext` for a task, allowing handlers to extend leases.
///
/// This is useful when you want to pass the context to a handler that needs
/// lease extension capability.
///
/// # Arguments
///
/// * `queue` - The queue for making lease extension calls
/// * `task` - The task being executed
/// * `etag` - The `ETag` from when the task was claimed
///
/// # Returns
///
/// A `TaskContext` that can be used to extend the lease.
#[must_use]
pub fn create_task_context(queue: &Queue, task: &Task, etag: &str) -> TaskContext {
    TaskContext::new(queue.clone(), task.clone(), etag.to_string())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::worker::TaskHandler;
    use async_trait::async_trait;

    struct SuccessHandler;

    #[async_trait]
    impl TaskHandler for SuccessHandler {
        fn task_type(&self) -> &str {
            "success"
        }

        async fn handle(&self, _input: Value) -> Result<Value, TaskError> {
            Ok(serde_json::json!({"result": "ok"}))
        }
    }

    struct FailHandler;

    #[async_trait]
    impl TaskHandler for FailHandler {
        fn task_type(&self) -> &str {
            "fail"
        }

        async fn handle(&self, _input: Value) -> Result<Value, TaskError> {
            Err(TaskError::Retryable("temporary failure".to_string()))
        }
    }

    struct PermanentFailHandler;

    #[async_trait]
    impl TaskHandler for PermanentFailHandler {
        fn task_type(&self) -> &str {
            "permanent_fail"
        }

        async fn handle(&self, _input: Value) -> Result<Value, TaskError> {
            Err(TaskError::Permanent("permanent failure".to_string()))
        }
    }

    struct SlowHandler;

    #[async_trait]
    impl TaskHandler for SlowHandler {
        fn task_type(&self) -> &str {
            "slow"
        }

        async fn handle(&self, _input: Value) -> Result<Value, TaskError> {
            tokio::time::sleep(Duration::from_secs(10)).await;
            Ok(serde_json::json!({"result": "completed"}))
        }
    }

    #[tokio::test]
    async fn test_execute_success() {
        let mut registry = HandlerRegistry::new();
        registry.register(Box::new(SuccessHandler));

        let task = Task::new("success", serde_json::json!({}));
        let result = execute_task(&task, &registry).await;

        assert!(result.is_success());
        assert!(!result.is_failed());
        assert!(result.output().is_some());
        assert!(result.error().is_none());
    }

    #[tokio::test]
    async fn test_execute_retryable_failure() {
        let mut registry = HandlerRegistry::new();
        registry.register(Box::new(FailHandler));

        let task = Task::new("fail", serde_json::json!({}));
        let result = execute_task(&task, &registry).await;

        assert!(result.is_failed());
        assert!(!result.is_success());
        assert!(result.output().is_none());
        match result.error() {
            Some(TaskError::Retryable(msg)) => assert!(msg.contains("temporary")),
            _ => panic!("Expected retryable error"),
        }
    }

    #[tokio::test]
    async fn test_execute_permanent_failure() {
        let mut registry = HandlerRegistry::new();
        registry.register(Box::new(PermanentFailHandler));

        let task = Task::new("permanent_fail", serde_json::json!({}));
        let result = execute_task(&task, &registry).await;

        assert!(result.is_failed());
        match result.error() {
            Some(TaskError::Permanent(msg)) => assert!(msg.contains("permanent")),
            _ => panic!("Expected permanent error"),
        }
    }

    #[tokio::test]
    async fn test_execute_no_handler() {
        let registry = HandlerRegistry::new();

        let task = Task::new("unknown", serde_json::json!({}));
        let result = execute_task(&task, &registry).await;

        assert!(result.is_failed());
        match result.error() {
            Some(TaskError::Permanent(msg)) => assert!(msg.contains("No handler")),
            _ => panic!("Expected permanent error for missing handler"),
        }
    }

    #[tokio::test]
    async fn test_execute_timeout() {
        let mut registry = HandlerRegistry::new();
        registry.register(Box::new(SlowHandler));

        let mut task = Task::new("slow", serde_json::json!({}));
        task.timeout_seconds = 1; // 1 second timeout

        let result = execute_task(&task, &registry).await;

        assert!(result.is_failed());
        match result.error() {
            Some(TaskError::Timeout) => {}
            _ => panic!("Expected timeout error"),
        }
    }

    #[test]
    fn test_execution_result_debug() {
        let success = ExecutionResult::Success(serde_json::json!({"ok": true}));
        let debug_str = format!("{:?}", success);
        assert!(debug_str.contains("Success"));

        let failed = ExecutionResult::Failed(TaskError::Timeout);
        let debug_str = format!("{:?}", failed);
        assert!(debug_str.contains("Failed"));
    }
}
