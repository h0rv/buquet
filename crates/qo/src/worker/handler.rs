//! Task handler traits and registry.
//!
//! This module provides the infrastructure for implementing and registering
//! task handlers that process different types of tasks.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::models::{Task, TaskError};
use crate::queue::{Queue, TaskOperationError};

/// Trait for task handlers.
///
/// Implement this trait to create a handler for a specific task type.
/// Handlers are async and should return the output on success or a `TaskError`
/// indicating whether the error is retryable.
///
/// # Basic Example
///
/// ```ignore
/// use async_trait::async_trait;
/// use serde_json::Value;
/// use qo::models::TaskError;
/// use qo::worker::TaskHandler;
///
/// struct SendEmailHandler;
///
/// #[async_trait]
/// impl TaskHandler for SendEmailHandler {
///     fn task_type(&self) -> &str {
///         "send_email"
///     }
///
///     async fn handle(&self, input: Value) -> Result<Value, TaskError> {
///         // Process the task...
///         Ok(serde_json::json!({"status": "sent"}))
///     }
/// }
/// ```
///
/// # Example with Context
///
/// For long-running tasks, override `handle_with_context` to access lease
/// extension and cancellation checking:
///
/// ```ignore
/// use async_trait::async_trait;
/// use serde_json::Value;
/// use qo::models::TaskError;
/// use qo::worker::{TaskContext, TaskHandler};
///
/// struct LongRunningHandler;
///
/// #[async_trait]
/// impl TaskHandler for LongRunningHandler {
///     fn task_type(&self) -> &str {
///         "long_running"
///     }
///
///     async fn handle(&self, _input: Value) -> Result<Value, TaskError> {
///         // This is never called when handle_with_context is overridden,
///         // but we provide a safe implementation just in case.
///         Err(TaskError::Permanent("Use handle_with_context".to_string()))
///     }
///
///     async fn handle_with_context(
///         &self,
///         input: Value,
///         ctx: &TaskContext,
///     ) -> Result<Value, TaskError> {
///         for chunk in get_chunks(&input) {
///             process_chunk(chunk).await;
///
///             // Extend lease for long-running work
///             ctx.extend_lease_secs(60).await
///                 .map_err(|e| TaskError::Retryable(e.to_string()))?;
///
///             // Check for cancellation
///             ctx.refresh().await.ok();
///             if ctx.is_cancellation_requested().await {
///                 return Ok(serde_json::json!({"status": "cancelled"}));
///             }
///         }
///         Ok(serde_json::json!({"status": "done"}))
///     }
/// }
/// ```
#[async_trait]
pub trait TaskHandler: Send + Sync {
    /// Returns the task type this handler processes.
    ///
    /// This string must match the `task_type` field on tasks that should
    /// be processed by this handler.
    fn task_type(&self) -> &str;

    /// Handles the task, returning the output on success.
    ///
    /// For simple handlers that don't need lease extension or cancellation
    /// checking, implement this method. For long-running tasks, implement
    /// [`handle_with_context`](Self::handle_with_context) instead.
    ///
    /// # Arguments
    ///
    /// * `input` - The task's input data as a JSON value
    ///
    /// # Returns
    ///
    /// * `Ok(Value)` - The output data on success
    /// * `Err(TaskError::Retryable(_))` - A temporary error that can be retried
    /// * `Err(TaskError::Permanent(_))` - A permanent error that should not be retried
    /// * `Err(TaskError::Timeout)` - The task timed out (usually set by the executor)
    ///
    /// # Errors
    ///
    /// Returns `TaskError` if the task processing fails.
    async fn handle(&self, input: Value) -> Result<Value, TaskError>;

    /// Handles the task with access to the task context.
    ///
    /// Override this method for long-running tasks that need to extend their
    /// lease or check for cancellation requests.
    ///
    /// The default implementation ignores the context and calls [`handle`](Self::handle).
    ///
    /// # Arguments
    ///
    /// * `input` - The task's input data as a JSON value
    /// * `ctx` - The task context for lease extension and cancellation checking
    ///
    /// # Errors
    ///
    /// Returns `TaskError` if the task processing fails.
    async fn handle_with_context(
        &self,
        input: Value,
        _ctx: &TaskContext,
    ) -> Result<Value, TaskError> {
        // Default: ignore context and delegate to handle()
        self.handle(input).await
    }
}

/// Registry of task handlers.
///
/// The registry maps task types to their handlers, allowing the worker
/// to dispatch tasks to the appropriate handler based on their type.
#[derive(Default)]
pub struct HandlerRegistry {
    handlers: HashMap<String, Box<dyn TaskHandler>>,
}

impl HandlerRegistry {
    /// Creates a new empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a handler for a task type.
    ///
    /// If a handler is already registered for the task type, it will be replaced.
    ///
    /// # Arguments
    ///
    /// * `handler` - The handler to register
    pub fn register(&mut self, handler: Box<dyn TaskHandler>) {
        self.handlers
            .insert(handler.task_type().to_string(), handler);
    }

    /// Gets a handler for the given task type.
    ///
    /// # Arguments
    ///
    /// * `task_type` - The task type to look up
    ///
    /// # Returns
    ///
    /// `Some(&dyn TaskHandler)` if a handler is registered, `None` otherwise.
    #[must_use]
    pub fn get(&self, task_type: &str) -> Option<&dyn TaskHandler> {
        self.handlers.get(task_type).map(AsRef::as_ref)
    }

    /// Returns true if a handler is registered for the task type.
    ///
    /// # Arguments
    ///
    /// * `task_type` - The task type to check
    #[must_use]
    pub fn has_handler(&self, task_type: &str) -> bool {
        self.handlers.contains_key(task_type)
    }

    /// Returns the number of registered handlers.
    #[must_use]
    pub fn len(&self) -> usize {
        self.handlers.len()
    }

    /// Returns true if no handlers are registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.handlers.is_empty()
    }
}

// Implement Debug for HandlerRegistry
impl std::fmt::Debug for HandlerRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let task_types: Vec<&str> = self.handlers.keys().map(String::as_str).collect();
        f.debug_struct("HandlerRegistry")
            .field("handlers", &task_types)
            .finish()
    }
}

// Implement Debug for dyn TaskHandler
impl std::fmt::Debug for dyn TaskHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TaskHandler({})", self.task_type())
    }
}

/// Context for a running task that allows lease extension.
///
/// This struct is provided to handlers that need to extend their lease
/// for long-running operations. It encapsulates the queue reference,
/// task state, and current etag needed to safely extend the lease.
///
/// # Example
///
/// ```ignore
/// async fn process_long_task(ctx: &TaskContext, input: Value) -> Result<Value, TaskError> {
///     for chunk in chunks {
///         process_chunk(chunk).await?;
///         // Extend lease every chunk to prevent timeout
///         ctx.extend_lease(Duration::from_secs(60)).await?;
///     }
///     Ok(json!({"status": "done"}))
/// }
/// ```
#[derive(Debug)]
pub struct TaskContext {
    queue: Queue,
    task_id: Uuid,
    /// The current task state (updated after each extension).
    task: Arc<RwLock<Task>>,
    /// The current `ETag` (updated after each extension).
    etag: Arc<RwLock<String>>,
}

impl TaskContext {
    /// Creates a new `TaskContext`.
    ///
    /// # Arguments
    ///
    /// * `queue` - The queue for making lease extension calls
    /// * `task` - The current task being executed
    /// * `etag` - The `ETag` from when the task was claimed
    pub fn new(queue: Queue, task: Task, etag: String) -> Self {
        Self {
            queue,
            task_id: task.id,
            task: Arc::new(RwLock::new(task)),
            etag: Arc::new(RwLock::new(etag)),
        }
    }

    /// Returns the task ID.
    #[must_use]
    pub const fn task_id(&self) -> Uuid {
        self.task_id
    }

    /// Returns a clone of the current task state.
    pub async fn task(&self) -> Task {
        self.task.read().await.clone()
    }

    /// Returns the current `ETag`.
    pub async fn etag(&self) -> String {
        self.etag.read().await.clone()
    }

    /// Extends the lease on the current task.
    ///
    /// This should be called periodically during long-running task execution
    /// to prevent the timeout monitor from reclaiming the task.
    ///
    /// # Arguments
    ///
    /// * `additional_time` - How much time to add to the lease
    ///
    /// # Returns
    ///
    /// `Ok(())` if the lease was successfully extended.
    ///
    /// # Errors
    ///
    /// Returns `TaskOperationError::LeaseExtensionFailed` if:
    /// - Another worker modified the task (CAS failed)
    /// - The lease has already expired and another worker claimed the task
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Extend lease by 60 seconds
    /// ctx.extend_lease(Duration::from_secs(60)).await?;
    /// ```
    pub async fn extend_lease(&self, additional_time: Duration) -> Result<(), TaskOperationError> {
        let task = self.task.read().await.clone();
        let etag = self.etag.read().await.clone();

        let (updated_task, new_etag) = self
            .queue
            .extend_lease(&task, &etag, additional_time)
            .await?;

        // Update internal state
        *self.task.write().await = updated_task;
        *self.etag.write().await = new_etag;

        tracing::debug!(
            task_id = %self.task_id,
            additional_secs = additional_time.as_secs(),
            "Extended task lease"
        );

        Ok(())
    }

    /// Convenience method to extend the lease by a number of seconds.
    ///
    /// # Arguments
    ///
    /// * `additional_secs` - How many seconds to add to the lease
    ///
    /// # Errors
    ///
    /// Returns `TaskOperationError::LeaseExtensionFailed` if extension fails.
    pub async fn extend_lease_secs(&self, additional_secs: u64) -> Result<(), TaskOperationError> {
        self.extend_lease(Duration::from_secs(additional_secs))
            .await
    }

    /// Refreshes the task state from S3.
    ///
    /// Call this to get the latest task state, including any cancellation
    /// requests that may have been made since the task was claimed.
    ///
    /// # Errors
    ///
    /// Returns an error if the task cannot be fetched from S3.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Refresh before checking cancellation
    /// ctx.refresh().await?;
    /// if ctx.is_cancellation_requested().await {
    ///     return Ok(json!({"status": "cancelled"}));
    /// }
    /// ```
    pub async fn refresh(&self) -> Result<(), TaskOperationError> {
        let (task, etag) = self
            .queue
            .get(self.task_id)
            .await
            .map_err(TaskOperationError::Storage)?
            .ok_or_else(|| TaskOperationError::not_found(self.task_id))?;

        *self.task.write().await = task;
        *self.etag.write().await = etag;

        Ok(())
    }

    /// Check if cancellation has been requested for this task.
    ///
    /// This reads from the cached task state. For real-time cancellation
    /// detection, call [`refresh()`](Self::refresh) first to fetch the
    /// latest state from S3.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Check cached state (fast, may be stale)
    /// if ctx.is_cancellation_requested().await {
    ///     return Ok(json!({"status": "cancelled"}));
    /// }
    ///
    /// // Or refresh first for latest state (slower, always fresh)
    /// ctx.refresh().await?;
    /// if ctx.is_cancellation_requested().await {
    ///     return Ok(json!({"status": "cancelled"}));
    /// }
    /// ```
    pub async fn is_cancellation_requested(&self) -> bool {
        self.task.read().await.cancel_requested
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestHandler {
        task_type: String,
    }

    impl TestHandler {
        fn new(task_type: &str) -> Self {
            Self {
                task_type: task_type.to_string(),
            }
        }
    }

    #[async_trait]
    impl TaskHandler for TestHandler {
        fn task_type(&self) -> &str {
            &self.task_type
        }

        async fn handle(&self, _input: Value) -> Result<Value, TaskError> {
            Ok(serde_json::json!({"processed": true}))
        }
    }

    #[test]
    fn test_registry_new_is_empty() {
        let registry = HandlerRegistry::new();
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);
    }

    #[test]
    fn test_registry_register_and_get() {
        let mut registry = HandlerRegistry::new();
        registry.register(Box::new(TestHandler::new("send_email")));

        assert!(!registry.is_empty());
        assert_eq!(registry.len(), 1);
        assert!(registry.has_handler("send_email"));
        assert!(registry.get("send_email").is_some());
        assert!(!registry.has_handler("unknown"));
        assert!(registry.get("unknown").is_none());
    }

    #[test]
    fn test_registry_replace_handler() {
        let mut registry = HandlerRegistry::new();
        registry.register(Box::new(TestHandler::new("send_email")));
        registry.register(Box::new(TestHandler::new("send_email")));

        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn test_registry_multiple_handlers() {
        let mut registry = HandlerRegistry::new();
        registry.register(Box::new(TestHandler::new("send_email")));
        registry.register(Box::new(TestHandler::new("process_image")));
        registry.register(Box::new(TestHandler::new("generate_report")));

        assert_eq!(registry.len(), 3);
        assert!(registry.has_handler("send_email"));
        assert!(registry.has_handler("process_image"));
        assert!(registry.has_handler("generate_report"));
    }

    #[test]
    fn test_registry_debug() {
        let mut registry = HandlerRegistry::new();
        registry.register(Box::new(TestHandler::new("test")));
        let debug_str = format!("{:?}", registry);
        assert!(debug_str.contains("HandlerRegistry"));
    }

    #[tokio::test]
    async fn test_handler_handle() {
        let handler = TestHandler::new("test");
        let result = handler.handle(serde_json::json!({})).await;
        assert!(result.is_ok());
    }
}
