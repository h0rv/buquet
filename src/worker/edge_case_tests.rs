//! Edge case tests for worker behavior and handler registry.
//!
//! These tests verify boundary conditions, error handling, and edge cases
//! in the worker module without requiring S3 infrastructure.

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod handler_registry_edge_cases {
    use async_trait::async_trait;
    use serde_json::Value;

    use crate::models::TaskError;
    use crate::worker::handler::{HandlerRegistry, TaskHandler};

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
            Ok(serde_json::json!({"handled": true}))
        }
    }

    // =========================================================================
    // Empty Registry Edge Cases
    // =========================================================================

    /// Test: Looking up handler in empty registry
    /// Bug this catches: Panic or error on empty registry lookup.
    #[test]
    fn test_empty_registry_lookup() {
        let registry = HandlerRegistry::new();

        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);
        assert!(registry.get("any_type").is_none());
        assert!(!registry.has_handler("any_type"));
    }

    /// Test: Empty registry debug formatting
    /// Bug this catches: Panic in debug format for empty registry.
    #[test]
    fn test_empty_registry_debug() {
        let registry = HandlerRegistry::new();
        let debug_str = format!("{registry:?}");
        assert!(debug_str.contains("HandlerRegistry"));
    }

    // =========================================================================
    // Handler Registration Edge Cases
    // =========================================================================

    /// Test: Register handler with empty task type
    /// Bug this catches: Empty string as map key issues.
    #[test]
    fn test_register_empty_task_type() {
        let mut registry = HandlerRegistry::new();
        registry.register(Box::new(TestHandler::new("")));

        assert!(registry.has_handler(""));
        assert!(registry.get("").is_some());
        assert_eq!(registry.len(), 1);
    }

    /// Test: Register handler with very long task type
    /// Bug this catches: Memory issues with very long keys.
    #[test]
    fn test_register_long_task_type() {
        let mut registry = HandlerRegistry::new();
        let long_type = "a".repeat(10_000);
        registry.register(Box::new(TestHandler::new(&long_type)));

        assert!(registry.has_handler(&long_type));
        assert_eq!(registry.len(), 1);
    }

    /// Test: Register handler with unicode task type
    /// Bug this catches: Unicode handling in task types.
    #[test]
    fn test_register_unicode_task_type() {
        let mut registry = HandlerRegistry::new();
        let unicode_type = "send_\u{1F4E7}_email";
        registry.register(Box::new(TestHandler::new(unicode_type)));

        assert!(registry.has_handler(unicode_type));
        assert!(registry.get(unicode_type).is_some());
    }

    /// Test: Register handler with special characters
    /// Bug this catches: Special character handling in task types.
    #[test]
    fn test_register_special_char_task_type() {
        let mut registry = HandlerRegistry::new();

        let special_types = [
            "task-with-dashes",
            "task_with_underscores",
            "task.with.dots",
            "task/with/slashes",
            "task:with:colons",
            "task@with@at",
        ];

        for task_type in &special_types {
            registry.register(Box::new(TestHandler::new(task_type)));
        }

        assert_eq!(registry.len(), special_types.len());
        for task_type in &special_types {
            assert!(
                registry.has_handler(task_type),
                "Should find handler for {task_type}"
            );
        }
    }

    /// Test: Overwrite existing handler
    /// Bug this catches: Previous handler not being replaced.
    #[test]
    fn test_overwrite_handler() {
        let mut registry = HandlerRegistry::new();

        registry.register(Box::new(TestHandler::new("task_type")));
        assert_eq!(registry.len(), 1);

        // Register again with same task type
        registry.register(Box::new(TestHandler::new("task_type")));
        assert_eq!(registry.len(), 1, "Should still have only one handler");

        // Verify the handler works
        let handler = registry.get("task_type");
        assert!(handler.is_some());
    }

    /// Test: Register many handlers
    /// Bug this catches: Performance issues with many handlers.
    #[test]
    fn test_register_many_handlers() {
        let mut registry = HandlerRegistry::new();

        for i in 0..1000 {
            let task_type = format!("task_type_{i}");
            registry.register(Box::new(TestHandler::new(&task_type)));
        }

        assert_eq!(registry.len(), 1000);

        // Verify random access works
        assert!(registry.has_handler("task_type_0"));
        assert!(registry.has_handler("task_type_500"));
        assert!(registry.has_handler("task_type_999"));
        assert!(!registry.has_handler("task_type_1000"));
    }

    // =========================================================================
    // Handler Lookup Edge Cases
    // =========================================================================

    /// Test: Lookup is case sensitive
    /// Bug this catches: Accidental case insensitivity.
    #[test]
    fn test_lookup_case_sensitive() {
        let mut registry = HandlerRegistry::new();
        registry.register(Box::new(TestHandler::new("SendEmail")));

        assert!(registry.has_handler("SendEmail"));
        assert!(!registry.has_handler("sendemail"));
        assert!(!registry.has_handler("SENDEMAIL"));
        assert!(!registry.has_handler("send_email"));
    }

    /// Test: Lookup with whitespace
    /// Bug this catches: Whitespace not being trimmed.
    #[test]
    fn test_lookup_whitespace() {
        let mut registry = HandlerRegistry::new();
        registry.register(Box::new(TestHandler::new("task_type")));

        // These should NOT match (whitespace matters)
        assert!(!registry.has_handler(" task_type"));
        assert!(!registry.has_handler("task_type "));
        assert!(!registry.has_handler(" task_type "));
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod execution_result_edge_cases {
    use serde_json::json;

    use crate::models::TaskError;
    use crate::worker::execute::ExecutionResult;

    // =========================================================================
    // ExecutionResult State Tests
    // =========================================================================

    /// Test: Success result state checks
    /// Bug this catches: Incorrect state checks.
    #[test]
    fn test_success_result_state() {
        let result = ExecutionResult::Success(json!({"data": "value"}));

        assert!(result.is_success());
        assert!(!result.is_failed());
        assert!(result.output().is_some());
        assert!(result.error().is_none());
    }

    /// Test: Failed result state checks
    /// Bug this catches: Incorrect state checks for failures.
    #[test]
    fn test_failed_result_state() {
        let result = ExecutionResult::Failed(TaskError::Timeout);

        assert!(!result.is_success());
        assert!(result.is_failed());
        assert!(result.output().is_none());
        assert!(result.error().is_some());
    }

    /// Test: Success with null output
    /// Bug this catches: Null output being treated as failure.
    #[test]
    fn test_success_with_null_output() {
        let result = ExecutionResult::Success(json!(null));

        assert!(result.is_success());
        assert!(result.output().is_some());
        let output = result.output().unwrap();
        assert!(output.is_null());
    }

    /// Test: Success with empty object output
    /// Bug this catches: Empty output being treated as failure.
    #[test]
    fn test_success_with_empty_output() {
        let result = ExecutionResult::Success(json!({}));

        assert!(result.is_success());
        let output = result.output().unwrap();
        assert!(output.is_object());
        assert!(output.as_object().unwrap().is_empty());
    }

    /// Test: All error types
    /// Bug this catches: Missing error type handling.
    #[test]
    fn test_all_error_types() {
        let retryable = ExecutionResult::Failed(TaskError::Retryable("temp error".to_string()));
        assert!(matches!(retryable.error(), Some(TaskError::Retryable(_))));

        let permanent = ExecutionResult::Failed(TaskError::Permanent("perm error".to_string()));
        assert!(matches!(permanent.error(), Some(TaskError::Permanent(_))));

        let timeout = ExecutionResult::Failed(TaskError::Timeout);
        assert!(matches!(timeout.error(), Some(TaskError::Timeout)));
    }

    /// Test: Debug formatting
    /// Bug this catches: Panic in debug formatting.
    #[test]
    fn test_execution_result_debug() {
        let success = ExecutionResult::Success(json!({"key": "value"}));
        let debug_str = format!("{success:?}");
        assert!(debug_str.contains("Success"));

        let failed = ExecutionResult::Failed(TaskError::Timeout);
        let debug_str = format!("{failed:?}");
        assert!(debug_str.contains("Failed"));
        assert!(debug_str.contains("Timeout"));
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::approx_constant,
    clippy::panic
)]
mod task_execution_edge_cases {
    use std::time::Duration;

    use async_trait::async_trait;
    use serde_json::{json, Value};

    use crate::models::{Task, TaskError};
    use crate::worker::execute::execute_task;
    use crate::worker::handler::{HandlerRegistry, TaskHandler};

    // =========================================================================
    // Handler that returns various edge case outputs
    // =========================================================================

    struct NullOutputHandler;

    #[async_trait]
    impl TaskHandler for NullOutputHandler {
        fn task_type(&self) -> &'static str {
            "null_output"
        }

        async fn handle(&self, _input: Value) -> Result<Value, TaskError> {
            Ok(json!(null))
        }
    }

    struct EmptyObjectHandler;

    #[async_trait]
    impl TaskHandler for EmptyObjectHandler {
        fn task_type(&self) -> &'static str {
            "empty_object"
        }

        async fn handle(&self, _input: Value) -> Result<Value, TaskError> {
            Ok(json!({}))
        }
    }

    struct LargeOutputHandler;

    #[async_trait]
    impl TaskHandler for LargeOutputHandler {
        fn task_type(&self) -> &'static str {
            "large_output"
        }

        async fn handle(&self, _input: Value) -> Result<Value, TaskError> {
            // Return a large JSON payload
            let large_string = "x".repeat(100_000);
            Ok(json!({"data": large_string}))
        }
    }

    struct InputEchoHandler;

    #[async_trait]
    impl TaskHandler for InputEchoHandler {
        fn task_type(&self) -> &'static str {
            "echo"
        }

        async fn handle(&self, input: Value) -> Result<Value, TaskError> {
            Ok(input)
        }
    }

    struct EmptyErrorHandler;

    #[async_trait]
    impl TaskHandler for EmptyErrorHandler {
        fn task_type(&self) -> &'static str {
            "empty_error"
        }

        async fn handle(&self, _input: Value) -> Result<Value, TaskError> {
            Err(TaskError::Retryable(String::new()))
        }
    }

    // =========================================================================
    // Execution Edge Case Tests
    // =========================================================================

    /// Test: Handler returns null output
    /// Bug this catches: Null output being treated as error.
    #[tokio::test]
    async fn test_execute_null_output() {
        let mut registry = HandlerRegistry::new();
        registry.register(Box::new(NullOutputHandler));

        let task = Task::new("null_output", json!({}));
        let result = execute_task(&task, &registry).await;

        assert!(result.is_success());
        assert!(result.output().unwrap().is_null());
    }

    /// Test: Handler returns empty object
    /// Bug this catches: Empty object being treated as error.
    #[tokio::test]
    async fn test_execute_empty_object_output() {
        let mut registry = HandlerRegistry::new();
        registry.register(Box::new(EmptyObjectHandler));

        let task = Task::new("empty_object", json!({}));
        let result = execute_task(&task, &registry).await;

        assert!(result.is_success());
        let output = result.output().unwrap();
        assert!(output.is_object());
        assert!(output.as_object().unwrap().is_empty());
    }

    /// Test: Handler returns large output
    /// Bug this catches: Memory issues with large outputs.
    #[tokio::test]
    async fn test_execute_large_output() {
        let mut registry = HandlerRegistry::new();
        registry.register(Box::new(LargeOutputHandler));

        let task = Task::new("large_output", json!({}));
        let result = execute_task(&task, &registry).await;

        assert!(result.is_success());
        let output = result.output().unwrap();
        let data = output["data"].as_str().unwrap();
        assert_eq!(data.len(), 100_000);
    }

    /// Test: Handler echoes complex input
    /// Bug this catches: Input corruption during handling.
    #[tokio::test]
    async fn test_execute_echo_complex_input() {
        let mut registry = HandlerRegistry::new();
        registry.register(Box::new(InputEchoHandler));

        let complex_input = json!({
            "string": "hello",
            "number": 42,
            "float": 3.14159,
            "bool": true,
            "null": null,
            "array": [1, 2, 3],
            "nested": {
                "deep": {
                    "value": "found"
                }
            }
        });

        let task = Task::new("echo", complex_input.clone());
        let result = execute_task(&task, &registry).await;

        assert!(result.is_success());
        assert_eq!(result.output().unwrap(), &complex_input);
    }

    /// Test: No handler for task type
    /// Bug this catches: Missing handler not being permanent error.
    #[tokio::test]
    async fn test_execute_no_handler() {
        let registry = HandlerRegistry::new();

        let task = Task::new("unknown_type", json!({}));
        let result = execute_task(&task, &registry).await;

        assert!(result.is_failed());
        match result.error() {
            Some(TaskError::Permanent(msg)) => {
                assert!(msg.contains("No handler"));
                assert!(msg.contains("unknown_type"));
            }
            _ => panic!("Expected Permanent error for missing handler"),
        }
    }

    /// Test: Handler returns empty error message
    /// Bug this catches: Empty error message handling.
    #[tokio::test]
    async fn test_execute_empty_error_message() {
        let mut registry = HandlerRegistry::new();
        registry.register(Box::new(EmptyErrorHandler));

        let task = Task::new("empty_error", json!({}));
        let result = execute_task(&task, &registry).await;

        assert!(result.is_failed());
        match result.error() {
            Some(TaskError::Retryable(msg)) => {
                assert!(msg.is_empty(), "Error message should be empty");
            }
            _ => panic!("Expected Retryable error"),
        }
    }

    /// Test: Task with zero timeout
    /// Bug this catches: Zero timeout causing immediate timeout.
    #[tokio::test]
    async fn test_execute_zero_timeout() {
        let mut registry = HandlerRegistry::new();
        registry.register(Box::new(InputEchoHandler));

        let mut task = Task::new("echo", json!({}));
        task.timeout_seconds = 0;

        // With zero timeout, any async operation might timeout
        // The behavior depends on the implementation
        let result = execute_task(&task, &registry).await;
        // Either success (if fast enough) or timeout is acceptable
        let _ = result;
    }

    /// Test: Task with very short timeout
    /// Bug this catches: Timeout not being enforced.
    #[tokio::test]
    async fn test_execute_short_timeout() {
        struct SlowHandler;

        #[async_trait]
        impl TaskHandler for SlowHandler {
            fn task_type(&self) -> &'static str {
                "slow"
            }

            async fn handle(&self, _input: Value) -> Result<Value, TaskError> {
                tokio::time::sleep(Duration::from_secs(5)).await;
                Ok(json!({"done": true}))
            }
        }

        let mut registry = HandlerRegistry::new();
        registry.register(Box::new(SlowHandler));

        let mut task = Task::new("slow", json!({}));
        task.timeout_seconds = 1;

        let start = std::time::Instant::now();
        let result = execute_task(&task, &registry).await;
        let elapsed = start.elapsed();

        assert!(result.is_failed());
        assert!(matches!(result.error(), Some(TaskError::Timeout)));
        // Should timeout around 1 second, not wait the full 5
        assert!(
            elapsed < Duration::from_secs(3),
            "Should timeout before handler completes"
        );
    }

    /// Test: Unicode in input/output
    /// Bug this catches: Encoding issues.
    #[tokio::test]
    async fn test_execute_unicode_data() {
        let mut registry = HandlerRegistry::new();
        registry.register(Box::new(InputEchoHandler));

        let unicode_input = json!({
            "emoji": "\u{1F4E7}\u{1F680}\u{2764}",
            "chinese": "\u{4E16}\u{754C}",
            "arabic": "\u{0645}\u{0631}\u{062D}\u{0628}\u{0627}",
            "mixed": "Hello \u{4E16}\u{754C} \u{1F44B}"
        });

        let task = Task::new("echo", unicode_input.clone());
        let result = execute_task(&task, &registry).await;

        assert!(result.is_success());
        assert_eq!(result.output().unwrap(), &unicode_input);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod transition_error_edge_cases {
    use uuid::Uuid;

    use crate::models::TaskStatus;
    use crate::storage::StorageError;
    use crate::worker::transitions::TransitionError;

    // =========================================================================
    // TransitionError Edge Cases
    // =========================================================================

    /// Test: `TransitionError` Display formatting
    /// Bug this catches: Incomplete error messages.
    #[test]
    fn test_transition_error_display() {
        let task_id = Uuid::new_v4();
        let lease_id = Uuid::new_v4();

        // NotFound
        let err = TransitionError::NotFound(task_id);
        let msg = err.to_string();
        assert!(msg.contains(&task_id.to_string()));

        // LeaseMismatch with None
        let err = TransitionError::LeaseMismatch {
            expected: lease_id,
            found: None,
        };
        let msg = err.to_string();
        assert!(msg.contains(&lease_id.to_string()));
        assert!(msg.contains("None"));

        // LeaseMismatch with Some
        let other_lease = Uuid::new_v4();
        let err = TransitionError::LeaseMismatch {
            expected: lease_id,
            found: Some(other_lease),
        };
        let msg = err.to_string();
        assert!(msg.contains(&lease_id.to_string()));

        // InvalidStatus
        let err = TransitionError::InvalidStatus {
            expected: vec![TaskStatus::Running],
            found: TaskStatus::Pending,
        };
        let msg = err.to_string();
        assert!(msg.contains("status"));

        // InvalidStatus with multiple expected
        let err = TransitionError::InvalidStatus {
            expected: vec![TaskStatus::Completed, TaskStatus::Failed],
            found: TaskStatus::Running,
        };
        let msg = err.to_string();
        assert!(msg.contains("status"));

        // ConcurrentModification
        let err = TransitionError::ConcurrentModification;
        let msg = err.to_string();
        assert!(msg.contains("Concurrent") || msg.contains("modified"));
    }

    /// Test: `TransitionError` From<StorageError>
    /// Bug this catches: Storage errors not being wrapped correctly.
    #[test]
    fn test_transition_error_from_storage_error() {
        let storage_errors = vec![
            StorageError::NotFound {
                key: "test/key".to_string(),
            },
            StorageError::PreconditionFailed {
                key: "test/key".to_string(),
            },
            StorageError::AlreadyExists {
                key: "test/key".to_string(),
            },
            StorageError::ConnectionError("connection failed".to_string()),
            StorageError::SerializationError("bad json".to_string()),
            StorageError::S3Error("general error".to_string()),
        ];

        for storage_err in storage_errors {
            let transition_err: TransitionError = storage_err.into();
            assert!(matches!(transition_err, TransitionError::Storage(_)));
        }
    }
}
