//! Integration tests for idempotency keys feature.
//!
//! Tests for preventing duplicate task creation using idempotency keys.

use oq::queue::{IdempotencyScope, SubmitOptions};
use serde_json::json;

use crate::common::{test_queue, unique_task_type};

// ============================================================================
// Basic idempotency tests
// ============================================================================

/// Test that submitting with the same idempotency_key returns the same task.
#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_same_idempotency_key_returns_same_task() {
    let queue = test_queue().await;
    let task_type = unique_task_type();
    let idempotency_key = format!("test-key-{}", uuid::Uuid::new_v4());

    let options = SubmitOptions {
        idempotency_key: Some(idempotency_key.clone()),
        idempotency_ttl_days: Some(30),
        ..Default::default()
    };

    // First submission
    let task1 = queue
        .submit(&task_type, json!({"order_id": "ORD-123"}), options.clone())
        .await
        .expect("Failed to submit first task");

    // Second submission with same key
    let task2 = queue
        .submit(&task_type, json!({"order_id": "ORD-123"}), options.clone())
        .await
        .expect("Failed to submit second task");

    // Should return the same task
    assert_eq!(
        task1.id, task2.id,
        "Same idempotency key should return the same task ID"
    );
    assert_eq!(task1.task_type, task2.task_type);
    assert_eq!(task1.input, task2.input);
}

/// BUG-013: If the idempotency record exists but the task is missing, create a new task.
#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_idempotency_missing_task_creates_new_task() {
    let queue = test_queue().await;
    let task_type = unique_task_type();
    let idempotency_key = format!("test-key-{}", uuid::Uuid::new_v4());
    let input = json!({"order_id": "ORD-123"});

    let options = SubmitOptions {
        idempotency_key: Some(idempotency_key.clone()),
        idempotency_ttl_days: Some(30),
        ..Default::default()
    };

    // First submission
    let task1 = queue
        .submit(&task_type, input.clone(), options.clone())
        .await
        .expect("Failed to submit first task");

    // Delete the task object directly to simulate a missing task.
    queue
        .client()
        .delete_object(&task1.key())
        .await
        .expect("Failed to delete task object");

    // Resubmit with same key and input; should create a new task.
    let task2 = queue
        .submit(&task_type, input.clone(), options.clone())
        .await
        .expect("Failed to submit task after delete");

    assert_ne!(
        task1.id, task2.id,
        "Missing task should result in a new task ID"
    );
}

/// Test that submitting with a different idempotency_key creates a new task.
#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_different_idempotency_key_creates_new_task() {
    let queue = test_queue().await;
    let task_type = unique_task_type();

    let key1 = format!("test-key-{}", uuid::Uuid::new_v4());
    let key2 = format!("test-key-{}", uuid::Uuid::new_v4());

    let options1 = SubmitOptions {
        idempotency_key: Some(key1),
        idempotency_ttl_days: Some(30),
        ..Default::default()
    };

    let options2 = SubmitOptions {
        idempotency_key: Some(key2),
        idempotency_ttl_days: Some(30),
        ..Default::default()
    };

    // First submission
    let task1 = queue
        .submit(&task_type, json!({"order_id": "ORD-123"}), options1)
        .await
        .expect("Failed to submit first task");

    // Second submission with different key
    let task2 = queue
        .submit(&task_type, json!({"order_id": "ORD-123"}), options2)
        .await
        .expect("Failed to submit second task");

    // Should create a new task
    assert_ne!(
        task1.id, task2.id,
        "Different idempotency keys should create different tasks"
    );
}

/// Test that submitting with the same key but different input returns an error.
#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_same_key_different_input_returns_error() {
    let queue = test_queue().await;
    let task_type = unique_task_type();
    let idempotency_key = format!("test-key-{}", uuid::Uuid::new_v4());

    let options = SubmitOptions {
        idempotency_key: Some(idempotency_key.clone()),
        idempotency_ttl_days: Some(30),
        ..Default::default()
    };

    // First submission
    let _task1 = queue
        .submit(&task_type, json!({"order_id": "ORD-123"}), options.clone())
        .await
        .expect("Failed to submit first task");

    // Second submission with same key but different input
    let result = queue
        .submit(&task_type, json!({"order_id": "ORD-456"}), options.clone())
        .await;

    // Should return an error (IdempotencyConflict)
    assert!(
        result.is_err(),
        "Same key with different input should return an error"
    );
    let err = result.unwrap_err();
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("Idempotency conflict") || err_msg.contains("idempotency"),
        "Error should mention idempotency conflict: {}",
        err_msg
    );
}

// ============================================================================
// Idempotency scope tests
// ============================================================================

/// Test idempotency_scope = TaskType (default) - keys are unique per task type.
#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_idempotency_scope_task_type() {
    let queue = test_queue().await;
    let task_type1 = unique_task_type();
    let task_type2 = unique_task_type();
    let idempotency_key = format!("shared-key-{}", uuid::Uuid::new_v4());

    let options = SubmitOptions {
        idempotency_key: Some(idempotency_key.clone()),
        idempotency_ttl_days: Some(30),
        idempotency_scope: Some(IdempotencyScope::TaskType),
        ..Default::default()
    };

    // Submit to first task type
    let task1 = queue
        .submit(&task_type1, json!({"data": "first"}), options.clone())
        .await
        .expect("Failed to submit to first task type");

    // Submit to second task type with same key
    let task2 = queue
        .submit(&task_type2, json!({"data": "second"}), options.clone())
        .await
        .expect("Failed to submit to second task type");

    // Should create different tasks because scope is per task type
    assert_ne!(
        task1.id, task2.id,
        "TaskType scope should allow same key across different task types"
    );
    assert_eq!(task1.task_type, task_type1);
    assert_eq!(task2.task_type, task_type2);
}

/// Test idempotency_scope = Queue - keys are unique across the entire queue.
#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_idempotency_scope_queue() {
    let queue = test_queue().await;
    let task_type1 = unique_task_type();
    let task_type2 = unique_task_type();
    let idempotency_key = format!("queue-key-{}", uuid::Uuid::new_v4());

    let options = SubmitOptions {
        idempotency_key: Some(idempotency_key.clone()),
        idempotency_ttl_days: Some(30),
        idempotency_scope: Some(IdempotencyScope::Queue),
        ..Default::default()
    };

    // Submit to first task type
    let task1 = queue
        .submit(&task_type1, json!({"data": "first"}), options.clone())
        .await
        .expect("Failed to submit to first task type");

    // Submit to second task type with same key and same input
    // (Note: with Queue scope, even different task types share the same key namespace,
    // but the input must match)
    let task2 = queue
        .submit(&task_type2, json!({"data": "first"}), options.clone())
        .await
        .expect("Failed to submit to second task type");

    // Should return the same task because Queue scope is global
    assert_eq!(
        task1.id, task2.id,
        "Queue scope should return same task for same key across different task types"
    );
}

/// Test idempotency_scope = Queue with different input returns error.
#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_idempotency_scope_queue_different_input_error() {
    let queue = test_queue().await;
    let task_type1 = unique_task_type();
    let task_type2 = unique_task_type();
    let idempotency_key = format!("queue-key-conflict-{}", uuid::Uuid::new_v4());

    let options = SubmitOptions {
        idempotency_key: Some(idempotency_key.clone()),
        idempotency_ttl_days: Some(30),
        idempotency_scope: Some(IdempotencyScope::Queue),
        ..Default::default()
    };

    // Submit to first task type
    let _task1 = queue
        .submit(&task_type1, json!({"data": "first"}), options.clone())
        .await
        .expect("Failed to submit to first task type");

    // Submit to second task type with same key but different input
    let result = queue
        .submit(&task_type2, json!({"data": "second"}), options.clone())
        .await;

    // Should return an error because input differs
    assert!(
        result.is_err(),
        "Queue scope with same key but different input should return error"
    );
}

/// Test that submitting without idempotency key always creates new tasks.
#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_no_idempotency_key_creates_new_tasks() {
    let queue = test_queue().await;
    let task_type = unique_task_type();

    // Submit without idempotency key
    let task1 = queue
        .submit(
            &task_type,
            json!({"order_id": "ORD-123"}),
            SubmitOptions::default(),
        )
        .await
        .expect("Failed to submit first task");

    // Submit again without idempotency key
    let task2 = queue
        .submit(
            &task_type,
            json!({"order_id": "ORD-123"}),
            SubmitOptions::default(),
        )
        .await
        .expect("Failed to submit second task");

    // Should create different tasks
    assert_ne!(
        task1.id, task2.id,
        "Submissions without idempotency key should always create new tasks"
    );
}
