//! Tests for idempotency keys functionality.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use chrono::{Duration, Utc};
use serde_json::json;

use super::idempotency::*;
use crate::models::Task;

#[test]
fn test_compute_key_hash_deterministic() {
    let hash1 = compute_key_hash("charge_customer", "order-123");
    let hash2 = compute_key_hash("charge_customer", "order-123");
    assert_eq!(hash1, hash2);
}

#[test]
fn test_compute_key_hash_different_scope() {
    let hash1 = compute_key_hash("charge_customer", "order-123");
    let hash2 = compute_key_hash("send_email", "order-123");
    assert_ne!(hash1, hash2);
}

#[test]
fn test_compute_key_hash_different_key() {
    let hash1 = compute_key_hash("charge_customer", "order-123");
    let hash2 = compute_key_hash("charge_customer", "order-456");
    assert_ne!(hash1, hash2);
}

#[test]
fn test_compute_key_hash_format() {
    let hash = compute_key_hash("test", "key");
    assert!(hash.starts_with("sha256:"));
    // SHA256 produces 64 hex characters
    assert_eq!(hash.len(), 7 + 64);
}

#[test]
fn test_compute_input_hash_deterministic() {
    let input = json!({"order_id": "ORD-123", "amount": 100});
    let hash1 = compute_input_hash(&input);
    let hash2 = compute_input_hash(&input);
    assert_eq!(hash1, hash2);
}

#[test]
fn test_compute_input_hash_different_values() {
    let input1 = json!({"order_id": "ORD-123", "amount": 100});
    let input2 = json!({"order_id": "ORD-123", "amount": 200});
    let hash1 = compute_input_hash(&input1);
    let hash2 = compute_input_hash(&input2);
    assert_ne!(hash1, hash2);
}

#[test]
fn test_compute_input_hash_format() {
    let input = json!({});
    let hash = compute_input_hash(&input);
    assert!(hash.starts_with("sha256:"));
    assert_eq!(hash.len(), 7 + 64);
}

#[test]
fn test_idempotency_scope_default() {
    let scope = IdempotencyScope::default();
    assert_eq!(scope, IdempotencyScope::TaskType);
}

#[test]
fn test_idempotency_scope_produces_different_keys_per_task_type() {
    // TaskType scope should produce different keys for different task types
    let scope = IdempotencyScope::TaskType;
    let key1 = idempotency_s3_key(&scope, "charge_customer", "order-123");
    let key2 = idempotency_s3_key(&scope, "send_email", "order-123");
    assert_ne!(key1, key2);
    assert!(key1.contains("charge_customer"));
    assert!(key2.contains("send_email"));
}

#[test]
fn test_idempotency_scope_queue_same_key_different_types() {
    // Queue scope should produce the same key regardless of task type
    let scope = IdempotencyScope::Queue;
    let key1 = idempotency_s3_key(&scope, "charge_customer", "order-123");
    let key2 = idempotency_s3_key(&scope, "send_email", "order-123");
    assert_eq!(key1, key2);
    assert!(key1.contains("queue"));
}

#[test]
fn test_idempotency_scope_custom_uses_namespace() {
    let scope = IdempotencyScope::Custom("my_namespace".to_string());
    let key = idempotency_s3_key(&scope, "charge_customer", "order-123");
    assert!(key.contains("my_namespace"));
}

#[test]
fn test_idempotency_record_new() {
    let input = json!({"order_id": "ORD-123"});
    let task = Task::new("charge_customer", input);
    let now = Utc::now();
    let scope = IdempotencyScope::TaskType;

    let record = IdempotencyRecord::new("charge-ORD-123".to_string(), &task, &scope, 30, now);

    assert_eq!(record.key, "charge-ORD-123");
    assert!(record.key_hash.starts_with("sha256:"));
    assert_eq!(record.task_id, task.id);
    assert_eq!(record.task_key, task.key());
    assert_eq!(record.created_at, now);
    assert_eq!(record.expires_at, now + Duration::days(30));
    assert_eq!(record.task_type, "charge_customer");
    assert!(record.input_hash.starts_with("sha256:"));
}

#[test]
fn test_idempotency_record_s3_key_task_type_scope() {
    let input = json!({});
    let task = Task::new("charge_customer", input);
    let now = Utc::now();
    let scope = IdempotencyScope::TaskType;

    let record = IdempotencyRecord::new("test-key".to_string(), &task, &scope, 30, now);
    let s3_key = record.s3_key(&scope);

    assert!(s3_key.starts_with("idempotency/charge_customer/"));
    assert!(s3_key.ends_with(".json"));
    // Should not contain "sha256:" in the path
    assert!(!s3_key.contains("sha256:"));
}

#[test]
fn test_idempotency_record_s3_key_queue_scope() {
    let input = json!({});
    let task = Task::new("charge_customer", input);
    let now = Utc::now();
    let scope = IdempotencyScope::Queue;

    let record = IdempotencyRecord::new("test-key".to_string(), &task, &scope, 30, now);
    let s3_key = record.s3_key(&scope);

    assert!(s3_key.starts_with("idempotency/queue/"));
    assert!(s3_key.ends_with(".json"));
}

#[test]
fn test_idempotency_record_s3_key_custom_scope() {
    let input = json!({});
    let task = Task::new("charge_customer", input);
    let now = Utc::now();
    let scope = IdempotencyScope::Custom("my_app".to_string());

    let record = IdempotencyRecord::new("test-key".to_string(), &task, &scope, 30, now);
    let s3_key = record.s3_key(&scope);

    assert!(s3_key.starts_with("idempotency/my_app/"));
    assert!(s3_key.ends_with(".json"));
}

#[test]
fn test_idempotency_record_is_expired() {
    let input = json!({});
    let task = Task::new("test", input);
    let now = Utc::now();
    let scope = IdempotencyScope::TaskType;

    let record = IdempotencyRecord::new("key".to_string(), &task, &scope, 1, now);

    // Not expired yet
    assert!(!record.is_expired(now));
    assert!(!record.is_expired(now + Duration::hours(12)));

    // Expired after TTL
    assert!(record.is_expired(now + Duration::days(2)));
}

#[test]
fn test_idempotency_record_is_expired_boundary() {
    let input = json!({});
    let task = Task::new("test", input);
    let now = Utc::now();
    let scope = IdempotencyScope::TaskType;

    let record = IdempotencyRecord::new("key".to_string(), &task, &scope, 1, now);

    // Exactly at expiry time - should not be expired yet
    assert!(!record.is_expired(now + Duration::days(1)));

    // One second after - should be expired
    assert!(record.is_expired(now + Duration::days(1) + Duration::seconds(1)));
}

#[test]
fn test_idempotency_record_validate_input_success() {
    let input = json!({"order_id": "ORD-123"});
    let task = Task::new("charge_customer", input.clone());
    let now = Utc::now();
    let scope = IdempotencyScope::TaskType;

    let record = IdempotencyRecord::new("key".to_string(), &task, &scope, 30, now);

    // Same input should pass
    assert!(record.validate_input(&input).is_ok());
}

#[test]
fn test_idempotency_record_validate_input_conflict() {
    let input = json!({"order_id": "ORD-123"});
    let task = Task::new("charge_customer", input);
    let now = Utc::now();
    let scope = IdempotencyScope::TaskType;

    let record = IdempotencyRecord::new("key".to_string(), &task, &scope, 30, now);

    // Different input should fail
    let different_input = json!({"order_id": "ORD-456"});
    let result = record.validate_input(&different_input);
    assert!(result.is_err());

    let err = result.unwrap_err();
    assert_eq!(err.key, "key");
    assert_eq!(err.existing_task_id, task.id);
    assert!(err.message.contains("Idempotency key"));
    assert!(err.message.contains("different input"));
}

#[test]
fn test_idempotency_record_serialization() {
    let input = json!({"order_id": "ORD-123"});
    let task = Task::new("charge_customer", input);
    let now = Utc::now();
    let scope = IdempotencyScope::TaskType;

    let record = IdempotencyRecord::new("key".to_string(), &task, &scope, 30, now);

    // Serialize and deserialize
    let json = serde_json::to_string(&record).expect("serialize");
    let deserialized: IdempotencyRecord = serde_json::from_str(&json).expect("deserialize");

    assert_eq!(record.key, deserialized.key);
    assert_eq!(record.key_hash, deserialized.key_hash);
    assert_eq!(record.task_id, deserialized.task_id);
    assert_eq!(record.task_key, deserialized.task_key);
    assert_eq!(record.task_type, deserialized.task_type);
    assert_eq!(record.input_hash, deserialized.input_hash);
}

#[test]
fn test_idempotency_s3_key_function() {
    let scope = IdempotencyScope::TaskType;
    let key = idempotency_s3_key(&scope, "charge_customer", "order-123");

    assert!(key.starts_with("idempotency/charge_customer/"));
    assert!(key.ends_with(".json"));
    assert!(!key.contains("sha256:"));
}

#[test]
fn test_idempotency_s3_key_queue_scope() {
    let scope = IdempotencyScope::Queue;
    let key = idempotency_s3_key(&scope, "charge_customer", "order-123");

    assert!(key.starts_with("idempotency/queue/"));
}

#[test]
fn test_idempotency_s3_key_custom_scope() {
    let scope = IdempotencyScope::Custom("billing".to_string());
    let key = idempotency_s3_key(&scope, "charge_customer", "order-123");

    assert!(key.starts_with("idempotency/billing/"));
}

#[test]
fn test_idempotency_result_created() {
    let task = Task::new("test", json!({}));
    let result = IdempotencyResult::Created(task.clone());

    assert!(result.is_created());
    assert!(!result.is_existing());

    let returned_task = result.into_task();
    assert_eq!(returned_task.id, task.id);
}

#[test]
fn test_idempotency_result_existing() {
    let task = Task::new("test", json!({}));
    let result = IdempotencyResult::Existing(task.clone());

    assert!(!result.is_created());
    assert!(result.is_existing());

    let returned_task = result.into_task();
    assert_eq!(returned_task.id, task.id);
}

#[test]
fn test_idempotency_conflict_error_display() {
    let err = IdempotencyConflictError {
        key: "test-key".to_string(),
        existing_task_id: uuid::Uuid::nil(),
        message: "Test conflict message".to_string(),
    };

    let display = format!("{err}");
    assert_eq!(display, "Test conflict message");
}

#[test]
fn test_idempotency_scope_serialization() {
    // TaskType
    let scope = IdempotencyScope::TaskType;
    let json = serde_json::to_string(&scope).expect("serialize");
    assert_eq!(json, "\"task_type\"");
    let deserialized: IdempotencyScope = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(scope, deserialized);

    // Queue
    let scope = IdempotencyScope::Queue;
    let json = serde_json::to_string(&scope).expect("serialize");
    assert_eq!(json, "\"queue\"");
    let deserialized: IdempotencyScope = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(scope, deserialized);

    // Custom
    let scope = IdempotencyScope::Custom("my_ns".to_string());
    let json = serde_json::to_string(&scope).expect("serialize");
    let deserialized: IdempotencyScope = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(scope, deserialized);
}

#[test]
fn test_idempotency_options_default() {
    let options = IdempotencyOptions::default();
    assert!(options.key.is_empty());
    assert!(options.ttl_days.is_none());
    assert_eq!(options.scope, IdempotencyScope::TaskType);
}

#[test]
fn test_default_ttl_days() {
    assert_eq!(DEFAULT_IDEMPOTENCY_TTL_DAYS, 30);
}

#[test]
fn test_idempotency_error_display() {
    let conflict_err = IdempotencyError::Conflict(IdempotencyConflictError {
        key: "test".to_string(),
        existing_task_id: uuid::Uuid::nil(),
        message: "Conflict!".to_string(),
    });
    let display = format!("{conflict_err}");
    assert_eq!(display, "Conflict!");

    let storage_err = IdempotencyError::Storage(crate::storage::StorageError::NotFound {
        key: "test".to_string(),
    });
    let display = format!("{storage_err}");
    assert!(display.contains("Storage error"));
}
