//! Idempotency keys for preventing duplicate task creation.
//!
//! This module provides support for idempotent task submissions. When a producer
//! retries a submission due to network issues or other failures, the same task
//! will be returned instead of creating a duplicate.
//!
//! ## S3 Layout
//!
//! ```text
//! bucket/
//! ├── tasks/{shard}/{task_id}.json
//! └── idempotency/{scope}/{key_hash}.json
//! ```
//!
//! ## Example
//!
//! ```rust,ignore
//! // Rust
//! queue.submit("charge_customer", input)
//!      .idempotency_key("charge-ORD-123")
//!      .idempotency_ttl(Duration::days(30));
//! ```
//!
//! ```python
//! # Python
//! await queue.submit(
//!     "charge_customer",
//!     input,
//!     idempotency_key="charge-ORD-123",
//!     idempotency_ttl_days=30,
//! )
//! ```

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::models::Task;
use crate::queue::Queue;
use crate::storage::{PutCondition, StorageError};

/// Default TTL for idempotency records (30 days).
pub const DEFAULT_IDEMPOTENCY_TTL_DAYS: i64 = 30;

/// Scope for idempotency key uniqueness.
///
/// The scope determines the namespace for idempotency keys:
/// - `TaskType`: Keys are unique per task type (default)
/// - `Queue`: Keys are unique across the entire queue
/// - `Custom`: Keys are unique within a custom string namespace
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdempotencyScope {
    /// Keys are unique per task type (default).
    TaskType,
    /// Keys are unique across the entire queue.
    Queue,
    /// Keys are unique within a custom namespace.
    Custom(String),
}

impl Default for IdempotencyScope {
    fn default() -> Self {
        Self::TaskType
    }
}

impl IdempotencyScope {
    /// Returns the scope string for hashing.
    fn scope_string(&self, task_type: &str) -> String {
        match self {
            Self::TaskType => task_type.to_string(),
            Self::Queue => "queue".to_string(),
            Self::Custom(s) => s.clone(),
        }
    }
}

/// An idempotency record stored in S3.
///
/// This record maps an idempotency key to a task ID, allowing subsequent
/// submissions with the same key to return the existing task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdempotencyRecord {
    /// The original idempotency key provided by the user.
    pub key: String,
    /// SHA256 hash of scope:key, prefixed with "sha256:".
    pub key_hash: String,
    /// The UUID of the associated task.
    pub task_id: Uuid,
    /// The S3 key of the associated task (e.g., "tasks/a/uuid.json").
    pub task_key: String,
    /// When this record was created.
    pub created_at: DateTime<Utc>,
    /// When this record expires.
    pub expires_at: DateTime<Utc>,
    /// The task type associated with this record.
    pub task_type: String,
    /// SHA256 hash of the input data for validation, prefixed with "sha256:".
    pub input_hash: String,
}

impl IdempotencyRecord {
    /// Creates a new idempotency record for a task.
    #[must_use]
    pub fn new(
        key: String,
        task: &Task,
        scope: &IdempotencyScope,
        ttl_days: i64,
        now: DateTime<Utc>,
    ) -> Self {
        let key_hash = compute_key_hash(&scope.scope_string(&task.task_type), &key);
        let input_hash = compute_input_hash(&task.input);

        Self {
            key,
            key_hash,
            task_id: task.id,
            task_key: task.key(),
            created_at: now,
            expires_at: now + Duration::days(ttl_days),
            task_type: task.task_type.clone(),
            input_hash,
        }
    }

    /// Returns the S3 key for this idempotency record.
    #[must_use]
    pub fn s3_key(&self, scope: &IdempotencyScope) -> String {
        let scope_str = self.scope_string(scope);
        // Only use the hex part of the hash (after "sha256:")
        let hash_hex = self
            .key_hash
            .strip_prefix("sha256:")
            .unwrap_or(&self.key_hash);
        format!("idempotency/{scope_str}/{hash_hex}.json")
    }

    /// Returns the scope string for this record.
    fn scope_string(&self, scope: &IdempotencyScope) -> String {
        scope.scope_string(&self.task_type)
    }

    /// Returns true if this record has expired.
    #[must_use]
    pub fn is_expired(&self, now: DateTime<Utc>) -> bool {
        now > self.expires_at
    }

    /// Validates that the given input matches this record's input hash.
    ///
    /// # Errors
    ///
    /// Returns `IdempotencyConflict` if the input hash does not match.
    pub fn validate_input(&self, input: &Value) -> Result<(), IdempotencyConflictError> {
        let new_hash = compute_input_hash(input);
        if new_hash != self.input_hash {
            return Err(IdempotencyConflictError {
                key: self.key.clone(),
                existing_task_id: self.task_id,
                message: format!(
                    "Idempotency key '{}' was used with different input. \
                     Existing task: {}. Input hash mismatch: expected {}, got {}",
                    self.key, self.task_id, self.input_hash, new_hash
                ),
            });
        }
        Ok(())
    }
}

/// Error returned when an idempotency key is reused with different input.
#[derive(Debug, Clone)]
pub struct IdempotencyConflictError {
    /// The idempotency key that caused the conflict.
    pub key: String,
    /// The existing task ID associated with this key.
    pub existing_task_id: Uuid,
    /// A human-readable message describing the conflict.
    pub message: String,
}

impl std::fmt::Display for IdempotencyConflictError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for IdempotencyConflictError {}

/// Computes the SHA256 hash of scope:key for idempotency lookups.
///
/// The hash is prefixed with "sha256:" for clarity and future-proofing.
#[must_use]
pub fn compute_key_hash(scope: &str, key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(scope.as_bytes());
    hasher.update(b":");
    hasher.update(key.as_bytes());
    let result = hasher.finalize();
    format!("sha256:{result:x}")
}

/// Canonicalizes a JSON value by sorting object keys recursively.
///
/// This ensures that logically equivalent JSON (with different key orders)
/// produces the same canonical representation for consistent hashing.
fn canonicalize_json(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            // Collect keys and sort them
            let mut sorted: Vec<_> = map.iter().collect();
            sorted.sort_by(|(a, _), (b, _)| a.cmp(b));

            // Build new map with sorted keys and recursively canonicalize values
            let canonical: serde_json::Map<String, Value> = sorted
                .into_iter()
                .map(|(k, v)| (k.clone(), canonicalize_json(v)))
                .collect();
            Value::Object(canonical)
        }
        Value::Array(arr) => {
            // Recursively canonicalize array elements
            Value::Array(arr.iter().map(canonicalize_json).collect())
        }
        // Primitives are already canonical
        _ => value.clone(),
    }
}

/// Computes the SHA256 hash of the input JSON for validation.
///
/// Uses canonical JSON serialization (sorted keys) to ensure that logically
/// equivalent inputs with different key orders produce the same hash.
///
/// The hash is prefixed with "sha256:" for clarity and future-proofing.
#[must_use]
pub fn compute_input_hash(input: &Value) -> String {
    let mut hasher = Sha256::new();
    // Canonicalize JSON before hashing to ensure consistent results
    let canonical = canonicalize_json(input);
    let json = serde_json::to_string(&canonical).unwrap_or_default();
    hasher.update(json.as_bytes());
    let result = hasher.finalize();
    format!("sha256:{result:x}")
}

/// Returns the S3 key for an idempotency record given its parameters.
#[must_use]
pub fn idempotency_s3_key(scope: &IdempotencyScope, task_type: &str, key: &str) -> String {
    let scope_str = scope.scope_string(task_type);
    let key_hash = compute_key_hash(&scope_str, key);
    let hash_hex = key_hash.strip_prefix("sha256:").unwrap_or(&key_hash);
    format!("idempotency/{scope_str}/{hash_hex}.json")
}

/// Result of an idempotent submit operation.
#[derive(Debug)]
pub enum IdempotencyResult {
    /// A new task was created.
    Created(Task),
    /// An existing task was found and returned.
    Existing(Task),
}

impl IdempotencyResult {
    /// Returns the task, regardless of whether it was created or existing.
    #[must_use]
    pub fn into_task(self) -> Task {
        match self {
            Self::Created(task) | Self::Existing(task) => task,
        }
    }

    /// Returns true if a new task was created.
    #[must_use]
    pub const fn is_created(&self) -> bool {
        matches!(self, Self::Created(_))
    }

    /// Returns true if an existing task was returned.
    #[must_use]
    pub const fn is_existing(&self) -> bool {
        matches!(self, Self::Existing(_))
    }
}

/// Options for idempotent submission.
#[derive(Debug, Clone, Default)]
pub struct IdempotencyOptions {
    /// The idempotency key.
    pub key: String,
    /// TTL in days for the idempotency record.
    pub ttl_days: Option<i64>,
    /// Scope for key uniqueness.
    pub scope: IdempotencyScope,
}

impl Queue {
    /// Attempts to create an idempotency record, returning existing task if one exists.
    ///
    /// This method handles the idempotent submit flow:
    /// 1. Compute key hash
    /// 2. Try to PUT the idempotency record with If-None-Match
    /// 3. If success, return `None` (caller should create the task)
    /// 4. If exists, read existing record, verify input hash, return existing task
    ///
    /// # Arguments
    ///
    /// * `key` - The idempotency key
    /// * `task_type` - The task type
    /// * `input` - The task input (for hash validation)
    /// * `scope` - The scope for key uniqueness
    /// * `ttl_days` - TTL for the record
    ///
    /// # Returns
    ///
    /// `None` if no existing record (caller should create task), or `Some(task)` if existing.
    ///
    /// # Errors
    ///
    /// Returns `IdempotencyConflictError` if the key exists with different input.
    pub async fn check_idempotency(
        &self,
        key: &str,
        _task_type: &str,
        input: &Value,
        scope: &IdempotencyScope,
        ttl_days: i64,
        task: &Task,
    ) -> Result<Option<Task>, IdempotencyError> {
        let now = self.now().await.map_err(IdempotencyError::Storage)?;

        // Create the idempotency record
        let record = IdempotencyRecord::new(key.to_string(), task, scope, ttl_days, now);

        let s3_key = record.s3_key(scope);
        let body = serde_json::to_vec(&record).map_err(|e| {
            IdempotencyError::Storage(StorageError::SerializationError(e.to_string()))
        })?;

        // Try to create the idempotency record atomically
        match self
            .client()
            .put_object(&s3_key, body, PutCondition::IfNoneMatch)
            .await
        {
            Ok(_) => {
                // Record created successfully, no existing task
                Ok(None)
            }
            Err(StorageError::PreconditionFailed { .. }) => {
                // Record already exists, check if it's expired or valid
                self.handle_existing_idempotency_record(&s3_key, key, input, scope, ttl_days, task)
                    .await
            }
            Err(e) => Err(IdempotencyError::Storage(e)),
        }
    }

    /// Handles the case where an idempotency record already exists.
    async fn handle_existing_idempotency_record(
        &self,
        s3_key: &str,
        key: &str,
        input: &Value,
        scope: &IdempotencyScope,
        ttl_days: i64,
        new_task: &Task,
    ) -> Result<Option<Task>, IdempotencyError> {
        // Read the existing record
        let (body, etag) = self
            .client()
            .get_object(s3_key)
            .await
            .map_err(IdempotencyError::Storage)?;

        let existing_record: IdempotencyRecord = serde_json::from_slice(&body).map_err(|e| {
            IdempotencyError::Storage(StorageError::SerializationError(e.to_string()))
        })?;

        let now = self.now().await.map_err(IdempotencyError::Storage)?;

        // Check if the record has expired
        if existing_record.is_expired(now) {
            // Try to overwrite the expired record with CAS
            let new_record =
                IdempotencyRecord::new(key.to_string(), new_task, scope, ttl_days, now);
            let new_body = serde_json::to_vec(&new_record).map_err(|e| {
                IdempotencyError::Storage(StorageError::SerializationError(e.to_string()))
            })?;

            match self
                .client()
                .put_object(s3_key, new_body, PutCondition::IfMatch(etag))
                .await
            {
                Ok(_) => {
                    // Successfully overwrote expired record
                    Ok(None)
                }
                Err(StorageError::PreconditionFailed { .. }) => {
                    // Another process beat us, recurse to check new state
                    // Use Box::pin to handle async recursion
                    Box::pin(self.handle_existing_idempotency_record(
                        s3_key, key, input, scope, ttl_days, new_task,
                    ))
                    .await
                }
                Err(e) => Err(IdempotencyError::Storage(e)),
            }
        } else {
            // Record is valid, validate input hash
            existing_record
                .validate_input(input)
                .map_err(IdempotencyError::Conflict)?;

            // Fetch and return the existing task
            match self.get(existing_record.task_id).await {
                Ok(Some((task, _))) => Ok(Some(task)),
                Ok(None) => {
                    // Task was deleted but idempotency record exists.
                    // Treat as expired: attempt to overwrite the record and allow submit.
                    let new_record =
                        IdempotencyRecord::new(key.to_string(), new_task, scope, ttl_days, now);
                    let new_body = serde_json::to_vec(&new_record).map_err(|e| {
                        IdempotencyError::Storage(StorageError::SerializationError(e.to_string()))
                    })?;

                    match self
                        .client()
                        .put_object(s3_key, new_body, PutCondition::IfMatch(etag))
                        .await
                    {
                        Ok(_) => Ok(None),
                        Err(StorageError::PreconditionFailed { .. }) => {
                            // Record changed; re-check latest state
                            Box::pin(self.handle_existing_idempotency_record(
                                s3_key, key, input, scope, ttl_days, new_task,
                            ))
                            .await
                        }
                        Err(e) => Err(IdempotencyError::Storage(e)),
                    }
                }
                Err(e) => Err(IdempotencyError::Storage(e)),
            }
        }
    }
}

/// Errors that can occur during idempotent operations.
#[derive(Debug)]
pub enum IdempotencyError {
    /// The idempotency key was reused with different input.
    Conflict(IdempotencyConflictError),
    /// A storage error occurred.
    Storage(StorageError),
}

impl std::fmt::Display for IdempotencyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Conflict(e) => write!(f, "{e}"),
            Self::Storage(e) => write!(f, "Storage error: {e}"),
        }
    }
}

impl std::error::Error for IdempotencyError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Conflict(e) => Some(e),
            Self::Storage(e) => Some(e),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_key_hash() {
        let hash = compute_key_hash("charge_customer", "order-123");
        assert!(hash.starts_with("sha256:"));
        assert_eq!(hash.len(), 7 + 64); // "sha256:" + 64 hex chars

        // Same input should produce same hash
        let hash2 = compute_key_hash("charge_customer", "order-123");
        assert_eq!(hash, hash2);

        // Different input should produce different hash
        let hash3 = compute_key_hash("charge_customer", "order-456");
        assert_ne!(hash, hash3);

        // Different scope should produce different hash
        let hash4 = compute_key_hash("send_email", "order-123");
        assert_ne!(hash, hash4);
    }

    #[test]
    fn test_compute_input_hash() {
        let input1 = serde_json::json!({"email": "test@example.com", "amount": 100});
        let hash1 = compute_input_hash(&input1);
        assert!(hash1.starts_with("sha256:"));

        // Same input should produce same hash
        let input2 = serde_json::json!({"email": "test@example.com", "amount": 100});
        let hash2 = compute_input_hash(&input2);
        assert_eq!(hash1, hash2);

        // Different input should produce different hash
        let input3 = serde_json::json!({"email": "other@example.com", "amount": 100});
        let hash3 = compute_input_hash(&input3);
        assert_ne!(hash1, hash3);
    }

    #[test]
    fn test_compute_input_hash_canonical_key_order() {
        // Different key order should produce the same hash (canonical JSON)
        let input1 = serde_json::json!({"a": 1, "b": 2, "c": 3});
        let input2 = serde_json::json!({"c": 3, "b": 2, "a": 1});
        let input3 = serde_json::json!({"b": 2, "a": 1, "c": 3});

        let hash1 = compute_input_hash(&input1);
        let hash2 = compute_input_hash(&input2);
        let hash3 = compute_input_hash(&input3);

        assert_eq!(hash1, hash2);
        assert_eq!(hash2, hash3);
    }

    #[test]
    fn test_compute_input_hash_canonical_nested() {
        // Nested objects should also be canonicalized
        let input1 = serde_json::json!({
            "outer": {"z": 1, "a": 2},
            "array": [{"b": 1, "a": 2}]
        });
        let input2 = serde_json::json!({
            "array": [{"a": 2, "b": 1}],
            "outer": {"a": 2, "z": 1}
        });

        let hash1 = compute_input_hash(&input1);
        let hash2 = compute_input_hash(&input2);

        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_idempotency_scope_default() {
        assert_eq!(IdempotencyScope::default(), IdempotencyScope::TaskType);
    }

    #[test]
    fn test_idempotency_scope_scope_string() {
        assert_eq!(
            IdempotencyScope::TaskType.scope_string("charge_customer"),
            "charge_customer"
        );
        assert_eq!(
            IdempotencyScope::Queue.scope_string("charge_customer"),
            "queue"
        );
        assert_eq!(
            IdempotencyScope::Custom("my_namespace".to_string()).scope_string("charge_customer"),
            "my_namespace"
        );
    }

    #[test]
    fn test_idempotency_record_new() {
        let input = serde_json::json!({"order_id": "ORD-123"});
        let task = Task::new("charge_customer", input.clone());
        let now = Utc::now();
        let scope = IdempotencyScope::TaskType;

        let record = IdempotencyRecord::new("charge-ORD-123".to_string(), &task, &scope, 30, now);

        assert_eq!(record.key, "charge-ORD-123");
        assert!(record.key_hash.starts_with("sha256:"));
        assert_eq!(record.task_id, task.id);
        assert_eq!(record.task_key, task.key());
        assert_eq!(record.task_type, "charge_customer");
        assert_eq!(record.created_at, now);
        assert_eq!(record.expires_at, now + Duration::days(30));
        assert!(record.input_hash.starts_with("sha256:"));
    }

    #[test]
    fn test_idempotency_record_is_expired() {
        let input = serde_json::json!({});
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
    fn test_idempotency_record_validate_input() {
        let input = serde_json::json!({"order_id": "ORD-123"});
        let task = Task::new("charge_customer", input.clone());
        let now = Utc::now();
        let scope = IdempotencyScope::TaskType;

        let record = IdempotencyRecord::new("key".to_string(), &task, &scope, 30, now);

        // Same input should pass
        assert!(record.validate_input(&input).is_ok());

        // Different input should fail
        let different_input = serde_json::json!({"order_id": "ORD-456"});
        let result = record.validate_input(&different_input);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.key, "key");
        assert_eq!(err.existing_task_id, task.id);
    }

    #[test]
    fn test_idempotency_s3_key() {
        let scope = IdempotencyScope::TaskType;
        let key = idempotency_s3_key(&scope, "charge_customer", "order-123");
        assert!(key.starts_with("idempotency/charge_customer/"));
        assert!(key.ends_with(".json"));

        // Queue scope uses "queue" as the scope string
        let scope = IdempotencyScope::Queue;
        let key = idempotency_s3_key(&scope, "charge_customer", "order-123");
        assert!(key.starts_with("idempotency/queue/"));

        // Custom scope
        let scope = IdempotencyScope::Custom("my_app".to_string());
        let key = idempotency_s3_key(&scope, "charge_customer", "order-123");
        assert!(key.starts_with("idempotency/my_app/"));
    }

    #[test]
    fn test_idempotency_result() {
        let task = Task::new("test", serde_json::json!({}));

        let created = IdempotencyResult::Created(task.clone());
        assert!(created.is_created());
        assert!(!created.is_existing());

        let existing = IdempotencyResult::Existing(task.clone());
        assert!(!existing.is_created());
        assert!(existing.is_existing());

        // Both should return the task
        let task_from_created = IdempotencyResult::Created(task.clone()).into_task();
        assert_eq!(task_from_created.id, task.id);
    }

    #[test]
    fn test_idempotency_scope_serialization() {
        let scope = IdempotencyScope::TaskType;
        let json = serde_json::to_string(&scope).expect("serialize");
        assert_eq!(json, "\"task_type\"");

        let scope = IdempotencyScope::Queue;
        let json = serde_json::to_string(&scope).expect("serialize");
        assert_eq!(json, "\"queue\"");

        let scope = IdempotencyScope::Custom("my_ns".to_string());
        let json = serde_json::to_string(&scope).expect("serialize");
        assert!(json.contains("my_ns"));
    }
}
