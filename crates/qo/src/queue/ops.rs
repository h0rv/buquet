//! Queue operations implementation.

use std::env;
use std::time::Duration;

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use futures::future::join_all;

use crate::config::load_config;
use crate::models::{RetryPolicy, Task, TaskStatus};
use crate::storage::{PutCondition, S3Client, S3Config, StorageError};
use metrics::counter;
use serde_json::Value;
use uuid::Uuid;

use super::config::QueueConfig;
use super::error::TaskOperationError;
use super::payload::{
    load_payload, should_offload_payload, store_payload, PayloadType,
    DEFAULT_PAYLOAD_REF_THRESHOLD_BYTES,
};

use super::idempotency::{IdempotencyError, IdempotencyScope, DEFAULT_IDEMPOTENCY_TTL_DAYS};

/// Result of cancelling tasks by type.
#[derive(Debug, Clone)]
pub struct CancelByTypeResult {
    /// Tasks that were successfully cancelled.
    pub cancelled: Vec<Task>,
    /// Tasks that failed to cancel, with their errors.
    pub failed: Vec<(Uuid, StorageError)>,
}

/// Options for submitting a task.
#[derive(Debug, Clone, Default)]
pub struct SubmitOptions {
    /// Task timeout in seconds (default: 300).
    pub timeout_seconds: Option<u64>,
    /// Maximum retries (default: 3).
    pub max_retries: Option<u32>,
    /// Custom retry policy.
    pub retry_policy: Option<RetryPolicy>,
    /// Schedule the task to run at a specific future time.
    /// The task will not be available for claiming until this time.
    pub schedule_at: Option<DateTime<Utc>>,
    /// Whether to use payload references for large inputs.
    /// When enabled, inputs exceeding the threshold will be stored separately.
    pub use_payload_refs: bool,
    /// Threshold in bytes for offloading payloads (default: 256KB).
    /// Only applies when `use_payload_refs` is true.
    pub payload_ref_threshold_bytes: Option<usize>,
    /// Idempotency key for preventing duplicate task creation.
    /// When provided, subsequent submissions with the same key will return
    /// the existing task instead of creating a new one.
    pub idempotency_key: Option<String>,
    /// TTL in days for the idempotency record (default: 30).
    /// Only applies when `idempotency_key` is provided.
    pub idempotency_ttl_days: Option<i64>,
    /// Scope for idempotency key uniqueness.
    /// - `TaskType`: Keys are unique per task type (default)
    /// - `Queue`: Keys are unique across the entire queue
    /// - `Custom`: Keys are unique within a custom namespace
    pub idempotency_scope: Option<IdempotencyScope>,
    /// Maximum number of reschedules before failing (None = unlimited).
    pub max_reschedules: Option<u32>,
    /// Task TTL (time-to-live) in seconds.
    /// If set, `expires_at` will be calculated as `now + ttl_seconds`.
    /// The task will expire if not processed by this deadline.
    /// Takes precedence over `expires_at` if both are set.
    pub ttl_seconds: Option<u64>,
    /// Absolute expiration time for the task.
    /// The task will expire if not processed by this deadline.
    /// Ignored if `ttl_seconds` is also set.
    pub expires_at: Option<DateTime<Utc>>,
}

/// Options for connecting to a queue.
#[derive(Debug, Clone, Default)]
pub struct ConnectOptions {
    /// S3 endpoint URL (for S3-compatible services).
    pub endpoint: Option<String>,
    /// S3 bucket name.
    pub bucket: Option<String>,
    /// AWS region.
    pub region: Option<String>,
    /// Shard prefix length (1-4, default: 1).
    pub shard_prefix_len: Option<usize>,
}

/// Error type for connection failures.
#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    /// Bucket not configured in env vars or config file.
    #[error("bucket not configured - set S3_BUCKET env var or add to .qo.toml")]
    MissingBucket,
    /// Region not configured in env vars or config file.
    #[error("region not configured - set S3_REGION env var or add to .qo.toml")]
    MissingRegion,
    /// S3 storage error.
    #[error("S3 error: {0}")]
    Storage(#[from] StorageError),
}

/// Connect to a queue with automatic config resolution.
///
/// Config priority (highest to lowest):
/// 1. Provided options
/// 2. Environment variables (S3_ENDPOINT, S3_BUCKET, S3_REGION)
/// 3. Config file (.qo.toml)
///
/// # Example
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use qo::queue::{connect, ConnectOptions};
///
/// // Use defaults (env vars / config file)
/// let queue = connect(None).await?;
///
/// // Override specific options
/// let queue = connect(Some(ConnectOptions {
///     bucket: Some("my-bucket".to_string()),
///     ..Default::default()
/// })).await?;
/// # Ok(())
/// # }
/// ```
pub async fn connect(options: Option<ConnectOptions>) -> Result<Queue, ConnectError> {
    let opts = options.unwrap_or_default();

    // Load config file (silently ignore errors)
    let config = load_config(None, None::<&std::path::PathBuf>).ok();

    // Resolve endpoint: options > env > config
    // No endpoint = use real AWS (the default for production)
    // Set S3_ENDPOINT explicitly for LocalStack/MinIO/etc.
    let endpoint = opts
        .endpoint
        .filter(|s| !s.is_empty())
        .or_else(|| env::var("S3_ENDPOINT").ok().filter(|s| !s.is_empty()))
        .or_else(|| {
            config
                .as_ref()
                .and_then(|c| c.endpoint.clone())
                .filter(|s| !s.is_empty())
        });

    // Resolve bucket: options > env > config
    let bucket = opts
        .bucket
        .or_else(|| env::var("S3_BUCKET").ok())
        .or_else(|| config.as_ref().map(|c| c.bucket.clone()))
        .ok_or(ConnectError::MissingBucket)?;

    // Resolve region: options > env > config > default
    let region = opts
        .region
        .or_else(|| env::var("S3_REGION").ok())
        .or_else(|| env::var("AWS_REGION").ok())
        .or_else(|| config.as_ref().map(|c| c.region.clone()))
        .unwrap_or_else(|| "us-east-1".to_string());

    // Create S3 client
    let s3_config = S3Config::new(endpoint, bucket, region);
    let client = S3Client::new(s3_config).await?;

    // Create queue with optional shard prefix config
    let mut queue_config = QueueConfig::default();
    if let Some(len) = opts.shard_prefix_len {
        queue_config.shard_prefix_len = len;
    }

    Ok(Queue::with_config(client, queue_config))
}

/// The main queue interface for task operations.
///
/// Provides methods for submitting tasks, retrieving tasks by ID,
/// listing tasks, and managing task state with atomic updates.
#[derive(Debug, Clone)]
pub struct Queue {
    client: S3Client,
    config: QueueConfig,
}

impl Queue {
    /// Creates a new Queue with the given S3 client and default config.
    #[must_use]
    pub fn new(client: S3Client) -> Self {
        Self {
            client,
            config: QueueConfig::default(),
        }
    }

    /// Creates a new Queue with the given S3 client and config.
    #[must_use]
    pub const fn with_config(client: S3Client, config: QueueConfig) -> Self {
        Self { client, config }
    }

    /// Returns the queue configuration.
    #[must_use]
    pub const fn config(&self) -> &QueueConfig {
        &self.config
    }

    /// Returns the shard prefix length.
    #[must_use]
    pub const fn shard_prefix_len(&self) -> usize {
        self.config.shard_prefix_len
    }

    /// Generate all shard prefixes for this queue's configuration.
    #[must_use]
    pub fn all_shards(&self) -> Vec<String> {
        self.config.all_shards()
    }

    /// Submits a new task to the queue.
    ///
    /// Creates the task object with `If-None-Match` to prevent duplicates,
    /// then creates the ready index object.
    ///
    /// If an idempotency key is provided, this method will:
    /// 1. Check for an existing idempotency record
    /// 2. If found and not expired, return the existing task
    /// 3. If found but expired, overwrite with the new task
    /// 4. If not found, create a new task and idempotency record
    ///
    /// # Arguments
    ///
    /// * `task_type` - The type of task (e.g., `send_email`, `process_image`)
    /// * `input` - The input data for the task as JSON
    /// * `options` - Optional configuration for timeout, retries, retry policy, and idempotency (see [`SubmitOptions`])
    ///
    /// # Returns
    ///
    /// The created task on success, or the existing task if idempotency key matched.
    ///
    /// # Errors
    ///
    /// * `StorageError::AlreadyExists` - If a task with the same ID already exists (without idempotency).
    /// * `TaskOperationError::IdempotencyConflict` - If the idempotency key was used with different input.
    pub async fn submit(
        &self,
        task_type: impl Into<String>,
        input: Value,
        options: SubmitOptions,
    ) -> Result<Task, StorageError> {
        let task_type_str = task_type.into();
        let mut task = Task::new(task_type_str.clone(), input.clone());
        // Recompute shard based on queue's configured prefix length
        task.shard = Task::shard_from_id_with_len(&task.id, self.config.shard_prefix_len);
        let now = self.now().await?;
        task.created_at = now;
        // Use schedule_at if provided, otherwise task is immediately available
        task.available_at = options.schedule_at.unwrap_or(now);
        task.updated_at = now;

        if let Some(timeout) = options.timeout_seconds {
            task.timeout_seconds = timeout;
        }
        if let Some(retries) = options.max_retries {
            task.max_retries = retries;
        }
        if let Some(policy) = options.retry_policy.clone() {
            task.retry_policy = policy;
        }
        if let Some(max_reschedules) = options.max_reschedules {
            task.max_reschedules = Some(max_reschedules);
        }

        // Handle TTL/expiration
        if let Some(ttl) = options.ttl_seconds {
            task.expires_at =
                Some(now + ChronoDuration::seconds(i64::try_from(ttl).unwrap_or(i64::MAX)));
        } else if let Some(expires_at) = options.expires_at {
            task.expires_at = Some(expires_at);
        }

        // Handle idempotency if a key is provided
        if let Some(ref idempotency_key) = options.idempotency_key {
            let scope = options.idempotency_scope.clone().unwrap_or_default();
            let ttl_days = options
                .idempotency_ttl_days
                .unwrap_or(DEFAULT_IDEMPOTENCY_TTL_DAYS);

            // Check for existing idempotency record
            match self
                .check_idempotency(
                    idempotency_key,
                    &task_type_str,
                    &input,
                    &scope,
                    ttl_days,
                    &task,
                )
                .await
            {
                Ok(Some(existing_task)) => {
                    // Return existing task
                    return Ok(existing_task);
                }
                Ok(None) => {
                    // No existing record (or expired), proceed with task creation
                }
                Err(IdempotencyError::Conflict(e)) => {
                    return Err(StorageError::S3Error(format!(
                        "Idempotency conflict: {}",
                        e.message
                    )));
                }
                Err(IdempotencyError::Storage(e)) => {
                    return Err(e);
                }
            }
        }

        // Handle payload refs if enabled
        if options.use_payload_refs {
            let threshold = options
                .payload_ref_threshold_bytes
                .unwrap_or(DEFAULT_PAYLOAD_REF_THRESHOLD_BYTES);

            if should_offload_payload(&task.input, threshold) {
                // Store input payload separately
                let input_ref =
                    store_payload(&self.client, task.id, PayloadType::Input, &task.input).await?;
                task.input_ref = Some(input_ref);
                // Replace input with minimal placeholder
                task.input = Value::Null;
            }
        }

        // Serialize task
        let body = serde_json::to_vec(&task)
            .map_err(|e| StorageError::SerializationError(e.to_string()))?;

        // PUT task with If-None-Match (create only if doesn't exist)
        self.client
            .put_object(&task.key(), body, PutCondition::IfNoneMatch)
            .await
            .map_err(|e| match e {
                StorageError::PreconditionFailed { key } => StorageError::AlreadyExists { key },
                other => other,
            })?;

        // PUT ready index (best-effort in hint mode; errors are logged)
        if let Err(e) = self.create_index(&task.ready_index_key()).await {
            tracing::warn!(
                key = %task.ready_index_key(),
                error = %e,
                "Failed to create ready index"
            );
        }

        counter!("qo.tasks.submitted", "task_type" => task.task_type.clone()).increment(1);

        Ok(task)
    }

    /// Gets a task by ID, returning the task and its current `ETag`.
    ///
    /// # Arguments
    ///
    /// * `task_id` - The UUID of the task to retrieve
    ///
    /// # Returns
    ///
    /// `Some((task, etag))` if the task exists, `None` if not found.
    pub async fn get(&self, task_id: Uuid) -> Result<Option<(Task, String)>, StorageError> {
        let shard = Task::shard_from_id_with_len(&task_id, self.config.shard_prefix_len);
        let key = format!("tasks/{shard}/{task_id}.json");

        match self.client.get_object(&key).await {
            Ok((body, etag)) => {
                let task: Task = serde_json::from_slice(&body)
                    .map_err(|e| StorageError::SerializationError(e.to_string()))?;
                Ok(Some((task, etag)))
            }
            Err(StorageError::NotFound { .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Gets the version history of a task.
    ///
    /// Returns all versions of the task object, ordered by last modified time
    /// (newest first). This is useful for debugging and auditing task state changes.
    ///
    /// # Arguments
    ///
    /// * `task_id` - The UUID of the task
    ///
    /// # Returns
    ///
    /// A vector of task versions, from newest to oldest.
    pub async fn get_history(&self, task_id: Uuid) -> Result<Vec<Task>, StorageError> {
        let shard = Task::shard_from_id_with_len(&task_id, self.config.shard_prefix_len);
        let key = format!("tasks/{shard}/{task_id}.json");

        let versions = self.client.list_object_versions(&key).await?;

        let mut tasks = Vec::with_capacity(versions.len());
        for version in versions {
            let body = self
                .client
                .get_object_version(&key, &version.version_id)
                .await?;
            let task: Task = serde_json::from_slice(&body)
                .map_err(|e| StorageError::SerializationError(e.to_string()))?;
            tasks.push(task);
        }

        Ok(tasks)
    }

    /// Lists tasks in a shard with optional status filter.
    ///
    /// # Arguments
    ///
    /// * `shard` - The shard to list (single hex character, 0-f)
    /// * `status_filter` - Optional status to filter by
    /// * `limit` - Maximum number of tasks to return
    ///
    /// # Returns
    ///
    /// A vector of tasks matching the criteria.
    pub async fn list(
        &self,
        shard: &str,
        status_filter: Option<TaskStatus>,
        limit: i32,
    ) -> Result<Vec<Task>, StorageError> {
        let prefix = format!("tasks/{shard}/");
        let keys = self.client.list_objects_paginated(&prefix, limit).await?;

        let mut tasks = Vec::new();
        for key in keys {
            if let Ok((body, _)) = self.client.get_object(&key).await {
                if let Ok(task) = serde_json::from_slice::<Task>(&body) {
                    if status_filter.is_none() || Some(task.status) == status_filter {
                        tasks.push(task);
                    }
                }
            }
        }

        Ok(tasks)
    }

    /// Lists tasks from ready index (pending tasks available for claiming).
    ///
    /// Filters out future buckets using authoritative S3 time.
    ///
    /// # Arguments
    ///
    /// * `shard` - The shard to list (single hex character, 0-f)
    /// * `limit` - Maximum number of task IDs to return
    ///
    /// # Returns
    ///
    /// A vector of task UUIDs that are ready for claiming.
    pub async fn list_ready(&self, shard: &str, limit: i32) -> Result<Vec<Uuid>, StorageError> {
        let prefix = format!("ready/{shard}/");
        let keys = self.client.list_objects_paginated(&prefix, limit).await?;

        let now = self.now().await?;
        let now_bucket = Task::epoch_minute_bucket_value(now);

        let mut task_ids = Vec::new();
        for key in keys {
            // Extract bucket + task ID from "ready/{shard}/{bucket}/{task_id}"
            let mut parts = key.split('/');
            let _ = parts.next(); // "ready"
            let _ = parts.next(); // shard
            let bucket_str = parts.next();
            let task_id_str = parts.next();

            let Some(bucket_str) = bucket_str else {
                continue;
            };
            let Ok(bucket) = bucket_str.parse::<i64>() else {
                continue;
            };
            if bucket > now_bucket {
                continue;
            }

            if let Some(task_id_str) = task_id_str {
                if let Ok(task_id) = Uuid::parse_str(task_id_str) {
                    task_ids.push(task_id);
                }
            }
        }

        Ok(task_ids)
    }

    /// Lists lease index keys for a shard.
    ///
    /// # Arguments
    ///
    /// * `shard` - The shard to list (single hex character, 0-f)
    /// * `limit` - Maximum number of keys to return
    ///
    /// # Returns
    ///
    /// A vector of lease index keys for the shard.
    pub async fn list_lease_keys(
        &self,
        shard: &str,
        limit: i32,
    ) -> Result<Vec<String>, StorageError> {
        let prefix = format!("leases/{shard}/");
        self.client.list_objects_paginated(&prefix, limit).await
    }

    /// Atomically updates a task with CAS (compare-and-swap).
    ///
    /// Uses the `ETag` from the previous get/update operation to ensure
    /// no concurrent modifications have occurred.
    ///
    /// # Arguments
    ///
    /// * `task` - The updated task to save
    /// * `expected_etag` - The `ETag` from the previous read
    ///
    /// # Returns
    ///
    /// The new `ETag` on success.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::PreconditionFailed` if the `ETag` doesn't match.
    pub async fn update(&self, task: &Task, expected_etag: &str) -> Result<String, StorageError> {
        let body = serde_json::to_vec(task)
            .map_err(|e| StorageError::SerializationError(e.to_string()))?;

        self.client
            .put_object(
                &task.key(),
                body,
                PutCondition::IfMatch(expected_etag.to_string()),
            )
            .await
    }

    /// Deletes an index object (best-effort).
    ///
    /// # Arguments
    ///
    /// * `key` - The index key to delete
    pub async fn delete_index(&self, key: &str) -> Result<(), StorageError> {
        self.client.delete_object(key).await
    }

    /// Creates an index object (best-effort).
    ///
    /// # Arguments
    ///
    /// * `key` - The index key to create
    pub async fn create_index(&self, key: &str) -> Result<(), StorageError> {
        self.client
            .put_object(key, vec![], PutCondition::None)
            .await
            .map(|_| ())
    }

    /// Returns a reference to the underlying S3 client.
    #[must_use]
    pub const fn client(&self) -> &S3Client {
        &self.client
    }

    /// Returns the current authoritative time from S3.
    ///
    /// This avoids reliance on local wall-clock time for correctness-critical
    /// scheduling and timeout decisions.
    pub async fn now(&self) -> Result<chrono::DateTime<chrono::Utc>, StorageError> {
        self.client.now().await
    }

    /// Extends the lease on a running task.
    ///
    /// This allows long-running tasks to prevent premature re-queue by the
    /// timeout monitor. The extension uses CAS (compare-and-swap) to ensure
    /// the task hasn't been modified by another worker.
    ///
    /// # Arguments
    ///
    /// * `task` - The task whose lease to extend
    /// * `etag` - The `ETag` from when the task was last read/updated
    /// * `additional_time` - How much time to add to the lease
    ///
    /// # Returns
    ///
    /// Returns the updated task with the new `etag` on success.
    ///
    /// # Errors
    ///
    /// * `TaskOperationError::LeaseExtensionFailed` - CAS failed (task modified)
    /// * `TaskOperationError::NotFound` - Task no longer exists
    /// * `TaskOperationError::Storage` - Underlying storage error
    ///
    /// # Example
    ///
    /// ```ignore
    /// let (task, etag) = queue.get(task_id).await?.unwrap();
    /// let (updated_task, new_etag) = queue.extend_lease(
    ///     &task,
    ///     &etag,
    ///     Duration::from_secs(60)
    /// ).await?;
    /// ```
    pub async fn extend_lease(
        &self,
        task: &Task,
        etag: &str,
        additional_time: Duration,
    ) -> Result<(Task, String), TaskOperationError> {
        // Verify task is in Running status
        if task.status != TaskStatus::Running {
            return Err(TaskOperationError::LeaseExtensionFailed {
                task_id: task.id,
                reason: format!("Task is not running (status: {:?})", task.status),
            });
        }

        // Verify task has a lease
        if task.lease_expires_at.is_none() {
            return Err(TaskOperationError::LeaseExtensionFailed {
                task_id: task.id,
                reason: "Task has no active lease".to_string(),
            });
        }

        // Get authoritative time from S3
        let now = self.now().await?;

        // Calculate new lease expiration
        // Use max(current_expiry, now) as base to ensure we always extend, never shorten
        let current_expiry = task.lease_expires_at.unwrap_or(now);
        let base_time = std::cmp::max(current_expiry, now);
        let additional_secs = i64::try_from(additional_time.as_secs()).unwrap_or(i64::MAX);
        let new_lease_expires_at = base_time + ChronoDuration::seconds(additional_secs);

        // Create updated task
        let mut updated_task = task.clone();
        updated_task.lease_expires_at = Some(new_lease_expires_at);
        updated_task.updated_at = now;

        // Update the old lease index key before updating the task
        let old_lease_key = task.lease_index_key();

        // Atomic update with If-Match condition
        let new_etag = match self.update(&updated_task, etag).await {
            Ok(etag) => etag,
            Err(StorageError::PreconditionFailed { .. }) => {
                return Err(TaskOperationError::LeaseExtensionFailed {
                    task_id: task.id,
                    reason: "Task was modified by another worker (ETag mismatch)".to_string(),
                });
            }
            Err(e) => return Err(TaskOperationError::Storage(e)),
        };

        // Best-effort: update lease index
        // Delete old lease index and create new one
        if let Some(old_key) = old_lease_key {
            if let Err(e) = self.delete_index(&old_key).await {
                tracing::warn!(key = %old_key, error = %e, "Failed to delete old lease index");
            }
        }
        if let Some(new_lease_key) = updated_task.lease_index_key() {
            if let Err(e) = self.create_index(&new_lease_key).await {
                tracing::warn!(key = %new_lease_key, error = %e, "Failed to create new lease index");
            }
        }

        Ok((updated_task, new_etag))
    }

    /// Loads the input data for a task, handling payload refs transparently.
    ///
    /// If the task has an `input_ref`, loads the data from S3. Otherwise,
    /// returns the inline input data.
    ///
    /// # Arguments
    ///
    /// * `task` - The task to load input for
    ///
    /// # Returns
    ///
    /// The task input as a JSON value.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let (task, _etag) = queue.get(task_id).await?.unwrap();
    /// let input = queue.load_task_input(&task).await?;
    /// ```
    pub async fn load_task_input(&self, task: &Task) -> Result<Value, StorageError> {
        if let Some(ref input_ref) = task.input_ref {
            load_payload(&self.client, input_ref).await
        } else {
            Ok(task.input.clone())
        }
    }

    /// Loads the output data for a task, handling payload refs transparently.
    ///
    /// If the task has an `output_ref`, loads the data from S3. Otherwise,
    /// returns the inline output data.
    ///
    /// # Arguments
    ///
    /// * `task` - The task to load output for
    ///
    /// # Returns
    ///
    /// The task output as an optional JSON value (None if not completed).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let (task, _etag) = queue.get(task_id).await?.unwrap();
    /// if let Some(output) = queue.load_task_output(&task).await? {
    ///     println!("Output: {:?}", output);
    /// }
    /// ```
    pub async fn load_task_output(&self, task: &Task) -> Result<Option<Value>, StorageError> {
        if let Some(ref output_ref) = task.output_ref {
            let output = load_payload(&self.client, output_ref).await?;
            Ok(Some(output))
        } else {
            Ok(task.output.clone())
        }
    }

    /// Cancels a pending task.
    ///
    /// Only tasks in `Pending` status can be cancelled. Running tasks require
    /// cooperative cancellation through the `cancel_requested` flag.
    ///
    /// # Arguments
    ///
    /// * `task_id` - The UUID of the task to cancel
    ///
    /// # Returns
    ///
    /// The cancelled task on success.
    ///
    /// # Errors
    ///
    /// * `StorageError::NotFound` - Task does not exist
    /// * `StorageError::PreconditionFailed` - Task is not in Pending status or was modified concurrently
    pub async fn cancel(&self, task_id: Uuid) -> Result<Task, StorageError> {
        self.cancel_with_reason(task_id, None, None).await
    }

    /// Cancels a pending task with an optional reason and `cancelled_by` identifier.
    ///
    /// Only tasks in `Pending` status can be cancelled. Running tasks require
    /// cooperative cancellation through the `cancel_requested` flag.
    ///
    /// # Arguments
    ///
    /// * `task_id` - The UUID of the task to cancel
    /// * `reason` - Optional reason for cancellation (stored in `last_error`)
    /// * `cancelled_by` - Optional identifier for who/what cancelled the task
    ///
    /// # Returns
    ///
    /// The cancelled task on success.
    ///
    /// # Errors
    ///
    /// * `StorageError::NotFound` - Task does not exist
    /// * `StorageError::PreconditionFailed` - Task is not in Pending status or was modified concurrently
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Cancel a pending task with reason
    /// let task = queue.cancel_with_reason(
    ///     task_id,
    ///     Some("No longer needed"),
    ///     Some("user:alice"),
    /// ).await?;
    /// ```
    pub async fn cancel_with_reason(
        &self,
        task_id: Uuid,
        reason: Option<&str>,
        cancelled_by: Option<&str>,
    ) -> Result<Task, StorageError> {
        // Step 1: GET task with ETag
        let (task, etag) = self
            .get(task_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                key: format!("task {task_id}"),
            })?;

        // Step 2: Verify task is in Pending status
        if task.status != TaskStatus::Pending {
            return Err(StorageError::PreconditionFailed {
                key: format!(
                    "Cannot cancel task with status {:?} (only Pending tasks can be cancelled)",
                    task.status
                ),
            });
        }

        // Step 3: Update task state
        let now = self.now().await?;
        let mut updated = task.clone();
        updated.status = TaskStatus::Cancelled;
        updated.cancelled_at = Some(now);
        updated.cancelled_by = cancelled_by.map(String::from);
        updated.updated_at = now;
        if let Some(r) = reason {
            updated.last_error = Some(r.to_string());
        }

        // Step 4: Atomic update with If-Match condition
        self.update(&updated, &etag).await?;

        // Step 5: Best-effort delete ready index
        if let Err(e) = self.delete_index(&task.ready_index_key()).await {
            tracing::warn!(key = %task.ready_index_key(), error = %e, "Failed to delete ready index during cancellation");
        }

        counter!("qo.tasks.cancelled", "task_type" => updated.task_type.clone()).increment(1);

        Ok(updated)
    }

    /// Request cancellation of a running task (cooperative).
    /// Sets `cancel_requested=true` so the handler can check and exit early.
    /// Only works on Running tasks - for Pending tasks, use [`cancel()`](Self::cancel) directly.
    ///
    /// # Arguments
    ///
    /// * `task_id` - The UUID of the task to request cancellation for
    ///
    /// # Returns
    ///
    /// The updated task with `cancel_requested` set to true.
    ///
    /// # Errors
    ///
    /// * `StorageError::NotFound` - Task does not exist
    /// * `StorageError::PreconditionFailed` - Task is not in Running status or was modified concurrently
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Request cancellation of a running task
    /// let task = queue.request_cancellation(task_id).await?;
    /// assert!(task.cancel_requested);
    /// ```
    pub async fn request_cancellation(&self, task_id: Uuid) -> Result<Task, StorageError> {
        // Step 1: GET task with ETag
        let (task, etag) = self
            .get(task_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                key: format!("task {task_id}"),
            })?;

        // Step 2: Verify task is in Running status
        if task.status != TaskStatus::Running {
            return Err(StorageError::PreconditionFailed {
                key: format!(
                    "Cannot request cancellation for task with status {:?} (only Running tasks can have cancellation requested)",
                    task.status
                ),
            });
        }

        // Step 3: Set cancel_requested = true
        let now = self.now().await?;
        let mut updated = task.clone();
        updated.cancel_requested = true;
        updated.updated_at = now;

        // Step 4: CAS update
        self.update(&updated, &etag).await?;

        tracing::info!(task_id = %task_id, "Cancellation requested for running task");

        Ok(updated)
    }

    /// Cancel multiple tasks in parallel.
    ///
    /// Each cancellation is independent - some may succeed while others fail.
    /// Uses the same CAS pattern as `cancel_with_reason`.
    ///
    /// # Arguments
    ///
    /// * `task_ids` - The UUIDs of the tasks to cancel
    /// * `reason` - Optional reason for cancellation (stored in `last_error`)
    /// * `cancelled_by` - Optional identifier for who/what cancelled the tasks
    ///
    /// # Returns
    ///
    /// A vector of results, one for each task ID in the same order as input.
    /// Each result is either the cancelled task or an error.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let results = queue.cancel_many(
    ///     vec![task_id_1, task_id_2, task_id_3],
    ///     Some("Batch cleanup"),
    ///     Some("admin"),
    /// ).await;
    ///
    /// for result in results {
    ///     match result {
    ///         Ok(task) => println!("Cancelled: {}", task.id),
    ///         Err(e) => println!("Failed: {}", e),
    ///     }
    /// }
    /// ```
    pub async fn cancel_many(
        &self,
        task_ids: Vec<Uuid>,
        reason: Option<&str>,
        cancelled_by: Option<&str>,
    ) -> Vec<Result<Task, StorageError>> {
        let futures = task_ids
            .into_iter()
            .map(|task_id| self.cancel_with_reason(task_id, reason, cancelled_by));

        join_all(futures).await
    }

    /// Cancel all pending tasks of a specific type.
    ///
    /// Scans all shards for pending tasks matching the given type and cancels them.
    /// Uses parallel cancellation internally via `cancel_many`.
    ///
    /// # Arguments
    ///
    /// * `task_type` - The type of tasks to cancel (e.g., `send_email`)
    /// * `reason` - Optional reason for cancellation (stored in `last_error`)
    /// * `cancelled_by` - Optional identifier for who/what cancelled the tasks
    ///
    /// # Returns
    ///
    /// A result containing successfully cancelled tasks and any failures.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let result = queue.cancel_by_type(
    ///     "send_email",
    ///     Some("Email service deprecated"),
    ///     Some("system"),
    /// ).await?;
    ///
    /// println!("Cancelled {} tasks", result.cancelled.len());
    /// if !result.failed.is_empty() {
    ///     println!("Failed to cancel {} tasks", result.failed.len());
    /// }
    /// ```
    pub async fn cancel_by_type(
        &self,
        task_type: &str,
        reason: Option<&str>,
        cancelled_by: Option<&str>,
    ) -> Result<CancelByTypeResult, StorageError> {
        // Step 1: List all shards
        let shards = self.all_shards();

        // Step 2: For each shard, list ALL pending tasks (no limit)
        let mut matching_task_ids = Vec::new();
        for shard in shards {
            let tasks = self
                .list(&shard, Some(TaskStatus::Pending), i32::MAX)
                .await?;

            // Step 3: Filter by task_type
            for task in tasks {
                if task.task_type == task_type {
                    matching_task_ids.push(task.id);
                }
            }
        }

        // Step 4: Call cancel_many on matching tasks
        let results = self
            .cancel_many(matching_task_ids.clone(), reason, cancelled_by)
            .await;

        // Step 5: Separate successes and failures
        let mut cancelled = Vec::new();
        let mut failed = Vec::new();

        for (task_id, result) in matching_task_ids.into_iter().zip(results) {
            match result {
                Ok(task) => cancelled.push(task),
                Err(e) => failed.push((task_id, e)),
            }
        }

        Ok(CancelByTypeResult { cancelled, failed })
    }

    /// Marks a pending task as expired.
    ///
    /// Only tasks in `Pending` status can be marked as expired.
    /// Running tasks continue to completion (expiration only affects pending tasks).
    ///
    /// # Arguments
    ///
    /// * `task_id` - The UUID of the task to mark as expired
    ///
    /// # Returns
    ///
    /// The expired task on success.
    ///
    /// # Errors
    ///
    /// * `StorageError::NotFound` - Task does not exist
    /// * `StorageError::PreconditionFailed` - Task is not in Pending status or was modified concurrently
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Mark an expired task
    /// let task = queue.mark_expired(task_id).await?;
    /// assert_eq!(task.status, TaskStatus::Expired);
    /// ```
    pub async fn mark_expired(&self, task_id: Uuid) -> Result<Task, StorageError> {
        // Step 1: GET task with ETag
        let (task, etag) = self
            .get(task_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                key: format!("task {task_id}"),
            })?;

        // Step 2: Verify task is in Pending status
        if task.status != TaskStatus::Pending {
            return Err(StorageError::PreconditionFailed {
                key: format!(
                    "Cannot expire task with status {:?} (only Pending tasks can expire)",
                    task.status
                ),
            });
        }

        // Step 3: Update task state
        let now = self.now().await?;
        let mut updated = task.clone();
        updated.status = TaskStatus::Expired;
        updated.expired_at = Some(now);
        updated.updated_at = now;

        // Step 4: Atomic update with If-Match condition
        self.update(&updated, &etag).await?;

        // Step 5: Best-effort delete ready index
        if let Err(e) = self.delete_index(&task.ready_index_key()).await {
            tracing::warn!(key = %task.ready_index_key(), error = %e, "Failed to delete ready index during expiration");
        }

        counter!("qo.tasks.expired", "task_type" => updated.task_type.clone()).increment(1);

        Ok(updated)
    }
}
