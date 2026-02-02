use std::str::FromStr;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use super::retry::RetryPolicy;

const EPOCH_MINUTE_BUCKET_WIDTH: usize = 10;

/// Represents the status of a task in the queue.
///
/// Tasks progress through states: `Pending` -> `Running` -> `Completed`/`Failed`.
/// Tasks can also be moved to `Archived` status for soft-deletion,
/// `Cancelled` if cancelled before completion, or `Expired` if they exceed their TTL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    /// Task is waiting to be claimed by a worker.
    Pending,
    /// Task has been claimed and is being executed.
    Running,
    /// Task completed successfully.
    Completed,
    /// Task failed after exhausting all retries.
    Failed,
    /// Task was cancelled before completion.
    Cancelled,
    /// Task has been soft-deleted/archived.
    Archived,
    /// Task expired before being claimed (exceeded TTL).
    Expired,
}

impl Default for TaskStatus {
    fn default() -> Self {
        Self::Pending
    }
}

impl TaskStatus {
    /// All status variants for exhaustive UI generation.
    pub const ALL: &'static [Self] = &[
        Self::Pending,
        Self::Running,
        Self::Completed,
        Self::Failed,
        Self::Cancelled,
        Self::Archived,
        Self::Expired,
    ];

    /// Display name for UI.
    #[must_use]
    pub const fn display_name(&self) -> &'static str {
        match self {
            Self::Pending => "Pending",
            Self::Running => "Running",
            Self::Completed => "Completed",
            Self::Failed => "Failed",
            Self::Cancelled => "Cancelled",
            Self::Archived => "Archived",
            Self::Expired => "Expired",
        }
    }

    /// Lowercase string for URL params.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Archived => "archived",
            Self::Expired => "expired",
        }
    }
}

impl FromStr for TaskStatus {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pending" => Ok(Self::Pending),
            "running" => Ok(Self::Running),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            "archived" => Ok(Self::Archived),
            "expired" => Ok(Self::Expired),
            _ => Err(()),
        }
    }
}

/// Represents errors that can occur during task execution.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", content = "message")]
pub enum TaskError {
    /// An error that can be retried (e.g., network timeout, temporary failure).
    Retryable(String),
    /// A permanent error that should not be retried (e.g., invalid input).
    Permanent(String),
    /// Task execution exceeded the timeout.
    Timeout,
    /// Task should be rescheduled for later execution.
    /// The value is the delay in seconds before the task becomes available again.
    #[serde(rename = "delay_seconds")]
    Reschedule(u64),
}

impl std::fmt::Display for TaskError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Retryable(msg) => write!(f, "Retryable error: {msg}"),
            Self::Permanent(msg) => write!(f, "Permanent error: {msg}"),
            Self::Timeout => write!(f, "Task execution timed out"),
            Self::Reschedule(delay) => write!(f, "Reschedule after {delay} seconds"),
        }
    }
}

impl std::error::Error for TaskError {}

/// A task in the distributed task queue.
///
/// Tasks are stored in S3 with optimistic concurrency control using `If-Match`/`ETag`.
/// Use `TaskBuilder` to create tasks with custom configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    /// Unique identifier for the task.
    pub id: Uuid,

    /// First hex character of UUID (0-f), used for sharding.
    pub shard: String,

    /// Type of task (e.g., `send_email`, `process_image`).
    /// Used to route tasks to appropriate handlers.
    pub task_type: String,

    /// Input data for the task as a JSON value.
    pub input: Value,

    /// Output data from task execution, if completed.
    pub output: Option<Value>,

    /// S3 key for externally stored input payload.
    /// When set, the actual input data is stored at this key instead of inline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_ref: Option<String>,

    /// S3 key for externally stored output payload.
    /// When set, the actual output data is stored at this key instead of inline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_ref: Option<String>,

    /// Maximum time in seconds a task can run before being considered timed out.
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,

    /// Maximum number of retry attempts before moving to DLQ.
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,

    /// Current retry attempt count.
    #[serde(default)]
    pub retry_count: u32,

    /// Policy for calculating retry delays.
    #[serde(default)]
    pub retry_policy: RetryPolicy,

    /// Current status of the task (`Pending`, `Running`, `Completed`, `Failed`, or `Archived`).
    #[serde(default)]
    pub status: TaskStatus,

    /// Timestamp when the task was created.
    pub created_at: DateTime<Utc>,

    /// Timestamp when the task becomes available for claiming (`available_at`).
    /// Enables backoff without sleep by scheduling into the future.
    pub available_at: DateTime<Utc>,

    /// Pre-computed deadline when the lease expires (`lease_expires_at`).
    /// Set when task is claimed, cleared when completed/failed.
    pub lease_expires_at: Option<DateTime<Utc>>,

    /// Claim token for current attempt. Prevents stale workers from
    /// completing tasks they no longer own.
    pub lease_id: Option<Uuid>,

    /// Number of times this task has been claimed (for debugging/history).
    #[serde(default)]
    pub attempt: u32,

    /// Last modification timestamp.
    pub updated_at: DateTime<Utc>,

    /// Timestamp when the task completed (successfully or failed).
    pub completed_at: Option<DateTime<Utc>>,

    /// ID of the worker currently processing this task.
    pub worker_id: Option<String>,

    /// Error message from the last failed execution attempt.
    pub last_error: Option<String>,

    /// Number of times this task has been rescheduled (for rate limiting/backpressure).
    #[serde(default)]
    pub reschedule_count: u32,

    /// Maximum number of reschedules before failing (None = unlimited).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_reschedules: Option<u32>,

    /// Whether cancellation has been requested for this task.
    /// Used for cooperative cancellation of running tasks.
    #[serde(default)]
    pub cancel_requested: bool,

    /// Timestamp when the task was cancelled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancelled_at: Option<DateTime<Utc>>,

    /// Who/what cancelled the task (e.g., "user:alice", "system").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancelled_by: Option<String>,

    /// When the task expires (becomes stale and should not be processed).
    /// None means the task never expires.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<DateTime<Utc>>,

    /// Timestamp when the task was marked as expired.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expired_at: Option<DateTime<Utc>>,
}

const fn default_timeout_seconds() -> u64 {
    300
}

const fn default_max_retries() -> u32 {
    3
}

impl Default for Task {
    fn default() -> Self {
        let now = Utc::now();
        let id = Uuid::new_v4();
        let shard = Self::shard_from_id(&id);
        Self {
            id,
            shard,
            task_type: String::new(),
            input: Value::Null,
            output: None,
            input_ref: None,
            output_ref: None,
            timeout_seconds: default_timeout_seconds(),
            max_retries: default_max_retries(),
            retry_count: 0,
            retry_policy: RetryPolicy::default(),
            status: TaskStatus::default(),
            created_at: now,
            available_at: now,
            lease_expires_at: None,
            lease_id: None,
            attempt: 0,
            updated_at: now,
            completed_at: None,
            worker_id: None,
            last_error: None,
            reschedule_count: 0,
            max_reschedules: None,
            cancel_requested: false,
            cancelled_at: None,
            cancelled_by: None,
            expires_at: None,
            expired_at: None,
        }
    }
}

impl Task {
    /// Creates a new task with the specified type and input.
    #[must_use]
    pub fn new(task_type: impl Into<String>, input: Value) -> Self {
        Self {
            task_type: task_type.into(),
            input,
            ..Default::default()
        }
    }

    /// Creates a new task builder for more customization.
    ///
    /// Returns a `TaskBuilder` for fluent configuration.
    #[must_use]
    pub fn builder(task_type: impl Into<String>, input: Value) -> TaskBuilder {
        TaskBuilder::new(task_type, input)
    }

    /// Extracts hex prefix from UUID for sharding.
    /// `prefix_len`=1 gives 16 shards (0-f), 2 gives 256 (00-ff), 3 gives 4096 (000-fff).
    #[must_use]
    pub fn shard_from_id_with_len(id: &Uuid, prefix_len: usize) -> String {
        id.to_string()
            .chars()
            .filter(|c| *c != '-')
            .take(prefix_len.clamp(1, 4)) // Clamp to 1-4
            .collect()
    }

    /// Extracts the first hex character from a UUID for sharding (backward-compatible default).
    #[must_use]
    pub fn shard_from_id(id: &Uuid) -> String {
        Self::shard_from_id_with_len(id, 1)
    }

    /// Returns the S3 key for this task: `tasks/{shard}/{id}.json`
    #[must_use]
    pub fn key(&self) -> String {
        format!("tasks/{}/{}.json", self.shard, self.id)
    }

    /// Returns true if the task is available for claiming (`available_at` <= now).
    #[must_use]
    pub fn is_available_at(&self, now: DateTime<Utc>) -> bool {
        now >= self.available_at
    }

    /// Returns true if the task's lease has expired.
    #[must_use]
    pub fn is_lease_expired_at(&self, now: DateTime<Utc>) -> bool {
        self.lease_expires_at
            .is_some_and(|expires_at| now > expires_at)
    }

    /// Returns true if the task has timed out based on `lease_expires_at`.
    #[must_use]
    pub fn is_timed_out_at(&self, now: DateTime<Utc>) -> bool {
        self.is_lease_expired_at(now)
    }

    /// Returns the ready index key: `ready/{shard}/{bucket}/{id}`
    /// where bucket is the fixed-width epoch-minute bucket derived from `available_at`.
    #[must_use]
    pub fn ready_index_key(&self) -> String {
        let bucket = Self::epoch_minute_bucket_string(self.available_at);
        format!("ready/{}/{}/{}", self.shard, bucket, self.id)
    }

    /// Returns the lease index key: `leases/{shard}/{bucket}/{id}`
    /// where bucket is the fixed-width epoch-minute bucket derived from `lease_expires_at`.
    /// Returns `None` if `lease_expires_at` is not set.
    #[must_use]
    pub fn lease_index_key(&self) -> Option<String> {
        self.lease_expires_at.map(|expires_at| {
            let bucket = Self::epoch_minute_bucket_string(expires_at);
            format!("leases/{}/{}/{}", self.shard, bucket, self.id)
        })
    }

    /// Returns the epoch-minute bucket value for a timestamp.
    #[must_use]
    pub const fn epoch_minute_bucket_value(timestamp: DateTime<Utc>) -> i64 {
        timestamp.timestamp().div_euclid(60)
    }

    /// Returns the fixed-width epoch-minute bucket string for a timestamp.
    #[must_use]
    pub fn epoch_minute_bucket_string(timestamp: DateTime<Utc>) -> String {
        Self::epoch_minute_bucket_string_from_value(Self::epoch_minute_bucket_value(timestamp))
    }

    /// Returns the fixed-width epoch-minute bucket string for a bucket value.
    #[must_use]
    #[allow(clippy::uninlined_format_args)]
    pub fn epoch_minute_bucket_string_from_value(bucket: i64) -> String {
        format!("{:0width$}", bucket, width = EPOCH_MINUTE_BUCKET_WIDTH)
    }

    /// Returns true if the task can be retried.
    #[must_use]
    pub const fn can_retry(&self) -> bool {
        self.retry_count < self.max_retries
    }

    /// Returns true if the task has expired (exceeded TTL) at the given time.
    /// A task with no `expires_at` never expires.
    #[must_use]
    pub fn is_expired_at(&self, now: DateTime<Utc>) -> bool {
        self.expires_at.is_some_and(|expires| now >= expires)
    }
}

/// Builder for creating `Task` instances with custom configuration.
pub struct TaskBuilder {
    task: Task,
}

impl TaskBuilder {
    /// Creates a new `TaskBuilder` with the required fields.
    #[must_use]
    pub fn new(task_type: impl Into<String>, input: Value) -> Self {
        Self {
            task: Task::new(task_type, input),
        }
    }

    /// Sets a specific task ID instead of generating one.
    /// Also updates the shard to match the new ID.
    #[must_use]
    pub fn with_id(mut self, id: Uuid) -> Self {
        self.task.id = id;
        self.task.shard = Task::shard_from_id(&id);
        self
    }

    /// Sets a specific task ID instead of generating one.
    /// Also updates the shard to match the new ID.
    #[must_use]
    pub fn id(mut self, id: Uuid) -> Self {
        self.task.id = id;
        self.task.shard = Task::shard_from_id(&id);
        self
    }

    /// Sets the timeout in seconds.
    #[must_use]
    pub const fn timeout_seconds(mut self, timeout: u64) -> Self {
        self.task.timeout_seconds = timeout;
        self
    }

    /// Sets the timeout in seconds.
    #[must_use]
    pub const fn with_timeout_seconds(mut self, timeout: u64) -> Self {
        self.task.timeout_seconds = timeout;
        self
    }

    /// Sets the maximum number of retries.
    #[must_use]
    pub const fn max_retries(mut self, max_retries: u32) -> Self {
        self.task.max_retries = max_retries;
        self
    }

    /// Sets the maximum number of retries.
    #[must_use]
    pub const fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.task.max_retries = max_retries;
        self
    }

    /// Sets the retry policy.
    #[allow(clippy::missing_const_for_fn)]
    #[must_use]
    pub fn retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.task.retry_policy = policy;
        self
    }

    /// Sets the retry policy.
    #[allow(clippy::missing_const_for_fn)]
    #[must_use]
    pub fn with_retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.task.retry_policy = policy;
        self
    }

    /// Sets when the task becomes available for claiming (`available_at`).
    #[allow(clippy::missing_const_for_fn)]
    #[must_use]
    pub fn available_at(mut self, available_at: DateTime<Utc>) -> Self {
        self.task.available_at = available_at;
        self
    }

    /// Sets when the task becomes available for claiming (`available_at`).
    #[allow(clippy::missing_const_for_fn)]
    #[must_use]
    pub fn with_available_at(mut self, available_at: DateTime<Utc>) -> Self {
        self.task.available_at = available_at;
        self
    }

    /// Sets the maximum number of reschedules before failing.
    #[must_use]
    pub const fn max_reschedules(mut self, max_reschedules: u32) -> Self {
        self.task.max_reschedules = Some(max_reschedules);
        self
    }

    /// Sets the maximum number of reschedules before failing.
    #[must_use]
    pub const fn with_max_reschedules(mut self, max_reschedules: u32) -> Self {
        self.task.max_reschedules = Some(max_reschedules);
        self
    }

    /// Builds the `Task` instance.
    #[must_use]
    pub fn build(self) -> Task {
        self.task
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::field_reassign_with_default
)]
mod tests {
    use super::*;
    use chrono::Duration;

    #[test]
    fn test_task_default() {
        let task = Task::default();
        assert_eq!(task.timeout_seconds, 300);
        assert_eq!(task.max_retries, 3);
        assert_eq!(task.retry_count, 0);
        assert_eq!(task.status, TaskStatus::Pending);
        assert!(task.output.is_none());
        assert!(task.completed_at.is_none());
        assert!(task.worker_id.is_none());
        assert!(task.last_error.is_none());
        assert!(task.lease_expires_at.is_none());
        assert!(task.lease_id.is_none());
        assert_eq!(task.attempt, 0);
        // Shard should be set from ID
        assert_eq!(task.shard.len(), 1);
        assert!(task.shard.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_task_new() {
        let input = serde_json::json!({"email": "test@example.com"});
        let task = Task::new("send_email", input.clone());

        assert_eq!(task.task_type, "send_email");
        assert_eq!(task.input, input);
        assert_eq!(task.status, TaskStatus::Pending);
        // Verify shard is computed
        assert_eq!(task.shard, Task::shard_from_id(&task.id));
    }

    #[test]
    fn test_task_builder() {
        let input = serde_json::json!({"url": "https://example.com"});
        let custom_policy = RetryPolicy::new(500, 30000, 1.5, 0.1);
        let future_time = Utc::now() + Duration::minutes(5);

        let task = Task::builder("fetch_url", input.clone())
            .timeout_seconds(120)
            .max_retries(5)
            .retry_policy(custom_policy.clone())
            .available_at(future_time)
            .build();

        assert_eq!(task.task_type, "fetch_url");
        assert_eq!(task.input, input);
        assert_eq!(task.timeout_seconds, 120);
        assert_eq!(task.max_retries, 5);
        assert_eq!(task.retry_policy, custom_policy);
        assert_eq!(task.available_at, future_time);
    }

    #[test]
    fn test_task_builder_with_custom_id() {
        let custom_id =
            Uuid::parse_str("a1234567-89ab-cdef-0123-456789abcdef").expect("valid UUID for test");
        let input = serde_json::json!({});

        let task = Task::builder("test", input).id(custom_id).build();

        assert_eq!(task.id, custom_id);
        assert_eq!(task.shard, "a");
    }

    #[test]
    fn test_can_retry() {
        let mut task = Task::default();
        task.max_retries = 3;

        task.retry_count = 0;
        assert!(task.can_retry());

        task.retry_count = 2;
        assert!(task.can_retry());

        task.retry_count = 3;
        assert!(!task.can_retry());

        task.retry_count = 4;
        assert!(!task.can_retry());
    }

    #[test]
    fn test_task_serialization() {
        let input = serde_json::json!({"key": "value"});
        let task = Task::new("test_task", input);

        let json = serde_json::to_string(&task).expect("serialization should succeed");
        let deserialized: Task =
            serde_json::from_str(&json).expect("deserialization should succeed");

        assert_eq!(task.id, deserialized.id);
        assert_eq!(task.task_type, deserialized.task_type);
        assert_eq!(task.input, deserialized.input);
        assert_eq!(task.status, deserialized.status);
        assert_eq!(task.shard, deserialized.shard);
    }

    #[test]
    fn test_task_status_serialization() {
        assert_eq!(
            serde_json::to_string(&TaskStatus::Pending).expect("serialize pending"),
            "\"pending\""
        );
        assert_eq!(
            serde_json::to_string(&TaskStatus::Running).expect("serialize running"),
            "\"running\""
        );
        assert_eq!(
            serde_json::to_string(&TaskStatus::Completed).expect("serialize completed"),
            "\"completed\""
        );
        assert_eq!(
            serde_json::to_string(&TaskStatus::Failed).expect("serialize failed"),
            "\"failed\""
        );
        assert_eq!(
            serde_json::to_string(&TaskStatus::Cancelled).expect("serialize cancelled"),
            "\"cancelled\""
        );
        assert_eq!(
            serde_json::to_string(&TaskStatus::Archived).expect("serialize archived"),
            "\"archived\""
        );
        assert_eq!(
            serde_json::to_string(&TaskStatus::Expired).expect("serialize expired"),
            "\"expired\""
        );
    }

    #[test]
    fn test_task_error_serialization() {
        let retryable = TaskError::Retryable("connection failed".to_string());
        let json = serde_json::to_string(&retryable).expect("serialize retryable error");
        let deserialized: TaskError =
            serde_json::from_str(&json).expect("deserialize retryable error");
        assert_eq!(retryable, deserialized);

        let permanent = TaskError::Permanent("invalid input".to_string());
        let json = serde_json::to_string(&permanent).expect("serialize permanent error");
        let deserialized: TaskError =
            serde_json::from_str(&json).expect("deserialize permanent error");
        assert_eq!(permanent, deserialized);

        let timeout = TaskError::Timeout;
        let json = serde_json::to_string(&timeout).expect("serialize timeout error");
        let deserialized: TaskError =
            serde_json::from_str(&json).expect("deserialize timeout error");
        assert_eq!(timeout, deserialized);
    }

    #[test]
    fn test_task_error_display() {
        assert_eq!(
            TaskError::Retryable("test".to_string()).to_string(),
            "Retryable error: test"
        );
        assert_eq!(
            TaskError::Permanent("test".to_string()).to_string(),
            "Permanent error: test"
        );
        assert_eq!(TaskError::Timeout.to_string(), "Task execution timed out");
    }

    #[test]
    fn test_shard_from_id_extracts_correct_hex_char() {
        // Test various UUIDs with known first characters
        let test_cases = [
            ("01234567-89ab-cdef-0123-456789abcdef", "0"),
            ("a1234567-89ab-cdef-0123-456789abcdef", "a"),
            ("f1234567-89ab-cdef-0123-456789abcdef", "f"),
            ("51234567-89ab-cdef-0123-456789abcdef", "5"),
            ("b1234567-89ab-cdef-0123-456789abcdef", "b"),
        ];

        for (uuid_str, expected_shard) in test_cases {
            let id = Uuid::parse_str(uuid_str).expect("valid UUID for test");
            let shard = Task::shard_from_id(&id);
            assert_eq!(
                shard, expected_shard,
                "UUID {uuid_str} should have shard {expected_shard}"
            );
        }
    }

    #[test]
    fn test_shard_from_id_with_len_prefix_1() {
        // UUID: a1b2c3d4-e5f6-7890-abcd-ef1234567890
        let id =
            Uuid::parse_str("a1b2c3d4-e5f6-7890-abcd-ef1234567890").expect("valid UUID for test");
        assert_eq!(Task::shard_from_id_with_len(&id, 1), "a");
    }

    #[test]
    fn test_shard_from_id_with_len_prefix_2() {
        // UUID: a1b2c3d4-e5f6-7890-abcd-ef1234567890
        // Without hyphens: a1b2c3d4e5f67890abcdef1234567890
        let id =
            Uuid::parse_str("a1b2c3d4-e5f6-7890-abcd-ef1234567890").expect("valid UUID for test");
        assert_eq!(Task::shard_from_id_with_len(&id, 2), "a1");
    }

    #[test]
    fn test_shard_from_id_with_len_prefix_3() {
        let id =
            Uuid::parse_str("a1b2c3d4-e5f6-7890-abcd-ef1234567890").expect("valid UUID for test");
        assert_eq!(Task::shard_from_id_with_len(&id, 3), "a1b");
    }

    #[test]
    fn test_shard_from_id_with_len_prefix_4() {
        let id =
            Uuid::parse_str("a1b2c3d4-e5f6-7890-abcd-ef1234567890").expect("valid UUID for test");
        assert_eq!(Task::shard_from_id_with_len(&id, 4), "a1b2");
    }

    #[test]
    fn test_shard_from_id_with_len_clamped_to_min() {
        let id =
            Uuid::parse_str("a1b2c3d4-e5f6-7890-abcd-ef1234567890").expect("valid UUID for test");
        // 0 should be clamped to 1
        assert_eq!(Task::shard_from_id_with_len(&id, 0), "a");
    }

    #[test]
    fn test_shard_from_id_with_len_clamped_to_max() {
        let id =
            Uuid::parse_str("a1b2c3d4-e5f6-7890-abcd-ef1234567890").expect("valid UUID for test");
        // 10 should be clamped to 4
        assert_eq!(Task::shard_from_id_with_len(&id, 10), "a1b2");
    }

    #[test]
    fn test_shard_from_id_with_len_skips_hyphens() {
        // UUID with hyphen at position 8: 01234567-89ab-cdef-0123-456789abcdef
        // Without hyphens: 0123456789abcdef0123456789abcdef
        let id =
            Uuid::parse_str("01234567-89ab-cdef-0123-456789abcdef").expect("valid UUID for test");
        // prefix_len=2 should give "01", not "0-"
        assert_eq!(Task::shard_from_id_with_len(&id, 2), "01");
        // prefix_len=4 should give "0123", crossing the hyphen position
        assert_eq!(Task::shard_from_id_with_len(&id, 4), "0123");
    }

    #[test]
    fn test_shard_from_id_backward_compatible() {
        // Verify that shard_from_id() returns the same as shard_from_id_with_len(&id, 1)
        let id =
            Uuid::parse_str("a1b2c3d4-e5f6-7890-abcd-ef1234567890").expect("valid UUID for test");
        assert_eq!(
            Task::shard_from_id(&id),
            Task::shard_from_id_with_len(&id, 1)
        );
    }

    #[test]
    fn test_key_returns_correct_path() {
        let id = Uuid::parse_str("a1234567-89ab-cdef-0123-456789abcdef").expect("valid UUID");
        let mut task = Task::default();
        task.id = id;
        task.shard = Task::shard_from_id(&id);

        assert_eq!(
            task.key(),
            "tasks/a/a1234567-89ab-cdef-0123-456789abcdef.json"
        );
    }

    #[test]
    fn test_is_available_logic() {
        let mut task = Task::default();
        let now = Utc::now();

        // Task with available_at in the past should be available
        task.available_at = now - Duration::seconds(10);
        assert!(task.is_available_at(now));

        // Task with available_at in the future should not be available
        task.available_at = now + Duration::seconds(10);
        assert!(!task.is_available_at(now));

        // Task with available_at at now should be available
        task.available_at = now;
        assert!(task.is_available_at(now));
    }

    #[test]
    fn test_is_lease_expired_logic() {
        let mut task = Task::default();
        let now = Utc::now();

        // No lease_expires_at means not expired
        task.lease_expires_at = None;
        assert!(!task.is_lease_expired_at(now));

        // Lease in the future means not expired
        task.lease_expires_at = Some(now + Duration::seconds(10));
        assert!(!task.is_lease_expired_at(now));

        // Lease in the past means expired
        task.lease_expires_at = Some(now - Duration::seconds(10));
        assert!(task.is_lease_expired_at(now));
    }

    #[test]
    fn test_is_timed_out_uses_lease_expires_at() {
        let mut task = Task::default();
        let now = Utc::now();

        // No lease means not timed out
        task.lease_expires_at = None;
        assert!(!task.is_timed_out_at(now));

        // Expired lease means timed out
        task.lease_expires_at = Some(now - Duration::seconds(10));
        assert!(task.is_timed_out_at(now));

        // Future lease means not timed out
        task.lease_expires_at = Some(now + Duration::seconds(10));
        assert!(!task.is_timed_out_at(now));
    }

    #[test]
    fn test_ready_index_key_format() {
        let id = Uuid::parse_str("a1234567-89ab-cdef-0123-456789abcdef").expect("valid UUID");
        let mut task = Task::default();
        task.id = id;
        task.shard = Task::shard_from_id(&id);

        // Set a specific available_at time
        task.available_at = chrono::DateTime::parse_from_rfc3339("2024-01-15T10:30:00Z")
            .expect("valid datetime")
            .with_timezone(&Utc);

        let key = task.ready_index_key();
        assert_eq!(
            key,
            "ready/a/0028421910/a1234567-89ab-cdef-0123-456789abcdef"
        );
    }

    #[test]
    fn test_lease_index_key_format() {
        let id = Uuid::parse_str("b1234567-89ab-cdef-0123-456789abcdef").expect("valid UUID");
        let mut task = Task::default();
        task.id = id;
        task.shard = Task::shard_from_id(&id);

        // No lease_expires_at should return None
        task.lease_expires_at = None;
        assert!(task.lease_index_key().is_none());

        // With lease_expires_at should return the key
        task.lease_expires_at = Some(
            chrono::DateTime::parse_from_rfc3339("2024-01-15T10:45:00Z")
                .expect("valid datetime")
                .with_timezone(&Utc),
        );

        let key = task.lease_index_key();
        assert_eq!(
            key,
            Some("leases/b/0028421925/b1234567-89ab-cdef-0123-456789abcdef".to_string())
        );
    }

    #[test]
    fn test_ready_index_key_bucket_padding() {
        let id = Uuid::parse_str("c1234567-89ab-cdef-0123-456789abcdef").expect("valid UUID");
        let mut task = Task::default();
        task.id = id;
        task.shard = Task::shard_from_id(&id);

        // Test epoch minute 5 gets padded to a fixed-width bucket.
        task.available_at = chrono::DateTime::parse_from_rfc3339("1970-01-01T00:05:00Z")
            .expect("valid datetime")
            .with_timezone(&Utc);

        let key = task.ready_index_key();
        assert_eq!(
            key,
            "ready/c/0000000005/c1234567-89ab-cdef-0123-456789abcdef"
        );
    }

    #[test]
    fn test_archived_status() {
        let mut task = Task::default();
        task.status = TaskStatus::Archived;

        let json = serde_json::to_string(&task).expect("serialize task");
        assert!(json.contains("\"archived\""));

        let deserialized: Task = serde_json::from_str(&json).expect("deserialize task");
        assert_eq!(deserialized.status, TaskStatus::Archived);
    }

    #[test]
    fn test_expired_status() {
        let mut task = Task::default();
        task.status = TaskStatus::Expired;

        let json = serde_json::to_string(&task).expect("serialize task");
        assert!(json.contains("\"expired\""));

        let deserialized: Task = serde_json::from_str(&json).expect("deserialize task");
        assert_eq!(deserialized.status, TaskStatus::Expired);
    }

    #[test]
    fn test_is_expired_at_logic() {
        let mut task = Task::default();
        let now = Utc::now();

        // No expires_at means not expired
        task.expires_at = None;
        assert!(!task.is_expired_at(now));

        // Expiration in the future means not expired
        task.expires_at = Some(now + Duration::seconds(10));
        assert!(!task.is_expired_at(now));

        // Expiration in the past means expired
        task.expires_at = Some(now - Duration::seconds(10));
        assert!(task.is_expired_at(now));

        // Expiration exactly at now means expired (>= comparison)
        task.expires_at = Some(now);
        assert!(task.is_expired_at(now));
    }

    #[test]
    fn test_expires_at_serialization() {
        let mut task = Task::default();
        let expiry = Utc::now() + Duration::hours(1);
        task.expires_at = Some(expiry);

        let json = serde_json::to_string(&task).expect("serialize task");
        assert!(json.contains("\"expires_at\""));

        let deserialized: Task = serde_json::from_str(&json).expect("deserialize task");
        assert_eq!(deserialized.expires_at, Some(expiry));
    }
}
