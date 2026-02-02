//! Worker information model for the task queue.
//!
//! This module contains the [`WorkerInfo`] struct which represents a registered
//! worker in the distributed task queue system.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Information about a registered worker in the task queue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerInfo {
    /// Unique identifier for this worker.
    pub worker_id: String,

    /// When this worker started.
    pub started_at: DateTime<Utc>,

    /// Last heartbeat timestamp.
    pub last_heartbeat: DateTime<Utc>,

    /// Which shards this worker is polling (e.g., `["0", "1", "a", "b"]`).
    pub shards: Vec<String>,

    /// Task currently being executed, if any.
    pub current_task: Option<Uuid>,

    /// Lifetime count of completed tasks.
    pub tasks_completed: u64,

    /// Lifetime count of failed tasks.
    pub tasks_failed: u64,
}

impl WorkerInfo {
    /// Creates a new `WorkerInfo` with the given ID and shards.
    #[must_use]
    pub fn new(worker_id: impl Into<String>, shards: Vec<String>) -> Self {
        let now = Utc::now();
        Self {
            worker_id: worker_id.into(),
            started_at: now,
            last_heartbeat: now,
            shards,
            current_task: None,
            tasks_completed: 0,
            tasks_failed: 0,
        }
    }

    /// Returns the S3 key for this worker's registration object.
    #[must_use]
    pub fn key(&self) -> String {
        format!("workers/{}.json", self.worker_id)
    }

    /// Checks if the worker is healthy based on heartbeat age.
    #[must_use]
    pub fn is_healthy_at(&self, now: DateTime<Utc>, threshold: Duration) -> bool {
        let age = now.signed_duration_since(self.last_heartbeat);
        age < threshold
    }

    /// Updates the heartbeat timestamp to the provided time.
    pub const fn touch_at(&mut self, now: DateTime<Utc>) {
        self.last_heartbeat = now;
    }

    /// Sets the current task being processed, using the provided time.
    pub const fn set_current_task_at(&mut self, task_id: Option<Uuid>, now: DateTime<Utc>) {
        self.current_task = task_id;
        self.touch_at(now);
    }

    /// Records a completed task using the provided time.
    pub const fn record_completed_at(&mut self, now: DateTime<Utc>) {
        self.tasks_completed += 1;
        self.current_task = None;
        self.touch_at(now);
    }

    /// Records a failed task using the provided time.
    pub const fn record_failed_at(&mut self, now: DateTime<Utc>) {
        self.tasks_failed += 1;
        self.current_task = None;
        self.touch_at(now);
    }
}

impl Default for WorkerInfo {
    fn default() -> Self {
        let uuid_str = Uuid::new_v4().to_string();
        let short_id = uuid_str.split('-').next().unwrap_or("unknown");
        Self::new(
            format!("worker-{short_id}"),
            (0..16).map(|i| format!("{i:x}")).collect(),
        )
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration as StdDuration;

    #[test]
    fn test_new_creates_with_correct_initial_values() {
        let shards = vec!["0".to_string(), "1".to_string(), "a".to_string()];
        let worker = WorkerInfo::new("test-worker-1", shards.clone());

        assert_eq!(worker.worker_id, "test-worker-1");
        assert_eq!(worker.shards, shards);
        assert!(worker.current_task.is_none());
        assert_eq!(worker.tasks_completed, 0);
        assert_eq!(worker.tasks_failed, 0);
        // started_at and last_heartbeat should be very close to now
        let now = Utc::now();
        let diff = now.signed_duration_since(worker.started_at);
        assert!(diff.num_seconds() < 1);
        assert_eq!(worker.started_at, worker.last_heartbeat);
    }

    #[test]
    fn test_key_returns_correct_path() {
        let worker = WorkerInfo::new("my-worker-id", vec![]);
        assert_eq!(worker.key(), "workers/my-worker-id.json");

        let worker2 = WorkerInfo::new("worker-abc-123", vec!["0".to_string()]);
        assert_eq!(worker2.key(), "workers/worker-abc-123.json");
    }

    #[test]
    fn test_is_healthy_with_fresh_heartbeat() {
        let worker = WorkerInfo::new("healthy-worker", vec![]);
        let now = Utc::now();
        // A fresh worker should be healthy with a 30-second threshold
        assert!(worker.is_healthy_at(now, Duration::seconds(30)));
        // Should also be healthy with a 1-second threshold (just created)
        assert!(worker.is_healthy_at(now, Duration::seconds(1)));
    }

    #[test]
    fn test_is_healthy_with_stale_heartbeat() {
        let mut worker = WorkerInfo::new("stale-worker", vec![]);
        let now = Utc::now();
        // Manually set the heartbeat to the past
        worker.last_heartbeat = now - Duration::seconds(60);

        // Should be unhealthy with a 30-second threshold
        assert!(!worker.is_healthy_at(now, Duration::seconds(30)));
        // Should be healthy with a 120-second threshold
        assert!(worker.is_healthy_at(now, Duration::seconds(120)));
    }

    #[test]
    fn test_touch_updates_heartbeat() {
        let mut worker = WorkerInfo::new("touch-worker", vec![]);
        let original_heartbeat = worker.last_heartbeat;

        // Small delay to ensure time difference
        thread::sleep(StdDuration::from_millis(10));

        worker.touch_at(Utc::now());

        assert!(worker.last_heartbeat > original_heartbeat);
    }

    #[test]
    fn test_set_current_task() {
        let mut worker = WorkerInfo::new("task-worker", vec![]);
        assert!(worker.current_task.is_none());
        let now = Utc::now();

        let task_id = Uuid::new_v4();
        worker.set_current_task_at(Some(task_id), now);

        assert_eq!(worker.current_task, Some(task_id));

        worker.set_current_task_at(None, now);
        assert!(worker.current_task.is_none());
    }

    #[test]
    fn test_record_completed_updates_state() {
        let mut worker = WorkerInfo::new("complete-worker", vec![]);
        let now = Utc::now();
        let task_id = Uuid::new_v4();
        worker.set_current_task_at(Some(task_id), now);

        assert_eq!(worker.tasks_completed, 0);
        assert!(worker.current_task.is_some());

        worker.record_completed_at(now);

        assert_eq!(worker.tasks_completed, 1);
        assert!(worker.current_task.is_none());

        worker.set_current_task_at(Some(Uuid::new_v4()), now);
        worker.record_completed_at(now);

        assert_eq!(worker.tasks_completed, 2);
    }

    #[test]
    fn test_record_failed_updates_state() {
        let mut worker = WorkerInfo::new("fail-worker", vec![]);
        let now = Utc::now();
        let task_id = Uuid::new_v4();
        worker.set_current_task_at(Some(task_id), now);

        assert_eq!(worker.tasks_failed, 0);
        assert!(worker.current_task.is_some());

        worker.record_failed_at(now);

        assert_eq!(worker.tasks_failed, 1);
        assert!(worker.current_task.is_none());

        worker.set_current_task_at(Some(Uuid::new_v4()), now);
        worker.record_failed_at(now);

        assert_eq!(worker.tasks_failed, 2);
    }

    #[test]
    fn test_json_serialization_roundtrip() {
        let shards = vec!["0".to_string(), "f".to_string()];
        let mut worker = WorkerInfo::new("json-worker", shards);
        let now = Utc::now();
        let task_id = Uuid::new_v4();
        worker.set_current_task_at(Some(task_id), now);
        worker.tasks_completed = 42;
        worker.tasks_failed = 3;

        let json = serde_json::to_string(&worker).expect("serialization should succeed");
        let deserialized: WorkerInfo =
            serde_json::from_str(&json).expect("deserialization should succeed");

        assert_eq!(deserialized.worker_id, worker.worker_id);
        assert_eq!(deserialized.started_at, worker.started_at);
        assert_eq!(deserialized.last_heartbeat, worker.last_heartbeat);
        assert_eq!(deserialized.shards, worker.shards);
        assert_eq!(deserialized.current_task, worker.current_task);
        assert_eq!(deserialized.tasks_completed, worker.tasks_completed);
        assert_eq!(deserialized.tasks_failed, worker.tasks_failed);
    }

    #[test]
    fn test_default_creates_valid_worker() {
        let worker = WorkerInfo::default();

        assert!(worker.worker_id.starts_with("worker-"));
        assert_eq!(worker.shards.len(), 16);
        // Verify shards are hex digits 0-f
        let expected_shards: Vec<String> = (0..16).map(|i| format!("{i:x}")).collect();
        assert_eq!(worker.shards, expected_shards);
        assert!(worker.current_task.is_none());
        assert_eq!(worker.tasks_completed, 0);
        assert_eq!(worker.tasks_failed, 0);
    }
}
