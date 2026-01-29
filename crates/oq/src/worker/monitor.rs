//! Timeout monitor for detecting and recovering stuck tasks.
//!
//! This module provides a background monitor that periodically scans for tasks
//! that have exceeded their lease timeout and recovers them by either retrying
//! (if retries remain) or marking them as failed.
//!
//! ## Incremental Scanning
//!
//! To avoid O(total objects) scans at scale, the monitor uses incremental
//! cursors that persist scan position across cycles. Each shard maintains
//! its own cursor, allowing the monitor to process a limited number of tasks
//! per cycle and continue from where it left off.

use chrono::Duration;
use metrics::counter;
use std::collections::HashMap;
use std::time::{Duration as StdDuration, Instant};
use tokio::sync::watch;
use uuid::Uuid;

use crate::models::{TaskStatus, WorkerInfo};
use crate::queue::Queue;
use crate::storage::StorageError;
use crate::worker::{IndexSweeper, SweeperConfig};

/// Configuration for the timeout monitor.
#[derive(Debug, Clone)]
pub struct MonitorConfig {
    /// How often to check for timed-out tasks.
    pub check_interval: StdDuration,
    /// How long before a worker is considered dead (based on heartbeat).
    pub worker_health_threshold: Duration,
    /// How often to run the index sweeper (None disables periodic sweeping).
    pub sweep_interval: Option<StdDuration>,
    /// Page size for the index sweeper.
    pub sweep_page_size: i32,
    /// Maximum number of tasks to process per scan cycle (default: 100).
    /// Setting this limits work per cycle to avoid O(total objects) scans.
    pub max_tasks_per_scan: usize,
}

impl Default for MonitorConfig {
    fn default() -> Self {
        Self {
            check_interval: StdDuration::from_secs(30),
            worker_health_threshold: Duration::seconds(60),
            sweep_interval: Some(StdDuration::from_secs(300)),
            sweep_page_size: 1000,
            max_tasks_per_scan: 100,
        }
    }
}

/// Cursor state for incremental shard scanning.
#[derive(Debug, Clone, Default)]
pub struct ShardCursor {
    /// S3 continuation token for resuming LIST operations.
    pub continuation_token: Option<String>,
    /// Whether we've completed a full scan of this shard.
    pub completed_full_scan: bool,
}

/// The timeout monitor that runs in the background.
///
/// The monitor periodically scans all shards for tasks that have exceeded their
/// lease timeout. When a timed-out task is found, it is either:
/// - Transitioned to Pending with exponential backoff (if retries remain)
/// - Transitioned to Failed (if max retries exhausted)
///
/// ## Incremental Scanning
///
/// The monitor uses incremental cursors to avoid full scans. Each scan cycle
/// processes up to `max_tasks_per_scan` tasks and remembers where it stopped.
/// On the next cycle, scanning continues from that position.
pub struct TimeoutMonitor {
    queue: Queue,
    config: MonitorConfig,
    /// Cursor state per shard for incremental scanning.
    shard_cursors: HashMap<String, ShardCursor>,
}

impl TimeoutMonitor {
    /// Creates a new timeout monitor.
    ///
    /// # Arguments
    ///
    /// * `queue` - The queue to monitor for timed-out tasks
    /// * `config` - Configuration for the monitor
    #[must_use]
    pub fn new(queue: Queue, config: MonitorConfig) -> Self {
        Self {
            queue,
            config,
            shard_cursors: HashMap::new(),
        }
    }

    /// Returns the current cursor state for a shard.
    #[must_use]
    pub fn get_cursor(&self, shard: &str) -> Option<&ShardCursor> {
        self.shard_cursors.get(shard)
    }

    /// Resets the cursor for a shard, forcing a fresh scan from the beginning.
    pub fn reset_cursor(&mut self, shard: &str) {
        self.shard_cursors.remove(shard);
    }

    /// Resets all cursors, forcing fresh scans for all shards.
    pub fn reset_all_cursors(&mut self) {
        self.shard_cursors.clear();
    }

    /// Checks a single shard for timed-out tasks using incremental scanning.
    ///
    /// Scans the lease index for the given shard and recovers any tasks
    /// whose leases have expired. Uses incremental cursors to limit work
    /// per cycle and avoid O(total objects) scans.
    ///
    /// # Arguments
    ///
    /// * `shard` - The shard to check (single hex character, 0-f)
    ///
    /// # Returns
    ///
    /// The IDs of recovered tasks on success.
    ///
    /// # Errors
    ///
    /// Returns `StorageError` if listing lease keys fails.
    #[allow(clippy::cognitive_complexity, clippy::too_many_lines)]
    pub async fn check_shard(&mut self, shard: &str) -> Result<Vec<Uuid>, StorageError> {
        let mut recovered = Vec::new();

        // Get or create cursor for this shard and extract continuation token
        let continuation_token = {
            let cursor = self.shard_cursors.entry(shard.to_string()).or_default();
            cursor.continuation_token.clone()
        };

        // List lease index for this shard with limit and continuation token
        let prefix = format!("leases/{shard}/");
        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        let limit = self.config.max_tasks_per_scan as i32;
        let (keys, next_token) = self
            .queue
            .client()
            .list_objects(&prefix, limit, continuation_token.as_deref())
            .await?;

        // Update cursor for next iteration (re-access via entry since we know it exists)
        {
            let cursor = self.shard_cursors.entry(shard.to_string()).or_default();
            if let Some(token) = next_token {
                cursor.continuation_token = Some(token);
                cursor.completed_full_scan = false;
            } else {
                // We've reached the end of this shard, reset for next full scan
                cursor.continuation_token = None;
                cursor.completed_full_scan = true;
            }
        }

        let now = self.queue.now().await?;

        for key in keys {
            // Extract task ID from "leases/{shard}/{bucket}/{task_id}"
            let Some(id_str) = key.split('/').next_back() else {
                continue;
            };
            let Ok(task_id) = Uuid::parse_str(id_str) else {
                tracing::debug!("Skipping invalid task ID in lease key: {key}");
                continue;
            };

            // Get the task to check its status
            let (mut task, etag) = match self.queue.get(task_id).await {
                Ok(Some((task, etag))) => (task, etag),
                Ok(None) => {
                    // Task doesn't exist but lease index does - clean up orphaned index
                    tracing::debug!(
                        "Cleaning up orphaned lease index for non-existent task: {task_id}"
                    );
                    if let Err(e) = self.queue.delete_index(&key).await {
                        tracing::warn!("Failed to delete orphaned lease index {key}: {e}");
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!("Error getting task {task_id}: {e}");
                    continue;
                }
            };

            // Only process tasks that are Running and have expired leases
            if task.status != TaskStatus::Running || !task.is_lease_expired_at(now) {
                continue;
            }

            tracing::info!(
                task_id = %task.id,
                worker_id = ?task.worker_id,
                lease_expires_at = ?task.lease_expires_at,
                "Detected timed-out task"
            );

            // Store the old lease index key before clearing lease fields
            let old_lease_key = task.lease_index_key();

            if task.can_retry() {
                // Calculate backoff delay
                let backoff = task.retry_policy.calculate_delay(task.retry_count);
                #[allow(clippy::cast_possible_truncation)]
                let backoff_chrono =
                    Duration::milliseconds(backoff.as_millis().min(i64::MAX as u128) as i64);

                // Update task for retry
                task.status = TaskStatus::Pending;
                task.retry_count += 1;
                task.available_at = now + backoff_chrono;
                task.lease_expires_at = None;
                task.lease_id = None;
                task.worker_id = None;
                task.last_error = Some("Timeout - task exceeded lease duration".to_string());
                task.updated_at = now;

                tracing::info!(
                    task_id = %task.id,
                    retry_count = task.retry_count,
                    max_retries = task.max_retries,
                    available_at = %task.available_at,
                    "Retrying timed-out task with backoff"
                );

                // Attempt to update the task with CAS
                match self.queue.update(&task, &etag).await {
                    Ok(_) => {
                        // Delete old lease index
                        if let Some(ref lease_key) = old_lease_key {
                            if let Err(e) = self.queue.delete_index(lease_key).await {
                                tracing::warn!("Failed to delete lease index {lease_key}: {e}");
                            }
                        }

                        // Create ready index for the task to be picked up
                        let ready_key = task.ready_index_key();
                        if let Err(e) = self.queue.create_index(&ready_key).await {
                            tracing::warn!("Failed to create ready index {ready_key}: {e}");
                        }

                        counter!("oq.tasks.timeout_recovered").increment(1);
                        recovered.push(task.id);
                    }
                    Err(StorageError::PreconditionFailed { .. }) => {
                        // Task was modified by another process - skip silently
                        tracing::debug!(
                            task_id = %task.id,
                            "Task was modified concurrently, skipping recovery"
                        );
                    }
                    Err(e) => {
                        tracing::warn!("Failed to update timed-out task {}: {e}", task.id);
                    }
                }
            } else {
                // Max retries exhausted - mark as failed
                task.status = TaskStatus::Failed;
                task.lease_expires_at = None;
                task.lease_id = None;
                task.completed_at = Some(now);
                task.last_error = Some(format!(
                    "Timeout after max retries ({} attempts)",
                    task.retry_count
                ));
                task.updated_at = now;

                tracing::warn!(
                    task_id = %task.id,
                    retry_count = task.retry_count,
                    max_retries = task.max_retries,
                    "Task failed after exhausting all retries"
                );

                // Attempt to update the task with CAS
                match self.queue.update(&task, &etag).await {
                    Ok(_) => {
                        // Delete old lease index
                        if let Some(ref lease_key) = old_lease_key {
                            if let Err(e) = self.queue.delete_index(lease_key).await {
                                tracing::warn!("Failed to delete lease index {lease_key}: {e}");
                            }
                        }

                        counter!("oq.tasks.retries_exhausted").increment(1);
                        recovered.push(task.id);
                    }
                    Err(StorageError::PreconditionFailed { .. }) => {
                        tracing::debug!(
                            task_id = %task.id,
                            "Task was modified concurrently, skipping recovery"
                        );
                    }
                    Err(e) => {
                        tracing::warn!("Failed to mark task {} as failed: {e}", task.id);
                    }
                }
            }
        }

        Ok(recovered)
    }

    /// Checks all 16 shards for timed-out tasks using incremental scanning.
    ///
    /// Iterates through all hex shards (0-f) and recovers timed-out tasks.
    /// Each shard is scanned incrementally, processing up to `max_tasks_per_scan`
    /// tasks per cycle. Errors in individual shards are logged but don't prevent
    /// checking other shards.
    ///
    /// # Returns
    ///
    /// The IDs of all recovered tasks across all shards.
    ///
    /// # Errors
    ///
    /// This method does not return errors for individual shard failures.
    /// It only returns `Ok` with the list of recovered task IDs.
    pub async fn check_all_shards(&mut self) -> Result<Vec<Uuid>, StorageError> {
        let mut all_recovered = Vec::new();

        for i in 0..16 {
            let shard = format!("{i:x}");
            match self.check_shard(&shard).await {
                Ok(recovered) => all_recovered.extend(recovered),
                Err(e) => {
                    // Log error but continue checking other shards
                    tracing::warn!("Error checking shard {shard}: {e}");
                }
            }
        }

        Ok(all_recovered)
    }

    /// Checks if all shards have completed at least one full scan.
    ///
    /// This is useful to verify that the monitor has inspected all leases
    /// at least once since startup.
    #[must_use]
    pub fn all_shards_scanned(&self) -> bool {
        for i in 0..16 {
            let shard = format!("{i:x}");
            match self.shard_cursors.get(&shard) {
                Some(cursor) if cursor.completed_full_scan => {}
                _ => return false,
            }
        }
        true
    }

    /// Checks worker health by examining heartbeats.
    ///
    /// Lists all registered workers and checks if their heartbeats are stale
    /// based on the configured health threshold.
    ///
    /// # Returns
    ///
    /// The IDs of unhealthy (stale heartbeat) workers.
    ///
    /// # Errors
    ///
    /// Returns `StorageError` if listing workers fails.
    pub async fn check_worker_health(&self) -> Result<Vec<String>, StorageError> {
        let mut unhealthy = Vec::new();
        let now = self.queue.now().await?;

        // List all workers
        let prefix = "workers/";
        let keys = self
            .queue
            .client()
            .list_objects_paginated(prefix, i32::MAX)
            .await?;

        for key in keys {
            // Get worker info
            let (body, _etag) = match self.queue.client().get_object(&key).await {
                Ok(result) => result,
                Err(StorageError::NotFound { .. }) => continue,
                Err(e) => {
                    tracing::warn!("Error getting worker {key}: {e}");
                    continue;
                }
            };

            let worker: WorkerInfo = match serde_json::from_slice(&body) {
                Ok(w) => w,
                Err(e) => {
                    tracing::warn!("Error deserializing worker {key}: {e}");
                    continue;
                }
            };

            if !worker.is_healthy_at(now, self.config.worker_health_threshold) {
                unhealthy.push(worker.worker_id.clone());
            }
        }

        Ok(unhealthy)
    }

    /// Runs the monitor loop until shutdown signal.
    ///
    /// Periodically checks all shards for timed-out tasks and logs worker
    /// health warnings. The loop continues until the shutdown signal is received.
    /// Uses incremental scanning to avoid O(total objects) cost per cycle.
    ///
    /// # Arguments
    ///
    /// * `shutdown` - A watch receiver that signals when to shut down
    ///
    /// # Returns
    ///
    /// `Ok(())` on clean shutdown.
    ///
    /// # Errors
    ///
    /// This method does not return errors from the monitoring loop.
    /// All errors are logged and the loop continues.
    #[allow(clippy::cognitive_complexity)]
    pub async fn run(&mut self, mut shutdown: watch::Receiver<bool>) -> Result<(), StorageError> {
        tracing::info!(
            check_interval = ?self.config.check_interval,
            worker_health_threshold = ?self.config.worker_health_threshold,
            max_tasks_per_scan = self.config.max_tasks_per_scan,
            "Timeout monitor started with incremental scanning"
        );

        let mut last_sweep = Instant::now();

        loop {
            tokio::select! {
                () = tokio::time::sleep(self.config.check_interval) => {
                    // Check for timed-out tasks
                    match self.check_all_shards().await {
                        Ok(recovered) => {
                            if !recovered.is_empty() {
                                tracing::info!(
                                    count = recovered.len(),
                                    task_ids = ?recovered,
                                    "Recovered timed-out tasks"
                                );
                            }
                        }
                        Err(e) => {
                            tracing::error!("Error in timeout check: {e}");
                        }
                    }

                    // Check worker health (optional, for logging)
                    match self.check_worker_health().await {
                        Ok(unhealthy) => {
                            for worker_id in unhealthy {
                                tracing::warn!(
                                    worker_id = %worker_id,
                                    "Worker appears unhealthy (stale heartbeat)"
                                );
                            }
                        }
                        Err(e) => {
                            tracing::debug!("Error checking worker health: {e}");
                        }
                    }

                    if let Some(interval) = self.config.sweep_interval {
                        if last_sweep.elapsed() >= interval {
                            let mut sweeper = IndexSweeper::new(
                                self.queue.clone(),
                                SweeperConfig {
                                    page_size: self.config.sweep_page_size,
                                    ..Default::default()
                                },
                            );
                            match sweeper.sweep_all_shards().await {
                                Ok(report) => tracing::info!(?report, "Index sweep completed"),
                                Err(e) => tracing::warn!("Index sweep failed: {e}"),
                            }
                            last_sweep = Instant::now();
                        }
                    }
                }
                () = async { shutdown.changed().await.ok(); } => {
                    if *shutdown.borrow() {
                        tracing::info!("Timeout monitor shutting down");
                        break;
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::Task;
    use chrono::Utc;
    use std::time::Duration as StdDuration;

    #[test]
    fn test_monitor_config_default() {
        let config = MonitorConfig::default();

        assert_eq!(config.check_interval, StdDuration::from_secs(30));
        assert_eq!(config.worker_health_threshold, Duration::seconds(60));
        assert_eq!(config.sweep_interval, Some(StdDuration::from_secs(300)));
        assert_eq!(config.sweep_page_size, 1000);
        assert_eq!(config.max_tasks_per_scan, 100);
    }

    #[test]
    fn test_monitor_config_custom() {
        let config = MonitorConfig {
            check_interval: StdDuration::from_secs(10),
            worker_health_threshold: Duration::seconds(120),
            sweep_interval: Some(StdDuration::from_secs(600)),
            sweep_page_size: 500,
            max_tasks_per_scan: 50,
        };

        assert_eq!(config.check_interval, StdDuration::from_secs(10));
        assert_eq!(config.worker_health_threshold, Duration::seconds(120));
        assert_eq!(config.sweep_interval, Some(StdDuration::from_secs(600)));
        assert_eq!(config.sweep_page_size, 500);
        assert_eq!(config.max_tasks_per_scan, 50);
    }

    #[test]
    fn test_shard_cursor_default() {
        let cursor = ShardCursor::default();
        assert!(cursor.continuation_token.is_none());
        assert!(!cursor.completed_full_scan);
    }

    #[test]
    fn test_task_can_retry_logic() {
        // Verify the retry logic we rely on in check_shard
        let mut task = Task {
            max_retries: 3,
            retry_count: 0,
            ..Default::default()
        };

        assert!(task.can_retry());

        task.retry_count = 2;
        assert!(task.can_retry());

        task.retry_count = 3;
        assert!(!task.can_retry());
    }

    #[test]
    fn test_task_is_lease_expired_logic() {
        // Verify the lease expiry logic we rely on in check_shard

        let now = Utc::now();
        // No lease_expires_at means not expired
        let task_no_lease = Task {
            lease_expires_at: None,
            ..Default::default()
        };
        assert!(!task_no_lease.is_lease_expired_at(now));

        // Future lease means not expired
        let task_future_lease = Task {
            lease_expires_at: Some(now + Duration::seconds(10)),
            ..Default::default()
        };
        assert!(!task_future_lease.is_lease_expired_at(now));

        // Past lease means expired
        let task_past_lease = Task {
            lease_expires_at: Some(now - Duration::seconds(10)),
            ..Default::default()
        };
        assert!(task_past_lease.is_lease_expired_at(now));
    }

    #[test]
    fn test_worker_is_healthy_logic() {
        // Verify the health check logic we rely on in check_worker_health
        let mut worker = WorkerInfo::new("test-worker", vec![]);
        let threshold = Duration::seconds(60);
        let now = Utc::now();

        // Fresh worker should be healthy
        assert!(worker.is_healthy_at(now, threshold));

        // Worker with stale heartbeat should be unhealthy
        worker.last_heartbeat = now - Duration::seconds(120);
        assert!(!worker.is_healthy_at(now, threshold));
    }

    #[test]
    fn test_backoff_calculation() {
        // Verify backoff calculation produces reasonable values
        let task = Task::default();
        let backoff = task.retry_policy.calculate_delay_without_jitter(0);

        // Default initial_interval_ms is 1000
        assert_eq!(backoff.as_millis(), 1000);

        // Second retry should be 2x (with default multiplier of 2.0)
        let backoff2 = task.retry_policy.calculate_delay_without_jitter(1);
        assert_eq!(backoff2.as_millis(), 2000);
    }

    #[test]
    #[allow(clippy::indexing_slicing)]
    fn test_shard_iteration() {
        // Verify we generate all 16 shards correctly
        let shards: Vec<String> = (0..16).map(|i| format!("{i:x}")).collect();

        assert_eq!(shards.len(), 16);
        assert_eq!(shards[0], "0");
        assert_eq!(shards[9], "9");
        assert_eq!(shards[10], "a");
        assert_eq!(shards[15], "f");
    }
}
