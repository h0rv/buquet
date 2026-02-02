//! Worker runner with polling loop and graceful shutdown.
//!
//! This module provides the main worker loop that:
//! - Polls for ready tasks across assigned shards
//! - Claims and executes tasks using registered handlers
//! - Handles task completion, retry, and failure transitions
//! - Supports graceful shutdown with configurable grace period
//! - Updates worker heartbeat periodically

use std::time::Duration;

use tokio::signal;
use tokio::sync::watch;
use uuid::Uuid;

use crate::models::{Task, TaskError, TaskStatus};
use crate::storage::{PutCondition, StorageError};

use super::{
    complete_task, execute_task_with_context, fail_task, reschedule_task, retry_task,
    try_claim_for_handler, ClaimResult, ExecutionResult, PollingStrategy, ShardLeaseConfig,
    ShardLeaseManager, Worker,
};

/// Configuration for the worker runner.
#[derive(Debug, Clone)]
pub struct RunnerConfig {
    /// Polling strategy (fixed or adaptive).
    pub polling: PollingStrategy,
    /// Initial poll interval when no tasks found (deprecated, use `polling` instead).
    #[deprecated(since = "0.2.0", note = "Use `polling` field instead")]
    pub poll_interval: Duration,
    /// Maximum poll interval (backoff cap) (deprecated, use `polling` instead).
    #[deprecated(since = "0.2.0", note = "Use `polling` field instead")]
    pub max_poll_interval: Duration,
    /// Backoff multiplier when no tasks found (deprecated, use `polling` instead).
    #[deprecated(since = "0.2.0", note = "Use `polling` field instead")]
    pub backoff_multiplier: f64,
    /// Grace period for in-flight task on shutdown.
    pub shutdown_grace_period: Duration,
    /// How often to update heartbeat.
    pub heartbeat_interval: Duration,
    /// How to discover ready tasks.
    pub index_mode: IndexMode,
    /// Page size when listing ready index keys.
    pub ready_page_size: i32,
    /// Page size when scanning tasks in log-scan mode.
    pub log_scan_page_size: i32,
    /// Optional shard leasing configuration.
    /// When enabled, workers dynamically acquire leases on shards instead of
    /// polling all assigned shards.
    pub shard_lease_config: Option<ShardLeaseConfig>,
}

impl Default for RunnerConfig {
    #[allow(deprecated)]
    fn default() -> Self {
        Self {
            polling: PollingStrategy::default(),
            poll_interval: Duration::from_millis(100),
            max_poll_interval: Duration::from_secs(5),
            backoff_multiplier: 1.5,
            shutdown_grace_period: Duration::from_secs(30),
            heartbeat_interval: Duration::from_secs(10),
            index_mode: IndexMode::Hybrid,
            ready_page_size: 100,
            log_scan_page_size: 50,
            shard_lease_config: None,
        }
    }
}

/// Controls how the worker discovers ready tasks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexMode {
    /// Use ready index only.
    IndexOnly,
    /// Use ready index first; fall back to a log scan if index is empty.
    Hybrid,
    /// Skip ready index and scan the task log directly.
    LogScan,
}

/// Creates a shutdown signal channel.
///
/// Returns a sender/receiver pair where:
/// - The sender can be used to trigger shutdown by sending `true`
/// - The receiver can be watched to detect shutdown requests
///
/// # Example
///
/// ```ignore
/// let (shutdown_tx, shutdown_rx) = shutdown_signal();
/// // Later, trigger shutdown:
/// shutdown_tx.send(true).ok();
/// ```
#[must_use]
pub fn shutdown_signal() -> (watch::Sender<bool>, watch::Receiver<bool>) {
    watch::channel(false)
}

/// Spawns a task that listens for SIGTERM/SIGINT and triggers shutdown.
///
/// This function blocks until either:
/// - SIGINT (Ctrl+C) is received
/// - SIGTERM is received (Unix only)
///
/// When a signal is received, it sends `true` through the shutdown channel.
///
/// # Arguments
///
/// * `shutdown_tx` - The sender side of a shutdown signal channel
///
/// # Example
///
/// ```ignore
/// let (shutdown_tx, shutdown_rx) = shutdown_signal();
/// tokio::spawn(async move {
///     wait_for_shutdown_signal(shutdown_tx).await;
/// });
/// ```
#[allow(clippy::cognitive_complexity)]
pub async fn wait_for_shutdown_signal(shutdown_tx: watch::Sender<bool>) {
    let ctrl_c = async {
        if let Err(e) = signal::ctrl_c().await {
            tracing::error!("Failed to listen for Ctrl+C: {}", e);
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match signal::unix::signal(signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(e) => {
                tracing::error!("Failed to listen for SIGTERM: {}", e);
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {
            tracing::info!("Received SIGINT (Ctrl+C), initiating shutdown");
        }
        () = terminate => {
            tracing::info!("Received SIGTERM, initiating shutdown");
        }
    }

    if let Err(e) = shutdown_tx.send(true) {
        tracing::error!("Failed to send shutdown signal: {}", e);
    }
}

impl Worker {
    /// Registers the worker in S3 (creates `workers/{worker_id}.json`).
    ///
    /// This creates a worker registration object that can be used by other
    /// components to discover active workers and monitor their health.
    ///
    /// # Errors
    ///
    /// Returns `StorageError` if the S3 operation fails.
    pub async fn register(&mut self) -> Result<(), StorageError> {
        let now = self.queue.now().await?;
        self.info.started_at = now;
        self.info.last_heartbeat = now;
        let body = serde_json::to_vec(&self.info)
            .map_err(|e| StorageError::SerializationError(e.to_string()))?;
        self.queue
            .client()
            .put_object(&self.info.key(), body, PutCondition::None)
            .await?;

        // Check bucket versioning and log reminder about lifecycle policies
        Self::log_versioning_guidance(self.queue.client()).await;

        Ok(())
    }

    /// Logs guidance about S3 lifecycle policies when versioning is enabled.
    ///
    /// This is an informational message to remind operators to configure
    /// lifecycle policies for version retention. Without lifecycle policies,
    /// version history grows indefinitely and can drive storage costs.
    async fn log_versioning_guidance(client: &crate::storage::S3Client) {
        // Only log once per process using a static flag
        use std::sync::atomic::{AtomicBool, Ordering};
        static LOGGED: AtomicBool = AtomicBool::new(false);

        if LOGGED.swap(true, Ordering::SeqCst) {
            return;
        }

        // Check if versioning is enabled
        if client.ensure_bucket_versioning_enabled().await.is_ok() {
            tracing::info!(
                target: "buquet::versioning",
                "S3 bucket versioning is enabled. For production use, configure S3 lifecycle \
                 policies to manage version retention and control storage costs. \
                 See: https://docs.rs/buquet/latest/guides/version-history-retention"
            );
        }
        // Versioning not enabled or check failed - no guidance needed
        // (versioning errors are already logged elsewhere)
    }

    /// Updates the worker's heartbeat in S3.
    ///
    /// This should be called periodically to indicate the worker is still
    /// alive and processing tasks. Other components can use the heartbeat
    /// timestamp to detect stale workers.
    ///
    /// # Errors
    ///
    /// Returns `StorageError` if the S3 operation fails.
    pub async fn heartbeat(&mut self) -> Result<(), StorageError> {
        let now = self.queue.now().await?;
        self.info.touch_at(now);
        let body = serde_json::to_vec(&self.info)
            .map_err(|e| StorageError::SerializationError(e.to_string()))?;
        self.queue
            .client()
            .put_object(&self.info.key(), body, PutCondition::None)
            .await?;
        Ok(())
    }

    /// Deregisters the worker from S3.
    ///
    /// This removes the worker registration object, indicating the worker
    /// has cleanly shut down.
    ///
    /// # Errors
    ///
    /// Returns `StorageError` if the S3 operation fails.
    pub async fn deregister(&self) -> Result<(), StorageError> {
        self.queue.client().delete_object(&self.info.key()).await
    }

    /// Polls once for a task to claim across all assigned shards.
    ///
    /// Iterates through the worker's assigned shards and attempts to claim
    /// the first available task. Uses atomic CAS operations to ensure
    /// exactly-once claiming.
    ///
    /// # Returns
    ///
    /// * `Ok(Some((task, etag, lease_id)))` - Successfully claimed a task
    /// * `Ok(None)` - No tasks available to claim
    /// * `Err(StorageError)` - A storage error occurred
    ///
    /// # Errors
    ///
    /// Returns `StorageError` for S3 communication errors.
    pub async fn poll_once(&mut self) -> Result<Option<(Task, String, Uuid)>, StorageError> {
        self.poll_once_with_config(&RunnerConfig::default()).await
    }

    async fn poll_once_with_config(
        &mut self,
        config: &RunnerConfig,
    ) -> Result<Option<(Task, String, Uuid)>, StorageError> {
        let shard_count = self.info.shards.len();
        if shard_count == 0 {
            return Ok(None);
        }
        let start = self.shard_cursor % shard_count;
        let next_cursor = (start + 1) % shard_count;

        for offset in 0..shard_count {
            let idx = (start + offset) % shard_count;
            // Clone shard to avoid holding a reference to self during mutable method calls
            let shard = self.info.shards[idx].clone();
            let index_candidates = if config.index_mode == IndexMode::LogScan {
                Vec::new()
            } else {
                self.list_ready_page(&shard, config.ready_page_size).await?
            };

            if config.index_mode != IndexMode::LogScan {
                for task_id in index_candidates.iter().copied() {
                    if let Some(result) = self.try_claim(task_id).await? {
                        self.shard_cursor = next_cursor;
                        return Ok(Some(result));
                    }
                }
            }

            if matches!(config.index_mode, IndexMode::Hybrid | IndexMode::LogScan) {
                let log_candidates = self
                    .list_ready_log_scan(&shard, config.log_scan_page_size)
                    .await?;
                for task_id in log_candidates {
                    if let Some(result) = self.try_claim(task_id).await? {
                        self.shard_cursor = next_cursor;
                        return Ok(Some(result));
                    }
                }
            }
        }

        self.shard_cursor = next_cursor;
        Ok(None)
    }

    async fn try_claim(
        &mut self,
        task_id: Uuid,
    ) -> Result<Option<(Task, String, Uuid)>, StorageError> {
        // Use the shared try_claim_for_handler function which checks for handler
        // before claiming. This is the canonical implementation of this logic.
        let handlers = &self.handlers;
        match try_claim_for_handler(&self.queue, task_id, &self.info.worker_id, |task_type| {
            handlers.has_handler(task_type)
        })
        .await?
        {
            ClaimResult::Claimed { task, etag } => {
                let lease_id = task.lease_id.ok_or_else(|| {
                    StorageError::S3Error("Claimed task missing lease_id".to_string())
                })?;
                self.info
                    .set_current_task_at(Some(task.id), task.updated_at);
                Ok(Some((*task, etag, lease_id)))
            }
            ClaimResult::Expired => {
                // Task has expired before being claimed, mark it as expired (best-effort)
                tracing::debug!(task_id = %task_id, "Task expired before claiming, marking as expired");
                if let Err(e) = self.queue.mark_expired(task_id).await {
                    tracing::debug!(task_id = %task_id, error = %e, "Failed to mark expired task (may have been handled by another worker)");
                }
                Ok(None)
            }
            ClaimResult::AlreadyClaimed | ClaimResult::NotAvailable | ClaimResult::NotFound => {
                Ok(None)
            }
        }
    }

    async fn list_ready_page(
        &mut self,
        shard: &str,
        limit: i32,
    ) -> Result<Vec<Uuid>, StorageError> {
        if limit <= 0 {
            return Ok(Vec::new());
        }

        let now = self.queue.now().await?;
        let now_bucket = Task::epoch_minute_bucket_value(now);

        // Check if cursor needs initialization before getting mutable reference
        // (avoids borrow checker conflict with find_oldest_ready_bucket)
        let needs_init = self
            .ready_cursors
            .get(shard)
            .is_some_and(|c| !c.initialized);

        let oldest_bucket = if needs_init {
            self.find_oldest_ready_bucket(shard).await?
        } else {
            None
        };

        let cursor = self
            .ready_cursors
            .get_mut(shard)
            .ok_or_else(|| StorageError::S3Error("Missing ready cursor for shard".to_string()))?;

        if !cursor.initialized {
            cursor.bucket = oldest_bucket.unwrap_or(now_bucket);
            cursor.token = None;
            cursor.initialized = true;
        }

        if cursor.bucket > now_bucket {
            cursor.bucket = now_bucket;
            cursor.token = None;
        }

        let bucket_str = Task::epoch_minute_bucket_string_from_value(cursor.bucket);
        let prefix = format!("ready/{shard}/{bucket_str}/");
        let (keys, next_token) = self
            .queue
            .client()
            .list_objects(&prefix, limit, cursor.token.as_deref())
            .await?;

        cursor.token.clone_from(&next_token);

        // Check if we need to refresh oldest bucket (before dropping mutable borrow)
        let needs_refresh = cursor.token.is_none() && cursor.bucket >= now_bucket;
        let current_bucket = cursor.bucket;

        if cursor.token.is_none() && current_bucket < now_bucket {
            cursor.bucket += 1;
        }

        // End cursor borrow before calling find_oldest_ready_bucket
        let _ = cursor;

        // If we need to refresh, do it now and update cursor
        if needs_refresh {
            let oldest = self
                .find_oldest_ready_bucket(shard)
                .await?
                .unwrap_or(now_bucket);
            if let Some(cursor) = self.ready_cursors.get_mut(shard) {
                cursor.bucket = oldest;
            }
        }

        let mut task_ids = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(task_id_str) = key.rsplit('/').next() {
                if let Ok(task_id) = Uuid::parse_str(task_id_str) {
                    task_ids.push(task_id);
                }
            }
        }

        Ok(task_ids)
    }

    async fn find_oldest_ready_bucket(&self, shard: &str) -> Result<Option<i64>, StorageError> {
        let prefix = format!("ready/{shard}/");
        let (keys, _token) = self.queue.client().list_objects(&prefix, 1, None).await?;
        let Some(first) = keys.first() else {
            return Ok(None);
        };
        let bucket_str = first.split('/').nth(2);
        let bucket = bucket_str.and_then(|s| s.parse::<i64>().ok());
        Ok(bucket)
    }

    async fn list_ready_log_scan(
        &mut self,
        shard: &str,
        limit: i32,
    ) -> Result<Vec<Uuid>, StorageError> {
        if limit <= 0 {
            return Ok(Vec::new());
        }

        let prefix = format!("tasks/{shard}/");
        let cursor = self
            .task_cursors
            .get(shard)
            .and_then(|token| token.as_deref());

        let (keys, next_token) = self
            .queue
            .client()
            .list_objects(&prefix, limit, cursor)
            .await?;
        self.task_cursors.insert(shard.to_string(), next_token);

        let now = self.queue.now().await?;
        let mut task_ids = Vec::new();
        for key in keys {
            let Ok((body, _etag)) = self.queue.client().get_object(&key).await else {
                continue;
            };
            let Ok(task) = serde_json::from_slice::<Task>(&body) else {
                continue;
            };
            if task.status == TaskStatus::Pending && task.is_available_at(now) {
                task_ids.push(task.id);
            }
        }

        Ok(task_ids)
    }

    /// Runs the worker loop until shutdown signal.
    ///
    /// This is the main entry point for running a worker. It:
    /// 1. Registers the worker in S3
    /// 2. Polls for and executes tasks in a loop
    /// 3. Updates heartbeat periodically
    /// 4. Handles graceful shutdown when signaled
    /// 5. Deregisters the worker on exit
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration for polling intervals, backoff, etc.
    /// * `shutdown` - Receiver for shutdown signal
    ///
    /// # Returns
    ///
    /// `Ok(())` on clean shutdown.
    ///
    /// # Errors
    ///
    /// Returns `StorageError` if worker registration fails.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let (shutdown_tx, shutdown_rx) = shutdown_signal();
    ///
    /// // Start shutdown listener in background
    /// tokio::spawn(async move {
    ///     wait_for_shutdown_signal(shutdown_tx).await;
    /// });
    ///
    /// // Run worker until shutdown
    /// worker.run(RunnerConfig::default(), shutdown_rx).await?;
    /// ```
    #[allow(clippy::cognitive_complexity, clippy::too_many_lines)]
    pub async fn run(
        &mut self,
        config: RunnerConfig,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), StorageError> {
        // Register worker
        self.register().await?;

        // Create shard lease manager if enabled
        let mut lease_manager = match &config.shard_lease_config {
            Some(lease_config) if lease_config.enabled => {
                tracing::info!(
                    worker_id = %self.info.worker_id,
                    shards_per_worker = lease_config.shards_per_worker,
                    lease_ttl = ?lease_config.lease_ttl,
                    renewal_interval = ?lease_config.renewal_interval,
                    "Shard leasing enabled"
                );
                Some(ShardLeaseManager::new(
                    self.queue.clone(),
                    self.info.worker_id.clone(),
                    lease_config.clone(),
                ))
            }
            _ => None,
        };
        let shard_leasing_enabled = lease_manager.is_some();

        tracing::info!(
            worker_id = %self.info.worker_id,
            shards = ?self.info.shards,
            polling = ?config.polling,
            shard_leasing = shard_leasing_enabled,
            "Worker registered"
        );

        let mut current_poll_interval = config.polling.initial_interval();
        let mut last_heartbeat = std::time::Instant::now();
        let mut last_lease_renewal = std::time::Instant::now();

        // Get the renewal interval for shard leases
        let lease_renewal_interval = config
            .shard_lease_config
            .as_ref()
            .map_or(Duration::from_secs(10), |c| c.renewal_interval);

        loop {
            // Check for shutdown
            if *shutdown.borrow() {
                break;
            }

            // Update heartbeat if needed
            if last_heartbeat.elapsed() >= config.heartbeat_interval {
                if let Err(e) = self.heartbeat().await {
                    tracing::warn!(error = %e, "Failed to update heartbeat");
                }
                last_heartbeat = std::time::Instant::now();
            }

            // Handle shard leasing if enabled
            if let Some(ref mut manager) = lease_manager {
                // Ensure we have enough leases
                if let Err(e) = manager.ensure_leases(&self.info.shards).await {
                    tracing::warn!(error = %e, "Failed to ensure shard leases");
                }

                // Renew leases periodically
                if last_lease_renewal.elapsed() >= lease_renewal_interval {
                    if let Err(e) = manager.renew_all().await {
                        tracing::warn!(error = %e, "Failed to renew shard leases");
                    }
                    last_lease_renewal = std::time::Instant::now();
                }
            }

            // Determine which shards to poll
            let shards_to_poll: Vec<String> = if let Some(ref manager) = lease_manager {
                manager.owned_shards()
            } else {
                self.info.shards.clone()
            };

            // Skip polling if we don't own any shards
            if shards_to_poll.is_empty() {
                let sleep_interval = config.polling.apply_jitter(current_poll_interval);
                tokio::select! {
                    () = tokio::time::sleep(sleep_interval) => {}
                    _ = shutdown.changed() => {
                        if *shutdown.borrow() {
                            break;
                        }
                    }
                }
                continue;
            }

            // Try to claim a task from owned shards
            match self.poll_once_for_shards(&config, &shards_to_poll).await {
                Ok(Some((task, etag, lease_id))) => {
                    tracing::info!(
                        task_id = %task.id,
                        task_type = %task.task_type,
                        attempt = task.attempt,
                        "Claimed task"
                    );

                    // Execute the task with context for lease extension support
                    let (result, final_etag) =
                        execute_task_with_context(&self.queue, &task, &etag, self.handlers()).await;

                    // Handle the result (use final_etag in case handler extended the lease)
                    self.handle_execution_result(&task, &final_etag, lease_id, result)
                        .await;

                    if let Ok(now) = self.queue.now().await {
                        self.info.set_current_task_at(None, now);
                    } else {
                        self.info.current_task = None;
                    }

                    // Reset to fast polling after processing a task
                    current_poll_interval =
                        config.polling.next_interval(current_poll_interval, true);
                }
                Ok(None) => {
                    // No tasks available, apply jitter and backoff
                    let sleep_interval = config.polling.apply_jitter(current_poll_interval);
                    tokio::select! {
                        () = tokio::time::sleep(sleep_interval) => {}
                        _ = shutdown.changed() => {
                            if *shutdown.borrow() {
                                break;
                            }
                        }
                    }

                    // Increase backoff (or stay fixed for Fixed strategy)
                    current_poll_interval =
                        config.polling.next_interval(current_poll_interval, false);
                }
                Err(e) => {
                    tracing::error!(error = %e, "Error polling for tasks");
                    // On error, wait before retrying to avoid hammering the storage
                    let sleep_interval = config.polling.apply_jitter(current_poll_interval);
                    tokio::select! {
                        () = tokio::time::sleep(sleep_interval) => {}
                        _ = shutdown.changed() => {
                            if *shutdown.borrow() {
                                break;
                            }
                        }
                    }
                }
            }
        }

        // Graceful shutdown
        tracing::info!(
            worker_id = %self.info.worker_id,
            grace_period = ?config.shutdown_grace_period,
            "Initiating graceful shutdown"
        );

        // Release shard leases if enabled
        if let Some(ref mut manager) = lease_manager {
            if let Err(e) = manager.release_all().await {
                tracing::warn!(error = %e, "Failed to release shard leases");
            } else {
                tracing::info!(
                    worker_id = %self.info.worker_id,
                    "Released all shard leases"
                );
            }
        }

        // Note: In a more complete implementation, we would track any in-flight task
        // and wait for it to complete within the grace period. Since we process tasks
        // synchronously in the loop above, when we reach this point, no task is in flight.

        // Deregister worker
        if let Err(e) = self.deregister().await {
            tracing::warn!(error = %e, "Failed to deregister worker");
        }

        tracing::info!(
            worker_id = %self.info.worker_id,
            tasks_completed = self.info.tasks_completed,
            tasks_failed = self.info.tasks_failed,
            "Worker shutdown complete"
        );

        Ok(())
    }

    /// Polls once for a task to claim across specified shards.
    ///
    /// Similar to `poll_once_with_config` but allows specifying which shards to poll.
    /// This is used when shard leasing is enabled to only poll owned shards.
    async fn poll_once_for_shards(
        &mut self,
        config: &RunnerConfig,
        shards: &[String],
    ) -> Result<Option<(Task, String, Uuid)>, StorageError> {
        let shard_count = shards.len();
        if shard_count == 0 {
            return Ok(None);
        }
        let start = self.shard_cursor % shard_count;
        let next_cursor = (start + 1) % shard_count;

        for offset in 0..shard_count {
            let idx = (start + offset) % shard_count;
            let shard = shards[idx].clone();
            let index_candidates = if config.index_mode == IndexMode::LogScan {
                Vec::new()
            } else {
                self.list_ready_page(&shard, config.ready_page_size).await?
            };

            if config.index_mode != IndexMode::LogScan {
                for task_id in index_candidates.iter().copied() {
                    if let Some(result) = self.try_claim(task_id).await? {
                        self.shard_cursor = next_cursor;
                        return Ok(Some(result));
                    }
                }
            }

            if matches!(config.index_mode, IndexMode::Hybrid | IndexMode::LogScan) {
                let log_candidates = self
                    .list_ready_log_scan(&shard, config.log_scan_page_size)
                    .await?;
                for task_id in log_candidates {
                    if let Some(result) = self.try_claim(task_id).await? {
                        self.shard_cursor = next_cursor;
                        return Ok(Some(result));
                    }
                }
            }
        }

        self.shard_cursor = next_cursor;
        Ok(None)
    }

    /// Handles the result of task execution.
    ///
    /// Based on the execution result, this method will:
    /// - Complete the task on success
    /// - Reschedule the task if handler requested deferral
    /// - Retry the task on retryable errors (if retries remain)
    /// - Fail the task on permanent errors or exhausted retries
    #[allow(clippy::cognitive_complexity, clippy::too_many_lines)]
    async fn handle_execution_result(
        &mut self,
        task: &Task,
        etag: &str,
        lease_id: Uuid,
        result: ExecutionResult,
    ) {
        match result {
            ExecutionResult::Success(output) => {
                match complete_task(&self.queue, task.id, lease_id, output).await {
                    Ok(_) => {
                        tracing::info!(task_id = %task.id, "Task completed successfully");
                        if let Ok(now) = self.queue.now().await {
                            self.info.record_completed_at(now);
                        } else {
                            self.info.tasks_completed += 1;
                            self.info.current_task = None;
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            task_id = %task.id,
                            error = %e,
                            "Failed to mark task complete"
                        );
                    }
                }
            }
            ExecutionResult::Failed(TaskError::Reschedule(delay_seconds)) => {
                // Handle reschedule - task wants to run later
                match reschedule_task(&self.queue, task, etag, lease_id, delay_seconds).await {
                    Ok(updated_task) => {
                        tracing::info!(
                            task_id = %task.id,
                            delay_seconds = delay_seconds,
                            reschedule_count = updated_task.reschedule_count,
                            available_at = %updated_task.available_at,
                            "Task rescheduled for later"
                        );
                    }
                    Err(super::TransitionError::MaxReschedulesExceeded(max)) => {
                        // Max reschedules exceeded - fail the task
                        let error_msg = format!("Max reschedules ({max}) exceeded");
                        match fail_task(&self.queue, task.id, lease_id, &error_msg).await {
                            Ok(_) => {
                                tracing::info!(
                                    task_id = %task.id,
                                    max_reschedules = max,
                                    reschedule_count = task.reschedule_count,
                                    "Task failed: max reschedules exceeded"
                                );
                                if let Ok(now) = self.queue.now().await {
                                    self.info.record_failed_at(now);
                                } else {
                                    self.info.tasks_failed += 1;
                                    self.info.current_task = None;
                                }
                            }
                            Err(e) => {
                                tracing::error!(
                                    task_id = %task.id,
                                    error = %e,
                                    "Failed to mark task as failed after max reschedules"
                                );
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            task_id = %task.id,
                            error = %e,
                            "Failed to reschedule task"
                        );
                    }
                }
            }
            ExecutionResult::Failed(error) => {
                let should_retry = matches!(error, TaskError::Retryable(_) | TaskError::Timeout)
                    && task.can_retry();

                if should_retry {
                    match retry_task(&self.queue, task.id, lease_id, &error.to_string()).await {
                        Ok(updated_task) => {
                            tracing::info!(
                                task_id = %task.id,
                                retry_count = updated_task.retry_count,
                                available_at = %updated_task.available_at,
                                "Task scheduled for retry"
                            );
                        }
                        Err(e) => {
                            tracing::error!(
                                task_id = %task.id,
                                error = %e,
                                "Failed to schedule retry"
                            );
                        }
                    }
                } else {
                    match fail_task(&self.queue, task.id, lease_id, &error.to_string()).await {
                        Ok(_) => {
                            tracing::info!(
                                task_id = %task.id,
                                error_type = ?error,
                                retry_count = task.retry_count,
                                max_retries = task.max_retries,
                                "Task failed permanently"
                            );
                            if let Ok(now) = self.queue.now().await {
                                self.info.record_failed_at(now);
                            } else {
                                self.info.tasks_failed += 1;
                                self.info.current_task = None;
                            }
                        }
                        Err(e) => {
                            tracing::error!(
                                task_id = %task.id,
                                error = %e,
                                "Failed to mark task as failed"
                            );
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(deprecated)]
    fn test_runner_config_default() {
        let config = RunnerConfig::default();

        // Test the new polling field
        assert_eq!(
            config.polling.initial_interval(),
            Duration::from_millis(100)
        );

        // Legacy fields (deprecated but still present for backward compat)
        assert_eq!(config.poll_interval, Duration::from_millis(100));
        assert_eq!(config.max_poll_interval, Duration::from_secs(5));
        assert!((config.backoff_multiplier - 1.5).abs() < f64::EPSILON);
        assert_eq!(config.shutdown_grace_period, Duration::from_secs(30));
        assert_eq!(config.heartbeat_interval, Duration::from_secs(10));
        assert_eq!(config.index_mode, IndexMode::Hybrid);
        assert_eq!(config.ready_page_size, 100);
        assert_eq!(config.log_scan_page_size, 50);
    }

    #[test]
    #[allow(deprecated)]
    fn test_runner_config_clone() {
        let config = RunnerConfig {
            polling: PollingStrategy::fixed(200),
            poll_interval: Duration::from_millis(200),
            max_poll_interval: Duration::from_secs(10),
            backoff_multiplier: 2.0,
            shutdown_grace_period: Duration::from_secs(60),
            heartbeat_interval: Duration::from_secs(15),
            index_mode: IndexMode::IndexOnly,
            ready_page_size: 10,
            log_scan_page_size: 25,
            shard_lease_config: None,
        };

        let cloned = config.clone();
        assert_eq!(
            cloned.polling.initial_interval(),
            Duration::from_millis(200)
        );
        assert_eq!(cloned.poll_interval, config.poll_interval);
        assert_eq!(cloned.max_poll_interval, config.max_poll_interval);
        assert!((cloned.backoff_multiplier - config.backoff_multiplier).abs() < f64::EPSILON);
        assert_eq!(cloned.shutdown_grace_period, config.shutdown_grace_period);
        assert_eq!(cloned.heartbeat_interval, config.heartbeat_interval);
        assert_eq!(cloned.index_mode, config.index_mode);
        assert_eq!(cloned.ready_page_size, config.ready_page_size);
        assert_eq!(cloned.log_scan_page_size, config.log_scan_page_size);
    }

    #[test]
    fn test_runner_config_debug() {
        let config = RunnerConfig::default();
        let debug_str = format!("{config:?}");

        assert!(debug_str.contains("RunnerConfig"));
        assert!(debug_str.contains("polling"));
        assert!(debug_str.contains("poll_interval"));
        assert!(debug_str.contains("max_poll_interval"));
        assert!(debug_str.contains("backoff_multiplier"));
        assert!(debug_str.contains("shutdown_grace_period"));
        assert!(debug_str.contains("heartbeat_interval"));
        assert!(debug_str.contains("index_mode"));
        assert!(debug_str.contains("ready_page_size"));
        assert!(debug_str.contains("log_scan_page_size"));
    }

    #[test]
    fn test_shutdown_signal_creates_channel() {
        let (tx, rx) = shutdown_signal();

        // Initially should be false
        assert!(!*rx.borrow());

        // After sending, should be true
        tx.send(true).ok();
        assert!(*rx.borrow());
    }

    #[tokio::test]
    async fn test_shutdown_signal_receiver_detects_change() {
        let (tx, mut rx) = shutdown_signal();

        // Initially false
        assert!(!*rx.borrow());

        // Spawn task to send shutdown after a small delay
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            tx.send(true).ok();
        });

        // Wait for change
        let changed = rx.changed().await;
        assert!(changed.is_ok());
        assert!(*rx.borrow());
    }

    #[test]
    fn test_polling_strategy_backoff() {
        // Test the new PollingStrategy-based backoff
        let strategy = PollingStrategy::adaptive_with_backoff(100, 5000, 2.0);
        let mut interval = strategy.initial_interval();

        // First backoff: 100ms * 2.0 = 200ms
        interval = strategy.next_interval(interval, false);
        assert_eq!(interval, Duration::from_millis(200));

        // Second backoff: 200ms * 2.0 = 400ms
        interval = strategy.next_interval(interval, false);
        assert_eq!(interval, Duration::from_millis(400));

        // Continue until cap
        for _ in 0..20 {
            interval = strategy.next_interval(interval, false);
        }

        // Should be capped at max
        assert_eq!(interval, Duration::from_millis(5000));
    }

    #[test]
    fn test_polling_strategy_reset_on_task_found() {
        let strategy = PollingStrategy::adaptive_with_backoff(100, 5000, 2.0);

        // Back off several times
        let mut interval = strategy.initial_interval();
        for _ in 0..5 {
            interval = strategy.next_interval(interval, false);
        }

        // Should have backed off
        assert!(interval > Duration::from_millis(100));

        // Finding a task should reset to min
        interval = strategy.next_interval(interval, true);
        assert_eq!(interval, Duration::from_millis(100));
    }

    #[test]
    #[allow(deprecated)]
    fn test_backoff_calculation() {
        // Keep the old test for backward compatibility verification
        let config = RunnerConfig::default();
        let mut interval = config.poll_interval;

        // First backoff: 100ms * 1.5 = 150ms
        interval = Duration::from_secs_f64(
            (interval.as_secs_f64() * config.backoff_multiplier)
                .min(config.max_poll_interval.as_secs_f64()),
        );
        assert_eq!(interval, Duration::from_millis(150));

        // Second backoff: 150ms * 1.5 = 225ms
        interval = Duration::from_secs_f64(
            (interval.as_secs_f64() * config.backoff_multiplier)
                .min(config.max_poll_interval.as_secs_f64()),
        );
        assert_eq!(interval, Duration::from_micros(225_000));

        // Continue until cap
        for _ in 0..20 {
            interval = Duration::from_secs_f64(
                (interval.as_secs_f64() * config.backoff_multiplier)
                    .min(config.max_poll_interval.as_secs_f64()),
            );
        }

        // Should be capped at max
        assert_eq!(interval, config.max_poll_interval);
    }
}
