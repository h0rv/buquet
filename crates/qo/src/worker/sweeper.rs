//! Index sweeper for repairing ready/lease indexes.
//!
//! This module provides a lightweight repair pass that:
//! - Deletes stale ready/lease index objects
//! - Recreates missing indexes from task state
//!
//! ## Incremental Scanning
//!
//! To avoid O(total objects) scans at scale, the sweeper supports two modes:
//! - `Continuous`: Processes a limited number of objects per sweep cycle,
//!   using continuation tokens to resume from where it left off.
//! - `OnDemand`: Performs a full sweep when explicitly triggered.
//!
//! The `max_objects_per_sweep` setting limits work per cycle in continuous mode.

use std::collections::{HashMap, HashSet};

use uuid::Uuid;

use crate::models::{Task, TaskStatus};
use crate::queue::Queue;
use crate::storage::StorageError;

/// Sweep mode for the index sweeper.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SweepMode {
    /// Continuous incremental sweeping with cursors.
    #[default]
    Continuous,
    /// Full sweep on-demand only (no incremental scanning).
    OnDemand,
}

/// Configuration for the index sweeper.
#[derive(Debug, Clone)]
pub struct SweeperConfig {
    /// Page size for S3 list operations.
    pub page_size: i32,
    /// Maximum number of objects to process per sweep cycle (default: 1000).
    /// Only applies in Continuous mode.
    pub max_objects_per_sweep: usize,
    /// Sweep mode (default: Continuous).
    pub sweep_mode: SweepMode,
}

impl Default for SweeperConfig {
    fn default() -> Self {
        Self {
            page_size: 1000,
            max_objects_per_sweep: 1000,
            sweep_mode: SweepMode::Continuous,
        }
    }
}

/// Summary of sweep actions.
#[derive(Debug, Default, Clone)]
pub struct SweepReport {
    /// Number of ready index keys scanned.
    pub ready_scanned: usize,
    /// Number of stale ready index keys deleted.
    pub ready_deleted: usize,
    /// Number of missing ready index keys created.
    pub ready_created: usize,
    /// Number of lease index keys scanned.
    pub lease_scanned: usize,
    /// Number of stale lease index keys deleted.
    pub lease_deleted: usize,
    /// Number of missing lease index keys created.
    pub lease_created: usize,
    /// Number of task objects scanned.
    pub tasks_scanned: usize,
    /// Whether scan was limited by `max_objects_per_sweep`.
    pub truncated: bool,
    /// Number of expired tasks marked during sweep.
    pub tasks_expired: usize,
}

/// Cursor state for incremental shard sweeping.
#[derive(Debug, Clone, Default)]
pub struct SweeperCursor {
    /// Phase of the sweep: 0=ready, 1=lease, 2=tasks.
    pub phase: u8,
    /// S3 continuation token for resuming LIST operations.
    pub continuation_token: Option<String>,
    /// Whether we've completed a full scan of this shard.
    pub completed_full_scan: bool,
}

/// Repairs ready/lease indexes by scanning indexes and task objects.
#[derive(Debug, Clone)]
pub struct IndexSweeper {
    queue: Queue,
    config: SweeperConfig,
    /// Cursor state per shard for incremental scanning.
    shard_cursors: HashMap<String, SweeperCursor>,
}

impl IndexSweeper {
    /// Creates a new index sweeper.
    #[must_use]
    pub fn new(queue: Queue, config: SweeperConfig) -> Self {
        Self {
            queue,
            config,
            shard_cursors: HashMap::new(),
        }
    }

    /// Returns the current cursor state for a shard.
    #[must_use]
    pub fn get_cursor(&self, shard: &str) -> Option<&SweeperCursor> {
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

    /// Sweeps a single shard for stale/missing index entries.
    ///
    /// In Continuous mode, processes up to `max_objects_per_sweep` objects
    /// and updates the cursor to resume from where it left off.
    #[allow(clippy::cognitive_complexity, clippy::too_many_lines)]
    pub async fn sweep_shard(&mut self, shard: &str) -> Result<SweepReport, StorageError> {
        match self.config.sweep_mode {
            SweepMode::OnDemand => self.sweep_shard_full(shard).await,
            SweepMode::Continuous => self.sweep_shard_incremental(shard).await,
        }
    }

    /// Performs a full sweep of a shard (original behavior).
    #[allow(
        clippy::cognitive_complexity,
        clippy::too_many_lines,
        clippy::large_stack_frames
    )]
    async fn sweep_shard_full(&self, shard: &str) -> Result<SweepReport, StorageError> {
        let ready_prefix = format!("ready/{shard}/");
        let lease_prefix = format!("leases/{shard}/");
        let task_prefix = format!("tasks/{shard}/");

        let ready_keys = self.list_all_keys(&ready_prefix).await?;
        let lease_keys = self.list_all_keys(&lease_prefix).await?;

        let mut ready_set: HashSet<String> = ready_keys.iter().cloned().collect();
        let mut lease_set: HashSet<String> = lease_keys.iter().cloned().collect();

        let mut report = SweepReport::default();
        let now = self.queue.now().await?;

        for key in ready_keys {
            report.ready_scanned += 1;
            let Some(task_id) = parse_task_id(&key) else {
                if self.delete_index(&key).await {
                    ready_set.remove(&key);
                    report.ready_deleted += 1;
                }
                continue;
            };

            let Some((task, _etag)) = self.fetch_task(task_id).await else {
                if self.delete_index(&key).await {
                    ready_set.remove(&key);
                    report.ready_deleted += 1;
                }
                continue;
            };

            if task.status != TaskStatus::Pending {
                if self.delete_index(&key).await {
                    ready_set.remove(&key);
                    report.ready_deleted += 1;
                }
                continue;
            }

            // Check for expired pending tasks and mark them
            if task.is_expired_at(now) {
                if self.queue.mark_expired(task_id).await.is_ok() {
                    report.tasks_expired += 1;
                    // mark_expired already deletes the ready index, but remove from set
                    ready_set.remove(&key);
                    report.ready_deleted += 1;
                }
                continue;
            }

            let expected = task.ready_index_key();
            if key != expected {
                if self.delete_index(&key).await {
                    ready_set.remove(&key);
                    report.ready_deleted += 1;
                }

                if ready_set.insert(expected.clone()) {
                    self.queue.create_index(&expected).await?;
                    report.ready_created += 1;
                }
            }
        }

        for key in lease_keys {
            report.lease_scanned += 1;
            let Some(task_id) = parse_task_id(&key) else {
                if self.delete_index(&key).await {
                    lease_set.remove(&key);
                    report.lease_deleted += 1;
                }
                continue;
            };

            let Some((task, _etag)) = self.fetch_task(task_id).await else {
                if self.delete_index(&key).await {
                    lease_set.remove(&key);
                    report.lease_deleted += 1;
                }
                continue;
            };

            let Some(expected) = task.lease_index_key() else {
                if self.delete_index(&key).await {
                    lease_set.remove(&key);
                    report.lease_deleted += 1;
                }
                continue;
            };

            if task.status != TaskStatus::Running {
                if self.delete_index(&key).await {
                    lease_set.remove(&key);
                    report.lease_deleted += 1;
                }
                continue;
            }

            if key != expected {
                if self.delete_index(&key).await {
                    lease_set.remove(&key);
                    report.lease_deleted += 1;
                }

                if lease_set.insert(expected.clone()) {
                    self.queue.create_index(&expected).await?;
                    report.lease_created += 1;
                }
            }
        }

        let task_keys = self.list_all_keys(&task_prefix).await?;
        for key in task_keys {
            report.tasks_scanned += 1;
            let Some(task) = self.fetch_task_by_key(&key).await else {
                continue;
            };

            match task.status {
                TaskStatus::Pending => {
                    // Check if task has expired
                    if task.is_expired_at(now) {
                        if self.queue.mark_expired(task.id).await.is_ok() {
                            report.tasks_expired += 1;
                        }
                        continue;
                    }
                    let expected = task.ready_index_key();
                    if ready_set.insert(expected.clone()) {
                        self.queue.create_index(&expected).await?;
                        report.ready_created += 1;
                    }
                }
                TaskStatus::Running => {
                    if let Some(expected) = task.lease_index_key() {
                        if lease_set.insert(expected.clone()) {
                            self.queue.create_index(&expected).await?;
                            report.lease_created += 1;
                        }
                    }
                }
                _ => {}
            }
        }

        Ok(report)
    }

    /// Performs an incremental sweep of a shard.
    ///
    /// Processes up to `max_objects_per_sweep` objects and saves cursor
    /// position for the next iteration.
    #[allow(clippy::cognitive_complexity, clippy::too_many_lines)]
    async fn sweep_shard_incremental(&mut self, shard: &str) -> Result<SweepReport, StorageError> {
        let ready_prefix = format!("ready/{shard}/");
        let lease_prefix = format!("leases/{shard}/");
        let task_prefix = format!("tasks/{shard}/");

        // Get or create cursor for this shard
        let cursor = self.shard_cursors.entry(shard.to_string()).or_default();
        let current_phase = cursor.phase;
        let continuation_token = cursor.continuation_token.clone();

        let mut report = SweepReport::default();
        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        let limit = self.config.max_objects_per_sweep as i32;

        // Determine which prefix to use based on phase
        let prefix = match current_phase {
            0 => &ready_prefix,
            1 => &lease_prefix,
            _ => &task_prefix,
        };

        // List objects with continuation token
        let (keys, next_token) = self
            .queue
            .client()
            .list_objects(prefix, limit, continuation_token.as_deref())
            .await?;

        let truncated = next_token.is_some();
        report.truncated = truncated;

        // Process keys based on phase
        // Get current time for expiration checks
        let now = self.queue.now().await?;

        match current_phase {
            0 => {
                // Ready index phase
                let ready_set: HashSet<String> = keys.iter().cloned().collect();
                for key in &keys {
                    report.ready_scanned += 1;
                    let Some(task_id) = parse_task_id(key) else {
                        if self.delete_index(key).await {
                            report.ready_deleted += 1;
                        }
                        continue;
                    };

                    let Some((task, _etag)) = self.fetch_task(task_id).await else {
                        if self.delete_index(key).await {
                            report.ready_deleted += 1;
                        }
                        continue;
                    };

                    if task.status != TaskStatus::Pending {
                        if self.delete_index(key).await {
                            report.ready_deleted += 1;
                        }
                        continue;
                    }

                    // Check for expired pending tasks and mark them
                    if task.is_expired_at(now) {
                        if self.queue.mark_expired(task_id).await.is_ok() {
                            report.tasks_expired += 1;
                            report.ready_deleted += 1;
                        }
                        continue;
                    }

                    let expected = task.ready_index_key();
                    if key != &expected {
                        if self.delete_index(key).await {
                            report.ready_deleted += 1;
                        }
                        if !ready_set.contains(&expected)
                            && self.queue.create_index(&expected).await.is_ok()
                        {
                            report.ready_created += 1;
                        }
                    }
                }
            }
            1 => {
                // Lease index phase
                let lease_set: HashSet<String> = keys.iter().cloned().collect();
                for key in &keys {
                    report.lease_scanned += 1;
                    let Some(task_id) = parse_task_id(key) else {
                        if self.delete_index(key).await {
                            report.lease_deleted += 1;
                        }
                        continue;
                    };

                    let Some((task, _etag)) = self.fetch_task(task_id).await else {
                        if self.delete_index(key).await {
                            report.lease_deleted += 1;
                        }
                        continue;
                    };

                    let Some(expected) = task.lease_index_key() else {
                        if self.delete_index(key).await {
                            report.lease_deleted += 1;
                        }
                        continue;
                    };

                    if task.status != TaskStatus::Running {
                        if self.delete_index(key).await {
                            report.lease_deleted += 1;
                        }
                        continue;
                    }

                    if key != &expected {
                        if self.delete_index(key).await {
                            report.lease_deleted += 1;
                        }
                        if !lease_set.contains(&expected)
                            && self.queue.create_index(&expected).await.is_ok()
                        {
                            report.lease_created += 1;
                        }
                    }
                }
            }
            _ => {
                // Task scanning phase (create missing indexes and expire stale tasks)
                for key in &keys {
                    report.tasks_scanned += 1;
                    let Some(task) = self.fetch_task_by_key(key).await else {
                        continue;
                    };

                    match task.status {
                        TaskStatus::Pending => {
                            // Check if task has expired
                            if task.is_expired_at(now) {
                                if self.queue.mark_expired(task.id).await.is_ok() {
                                    report.tasks_expired += 1;
                                }
                                continue;
                            }
                            let expected = task.ready_index_key();
                            // Best-effort create (don't fail on errors)
                            if self.queue.create_index(&expected).await.is_ok() {
                                report.ready_created += 1;
                            }
                        }
                        TaskStatus::Running => {
                            if let Some(expected) = task.lease_index_key() {
                                if self.queue.create_index(&expected).await.is_ok() {
                                    report.lease_created += 1;
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
        }

        // Update cursor for next iteration (re-access via entry since we know it exists)
        {
            let cursor = self.shard_cursors.entry(shard.to_string()).or_default();
            if truncated {
                cursor.continuation_token = next_token;
                cursor.completed_full_scan = false;
            } else {
                // Move to next phase or complete
                if current_phase < 2 {
                    cursor.phase = current_phase + 1;
                    cursor.continuation_token = None;
                } else {
                    // Completed all phases, reset for next full scan
                    cursor.phase = 0;
                    cursor.continuation_token = None;
                    cursor.completed_full_scan = true;
                }
            }
        }

        Ok(report)
    }

    /// Sweeps all shards (0-f).
    pub async fn sweep_all_shards(&mut self) -> Result<SweepReport, StorageError> {
        let mut total = SweepReport::default();

        for i in 0..16 {
            let shard = format!("{i:x}");
            match self.sweep_shard(&shard).await {
                Ok(report) => merge_reports(&mut total, &report),
                Err(e) => tracing::warn!(shard = %shard, "Index sweep failed: {e}"),
            }
        }

        Ok(total)
    }

    /// Checks if all shards have completed at least one full scan.
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

    async fn list_all_keys(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        let mut keys = Vec::new();
        let mut token = None;

        loop {
            let (mut page_keys, next_token) = self
                .queue
                .client()
                .list_objects(prefix, self.config.page_size, token.as_deref())
                .await?;
            keys.append(&mut page_keys);
            match next_token {
                Some(next) => token = Some(next),
                None => break,
            }
        }

        Ok(keys)
    }

    async fn fetch_task(&self, task_id: Uuid) -> Option<(Task, String)> {
        match self.queue.get(task_id).await {
            Ok(task) => task,
            Err(e) => {
                tracing::warn!(task_id = %task_id, "Error fetching task: {e}");
                None
            }
        }
    }

    async fn fetch_task_by_key(&self, key: &str) -> Option<Task> {
        match self.queue.client().get_object(key).await {
            Ok((body, _etag)) => match serde_json::from_slice::<Task>(&body) {
                Ok(task) => Some(task),
                Err(e) => {
                    tracing::warn!(key = %key, "Error deserializing task: {e}");
                    None
                }
            },
            Err(StorageError::NotFound { .. }) => None,
            Err(e) => {
                tracing::warn!(key = %key, "Error fetching task: {e}");
                None
            }
        }
    }

    async fn delete_index(&self, key: &str) -> bool {
        match self.queue.delete_index(key).await {
            Ok(()) => true,
            Err(e) => {
                tracing::warn!(key = %key, "Failed to delete index: {e}");
                false
            }
        }
    }
}

fn parse_task_id(key: &str) -> Option<Uuid> {
    key.rsplit('/')
        .next()
        .and_then(|id| Uuid::parse_str(id).ok())
}

const fn merge_reports(total: &mut SweepReport, report: &SweepReport) {
    total.ready_scanned += report.ready_scanned;
    total.ready_deleted += report.ready_deleted;
    total.ready_created += report.ready_created;
    total.lease_scanned += report.lease_scanned;
    total.lease_deleted += report.lease_deleted;
    total.lease_created += report.lease_created;
    total.tasks_scanned += report.tasks_scanned;
    total.truncated = total.truncated || report.truncated;
    total.tasks_expired += report.tasks_expired;
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_sweeper_config_default() {
        let config = SweeperConfig::default();
        assert_eq!(config.page_size, 1000);
        assert_eq!(config.max_objects_per_sweep, 1000);
        assert_eq!(config.sweep_mode, SweepMode::Continuous);
    }

    #[test]
    fn test_sweeper_config_custom() {
        let config = SweeperConfig {
            page_size: 500,
            max_objects_per_sweep: 200,
            sweep_mode: SweepMode::OnDemand,
        };
        assert_eq!(config.page_size, 500);
        assert_eq!(config.max_objects_per_sweep, 200);
        assert_eq!(config.sweep_mode, SweepMode::OnDemand);
    }

    #[test]
    fn test_sweeper_cursor_default() {
        let cursor = SweeperCursor::default();
        assert_eq!(cursor.phase, 0);
        assert!(cursor.continuation_token.is_none());
        assert!(!cursor.completed_full_scan);
    }

    #[test]
    fn test_sweep_report_default() {
        let report = SweepReport::default();
        assert_eq!(report.ready_scanned, 0);
        assert_eq!(report.ready_deleted, 0);
        assert_eq!(report.ready_created, 0);
        assert_eq!(report.lease_scanned, 0);
        assert_eq!(report.lease_deleted, 0);
        assert_eq!(report.lease_created, 0);
        assert_eq!(report.tasks_scanned, 0);
        assert!(!report.truncated);
    }

    #[test]
    fn test_merge_reports() {
        let mut total = SweepReport {
            ready_scanned: 10,
            ready_deleted: 2,
            ready_created: 1,
            lease_scanned: 5,
            lease_deleted: 1,
            lease_created: 0,
            tasks_scanned: 20,
            truncated: false,
            tasks_expired: 0,
        };

        let report = SweepReport {
            ready_scanned: 5,
            ready_deleted: 1,
            ready_created: 2,
            lease_scanned: 3,
            lease_deleted: 0,
            lease_created: 1,
            tasks_scanned: 10,
            truncated: true,
            tasks_expired: 0,
        };

        merge_reports(&mut total, &report);

        assert_eq!(total.ready_scanned, 15);
        assert_eq!(total.ready_deleted, 3);
        assert_eq!(total.ready_created, 3);
        assert_eq!(total.lease_scanned, 8);
        assert_eq!(total.lease_deleted, 1);
        assert_eq!(total.lease_created, 1);
        assert_eq!(total.tasks_scanned, 30);
        assert!(total.truncated);
    }

    #[test]
    fn test_parse_task_id() {
        let key = "ready/0/1234567890/550e8400-e29b-41d4-a716-446655440000";
        let task_id = parse_task_id(key);
        assert!(task_id.is_some());
        assert_eq!(
            task_id.unwrap().to_string(),
            "550e8400-e29b-41d4-a716-446655440000"
        );

        let invalid_key = "ready/0/1234567890/not-a-uuid";
        assert!(parse_task_id(invalid_key).is_none());
    }
}
