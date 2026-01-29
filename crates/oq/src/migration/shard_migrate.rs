//! Shard migration implementation.
//!
//! Migrates tasks and indexes from one shard prefix length to another.
//! Uses S3 `CopyObject` for efficient same-bucket copies.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use futures::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::models::Task;
use crate::storage::{S3Client, StorageError};

/// Represents a planned object move from one key to another.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectMove {
    /// The current S3 key.
    pub from_key: String,
    /// The new S3 key after migration.
    pub to_key: String,
    /// The object type (task, `ready_index`, `lease_index`).
    pub object_type: ObjectType,
    /// The task ID this object belongs to.
    pub task_id: Uuid,
}

/// Type of object being migrated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectType {
    /// Task object at `tasks/{shard}/{id}.json`.
    Task,
    /// Ready index at `ready/{shard}/{bucket}/{id}`.
    ReadyIndex,
    /// Lease index at `leases/{shard}/{bucket}/{id}`.
    LeaseIndex,
}

/// Summary of what will be migrated.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MigrationPlan {
    /// Total number of objects to migrate.
    pub total_objects: usize,
    /// Number of task objects.
    pub task_count: usize,
    /// Number of ready index objects.
    pub ready_index_count: usize,
    /// Number of lease index objects.
    pub lease_index_count: usize,
    /// Objects that need to be moved (shard differs).
    pub moves: Vec<ObjectMove>,
    /// Objects that are already in the correct location.
    pub already_correct: usize,
    /// Source prefix length.
    pub from_prefix_len: usize,
    /// Target prefix length.
    pub to_prefix_len: usize,
    /// Shard distribution before migration.
    pub from_shard_distribution: HashMap<String, usize>,
    /// Expected shard distribution after migration.
    pub to_shard_distribution: HashMap<String, usize>,
}

impl MigrationPlan {
    /// Returns true if there are no objects to migrate.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.moves.is_empty()
    }
}

/// Progress tracking during migration.
#[derive(Debug, Default)]
pub struct MigrationProgress {
    /// Number of objects successfully migrated.
    completed: AtomicUsize,
    /// Number of objects that failed to migrate.
    failed: AtomicUsize,
    /// Number of objects skipped (already correct).
    skipped: AtomicUsize,
}

impl MigrationProgress {
    /// Create a new progress tracker.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the number of completed migrations.
    #[must_use]
    pub fn completed(&self) -> usize {
        self.completed.load(Ordering::Relaxed)
    }

    /// Get the number of failed migrations.
    #[must_use]
    pub fn failed(&self) -> usize {
        self.failed.load(Ordering::Relaxed)
    }

    /// Get the number of skipped objects.
    #[must_use]
    pub fn skipped(&self) -> usize {
        self.skipped.load(Ordering::Relaxed)
    }
}

/// Result of a migration operation.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MigrationResult {
    /// Number of objects successfully migrated.
    pub migrated: usize,
    /// Number of objects that failed to migrate.
    pub failed: usize,
    /// Number of objects skipped (already in correct location).
    pub skipped: usize,
    /// Error messages for failed migrations.
    pub errors: Vec<String>,
}

impl MigrationResult {
    /// Returns true if the migration completed without errors.
    #[must_use]
    pub const fn is_success(&self) -> bool {
        self.failed == 0
    }
}

/// Shard migration tool for changing shard prefix length.
///
/// Handles migrating all task-related objects (tasks, ready indexes, lease indexes)
/// from one shard configuration to another.
///
/// # Example
///
/// ```no_run
/// use oq::migration::ShardMigration;
/// use oq::storage::S3Client;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client: S3Client = todo!();
/// let migration = ShardMigration::new(client, 1, 2, true);
///
/// // First analyze what would be migrated
/// let plan = migration.analyze().await?;
/// println!("Would migrate {} objects", plan.moves.len());
///
/// // Then execute (set dry_run=false)
/// let migration = ShardMigration::new(migration.into_client(), 1, 2, false);
/// let result = migration.execute().await?;
/// println!("Migrated {} objects", result.migrated);
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct ShardMigration {
    client: S3Client,
    from_prefix_len: usize,
    to_prefix_len: usize,
    dry_run: bool,
    concurrency: usize,
}

impl ShardMigration {
    /// Creates a new shard migration.
    ///
    /// # Arguments
    ///
    /// * `client` - The S3 client to use
    /// * `from_prefix_len` - Current shard prefix length (1-4)
    /// * `to_prefix_len` - Target shard prefix length (1-4)
    /// * `dry_run` - If true, only analyze without making changes
    #[must_use]
    pub fn new(
        client: S3Client,
        from_prefix_len: usize,
        to_prefix_len: usize,
        dry_run: bool,
    ) -> Self {
        Self {
            client,
            from_prefix_len: from_prefix_len.clamp(1, 4),
            to_prefix_len: to_prefix_len.clamp(1, 4),
            dry_run,
            concurrency: 10, // Default concurrency
        }
    }

    /// Sets the concurrency level for migration operations.
    #[must_use]
    pub const fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    /// Returns the underlying S3 client.
    #[must_use]
    pub fn into_client(self) -> S3Client {
        self.client
    }

    /// Analyzes the migration without making any changes.
    ///
    /// Scans all tasks, ready indexes, and lease indexes to determine
    /// what needs to be moved.
    ///
    /// # Returns
    ///
    /// A `MigrationPlan` describing what would be migrated.
    pub async fn analyze(&self) -> Result<MigrationPlan, StorageError> {
        let mut plan = MigrationPlan {
            from_prefix_len: self.from_prefix_len,
            to_prefix_len: self.to_prefix_len,
            ..Default::default()
        };

        // Scan tasks
        self.scan_tasks(&mut plan).await?;

        // Scan ready indexes
        self.scan_ready_indexes(&mut plan).await?;

        // Scan lease indexes
        self.scan_lease_indexes(&mut plan).await?;

        plan.total_objects = plan.task_count + plan.ready_index_count + plan.lease_index_count;

        Ok(plan)
    }

    /// Scans task objects and adds moves to the plan.
    #[allow(clippy::if_not_else)]
    async fn scan_tasks(&self, plan: &mut MigrationPlan) -> Result<(), StorageError> {
        let prefix = "tasks/";
        let mut continuation_token: Option<String> = None;

        loop {
            let (keys, next_token) = self
                .client
                .list_objects(prefix, 1000, continuation_token.as_deref())
                .await?;

            for key in keys {
                // Parse task ID from key: tasks/{shard}/{id}.json
                if let Some(task_id) = Self::parse_task_id_from_key(&key) {
                    plan.task_count += 1;

                    // Current shard from key
                    let current_shard = Self::extract_shard_from_task_key(&key);

                    // Compute new shard
                    let new_shard = Task::shard_from_id_with_len(&task_id, self.to_prefix_len);

                    // Track distribution
                    *plan
                        .from_shard_distribution
                        .entry(current_shard.clone())
                        .or_insert(0) += 1;
                    *plan
                        .to_shard_distribution
                        .entry(new_shard.clone())
                        .or_insert(0) += 1;

                    if current_shard != new_shard {
                        let new_key = format!("tasks/{new_shard}/{task_id}.json");
                        plan.moves.push(ObjectMove {
                            from_key: key,
                            to_key: new_key,
                            object_type: ObjectType::Task,
                            task_id,
                        });
                    } else {
                        plan.already_correct += 1;
                    }
                }
            }

            match next_token {
                Some(token) => continuation_token = Some(token),
                None => break,
            }
        }

        Ok(())
    }

    /// Scans ready index objects and adds moves to the plan.
    #[allow(clippy::if_not_else)]
    async fn scan_ready_indexes(&self, plan: &mut MigrationPlan) -> Result<(), StorageError> {
        let prefix = "ready/";
        let mut continuation_token: Option<String> = None;

        loop {
            let (keys, next_token) = self
                .client
                .list_objects(prefix, 1000, continuation_token.as_deref())
                .await?;

            for key in keys {
                // Parse task ID from key: ready/{shard}/{bucket}/{id}
                if let Some((task_id, bucket)) = Self::parse_ready_index_key(&key) {
                    plan.ready_index_count += 1;

                    let current_shard = Self::extract_shard_from_ready_key(&key);
                    let new_shard = Task::shard_from_id_with_len(&task_id, self.to_prefix_len);

                    if current_shard != new_shard {
                        let new_key = format!("ready/{new_shard}/{bucket}/{task_id}");
                        plan.moves.push(ObjectMove {
                            from_key: key,
                            to_key: new_key,
                            object_type: ObjectType::ReadyIndex,
                            task_id,
                        });
                    } else {
                        plan.already_correct += 1;
                    }
                }
            }

            match next_token {
                Some(token) => continuation_token = Some(token),
                None => break,
            }
        }

        Ok(())
    }

    /// Scans lease index objects and adds moves to the plan.
    #[allow(clippy::if_not_else)]
    async fn scan_lease_indexes(&self, plan: &mut MigrationPlan) -> Result<(), StorageError> {
        let prefix = "leases/";
        let mut continuation_token: Option<String> = None;

        loop {
            let (keys, next_token) = self
                .client
                .list_objects(prefix, 1000, continuation_token.as_deref())
                .await?;

            for key in keys {
                // Parse task ID from key: leases/{shard}/{bucket}/{id}
                if let Some((task_id, bucket)) = Self::parse_lease_index_key(&key) {
                    plan.lease_index_count += 1;

                    let current_shard = Self::extract_shard_from_lease_key(&key);
                    let new_shard = Task::shard_from_id_with_len(&task_id, self.to_prefix_len);

                    if current_shard != new_shard {
                        let new_key = format!("leases/{new_shard}/{bucket}/{task_id}");
                        plan.moves.push(ObjectMove {
                            from_key: key,
                            to_key: new_key,
                            object_type: ObjectType::LeaseIndex,
                            task_id,
                        });
                    } else {
                        plan.already_correct += 1;
                    }
                }
            }

            match next_token {
                Some(token) => continuation_token = Some(token),
                None => break,
            }
        }

        Ok(())
    }

    /// Executes the migration.
    ///
    /// For each object that needs to move:
    /// 1. Copy to new location using S3 `CopyObject`
    /// 2. Delete from old location
    ///
    /// If `dry_run` is true, this will only analyze and return the plan
    /// without making any changes.
    ///
    /// # Returns
    ///
    /// A `MigrationResult` with counts of migrated, failed, and skipped objects.
    pub async fn execute(&self) -> Result<MigrationResult, StorageError> {
        let plan = self.analyze().await?;

        if self.dry_run {
            return Ok(MigrationResult {
                migrated: 0,
                failed: 0,
                skipped: plan.total_objects,
                errors: vec![],
            });
        }

        let progress = Arc::new(MigrationProgress::new());
        let errors = Arc::new(tokio::sync::Mutex::new(Vec::<String>::new()));

        // Process moves concurrently
        stream::iter(plan.moves)
            .map(|object_move| {
                let client = self.client.clone();
                let progress = Arc::clone(&progress);
                let errors = Arc::clone(&errors);

                async move {
                    match Self::migrate_object(&client, &object_move).await {
                        Ok(()) => {
                            progress.completed.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(e) => {
                            progress.failed.fetch_add(1, Ordering::Relaxed);
                            errors
                                .lock()
                                .await
                                .push(format!("Failed to migrate {}: {e}", object_move.from_key));
                        }
                    }
                }
            })
            .buffer_unordered(self.concurrency)
            .collect::<Vec<()>>()
            .await;

        // Extract errors - at this point all tasks are complete so we're the only owner
        let final_errors = {
            let guard = errors.lock().await;
            guard.clone()
        };

        Ok(MigrationResult {
            migrated: progress.completed(),
            failed: progress.failed(),
            skipped: plan.already_correct,
            errors: final_errors,
        })
    }

    /// Migrates a single object by copying to new location and deleting old.
    async fn migrate_object(
        client: &S3Client,
        object_move: &ObjectMove,
    ) -> Result<(), StorageError> {
        // Copy to new location
        client
            .copy_object(&object_move.from_key, &object_move.to_key)
            .await?;

        // Delete old location
        client.delete_object(&object_move.from_key).await?;

        Ok(())
    }

    /// Parses task ID from a task key: `tasks/{shard}/{id}.json`
    fn parse_task_id_from_key(key: &str) -> Option<Uuid> {
        // Remove "tasks/" prefix and ".json" suffix
        let stripped = key.strip_prefix("tasks/")?;
        // Split by "/" to get shard and filename
        let parts: Vec<&str> = stripped.split('/').collect();
        if parts.len() != 2 {
            return None;
        }
        let filename = parts[1];
        let id_str = filename.strip_suffix(".json")?;
        Uuid::parse_str(id_str).ok()
    }

    /// Extracts the shard from a task key: `tasks/{shard}/{id}.json`
    fn extract_shard_from_task_key(key: &str) -> String {
        key.strip_prefix("tasks/")
            .and_then(|s| s.split('/').next())
            .unwrap_or("")
            .to_string()
    }

    /// Parses task ID and bucket from a ready index key: `ready/{shard}/{bucket}/{id}`
    fn parse_ready_index_key(key: &str) -> Option<(Uuid, String)> {
        let stripped = key.strip_prefix("ready/")?;
        let parts: Vec<&str> = stripped.split('/').collect();
        if parts.len() != 3 {
            return None;
        }
        let bucket = parts[1].to_string();
        let id_str = parts[2];
        let task_id = Uuid::parse_str(id_str).ok()?;
        Some((task_id, bucket))
    }

    /// Extracts the shard from a ready index key: `ready/{shard}/{bucket}/{id}`
    fn extract_shard_from_ready_key(key: &str) -> String {
        key.strip_prefix("ready/")
            .and_then(|s| s.split('/').next())
            .unwrap_or("")
            .to_string()
    }

    /// Parses task ID and bucket from a lease index key: `leases/{shard}/{bucket}/{id}`
    fn parse_lease_index_key(key: &str) -> Option<(Uuid, String)> {
        let stripped = key.strip_prefix("leases/")?;
        let parts: Vec<&str> = stripped.split('/').collect();
        if parts.len() != 3 {
            return None;
        }
        let bucket = parts[1].to_string();
        let id_str = parts[2];
        let task_id = Uuid::parse_str(id_str).ok()?;
        Some((task_id, bucket))
    }

    /// Extracts the shard from a lease index key: `leases/{shard}/{bucket}/{id}`
    fn extract_shard_from_lease_key(key: &str) -> String {
        key.strip_prefix("leases/")
            .and_then(|s| s.split('/').next())
            .unwrap_or("")
            .to_string()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    // Helper functions to test parsing logic without needing an S3Client

    fn parse_task_id_from_key(key: &str) -> Option<Uuid> {
        let stripped = key.strip_prefix("tasks/")?;
        let parts: Vec<&str> = stripped.split('/').collect();
        if parts.len() != 2 {
            return None;
        }
        let filename = parts[1];
        let id_str = filename.strip_suffix(".json")?;
        Uuid::parse_str(id_str).ok()
    }

    fn extract_shard_from_task_key(key: &str) -> String {
        key.strip_prefix("tasks/")
            .and_then(|s| s.split('/').next())
            .unwrap_or("")
            .to_string()
    }

    fn parse_ready_index_key(key: &str) -> Option<(Uuid, String)> {
        let stripped = key.strip_prefix("ready/")?;
        let parts: Vec<&str> = stripped.split('/').collect();
        if parts.len() != 3 {
            return None;
        }
        let bucket = parts[1].to_string();
        let id_str = parts[2];
        let task_id = Uuid::parse_str(id_str).ok()?;
        Some((task_id, bucket))
    }

    #[test]
    fn test_parse_task_id_from_key() {
        let key = "tasks/a/a1b2c3d4-e5f6-7890-abcd-ef1234567890.json";
        let task_id = parse_task_id_from_key(key);
        assert!(task_id.is_some());
        assert_eq!(
            task_id.unwrap().to_string(),
            "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
        );
    }

    #[test]
    fn test_extract_shard_from_task_key() {
        assert_eq!(extract_shard_from_task_key("tasks/a/uuid.json"), "a");
        assert_eq!(extract_shard_from_task_key("tasks/a1/uuid.json"), "a1");
        assert_eq!(extract_shard_from_task_key("tasks/abc/uuid.json"), "abc");
    }

    #[test]
    fn test_parse_ready_index_key() {
        let key = "ready/a/0028421910/a1b2c3d4-e5f6-7890-abcd-ef1234567890";
        let result = parse_ready_index_key(key);
        assert!(result.is_some());
        let (task_id, bucket) = result.unwrap();
        assert_eq!(task_id.to_string(), "a1b2c3d4-e5f6-7890-abcd-ef1234567890");
        assert_eq!(bucket, "0028421910");
    }

    #[test]
    fn test_shard_changes() {
        // UUID: a1b2c3d4-e5f6-7890-abcd-ef1234567890
        // prefix_len=1: "a"
        // prefix_len=2: "a1"
        let id =
            Uuid::parse_str("a1b2c3d4-e5f6-7890-abcd-ef1234567890").expect("valid UUID for test");

        assert_eq!(Task::shard_from_id_with_len(&id, 1), "a");
        assert_eq!(Task::shard_from_id_with_len(&id, 2), "a1");
        assert_eq!(Task::shard_from_id_with_len(&id, 3), "a1b");
    }

    #[test]
    fn test_migration_plan_is_empty() {
        let plan = MigrationPlan::default();
        assert!(plan.is_empty());

        let plan_with_moves = MigrationPlan {
            moves: vec![ObjectMove {
                from_key: "tasks/a/test.json".to_string(),
                to_key: "tasks/a1/test.json".to_string(),
                object_type: ObjectType::Task,
                task_id: Uuid::new_v4(),
            }],
            ..Default::default()
        };
        assert!(!plan_with_moves.is_empty());
    }

    #[test]
    fn test_migration_result_is_success() {
        let success_result = MigrationResult {
            migrated: 10,
            failed: 0,
            skipped: 5,
            errors: vec![],
        };
        assert!(success_result.is_success());

        let failed_result = MigrationResult {
            migrated: 8,
            failed: 2,
            skipped: 5,
            errors: vec!["error 1".to_string(), "error 2".to_string()],
        };
        assert!(!failed_result.is_success());
    }
}
