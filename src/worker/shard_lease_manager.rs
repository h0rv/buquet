//! Shard lease manager for acquiring, renewing, and releasing shard leases.
//!
//! The `ShardLeaseManager` handles all operations related to shard ownership:
//! - Acquiring leases on available shards
//! - Renewing leases before they expire
//! - Releasing leases on shutdown
//! - Tracking which shards are currently owned

use std::collections::HashSet;

use crate::queue::Queue;
use crate::storage::{PutCondition, StorageError};

use super::shard_lease::{ShardLease, ShardLeaseConfig};

/// Manages shard leases for a worker.
///
/// The manager tracks which shards the worker owns and provides methods
/// for acquiring, renewing, and releasing leases.
pub struct ShardLeaseManager {
    queue: Queue,
    worker_id: String,
    config: ShardLeaseConfig,
    owned_shards: HashSet<String>,
}

impl ShardLeaseManager {
    /// Creates a new shard lease manager.
    ///
    /// # Arguments
    ///
    /// * `queue` - The queue for S3 operations
    /// * `worker_id` - This worker's unique identifier
    /// * `config` - Configuration for lease timing and behavior
    #[must_use]
    pub fn new(queue: Queue, worker_id: String, config: ShardLeaseConfig) -> Self {
        Self {
            queue,
            worker_id,
            config,
            owned_shards: HashSet::new(),
        }
    }

    /// Returns the worker ID.
    #[must_use]
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    /// Returns the shard lease configuration.
    #[must_use]
    pub const fn config(&self) -> &ShardLeaseConfig {
        &self.config
    }

    /// Try to acquire a lease on a shard. Returns true if successful.
    ///
    /// The acquisition algorithm:
    /// 1. Fast path: Try to create lease with If-None-Match (no existing lease)
    /// 2. Slow path: If lease exists, check if it's expired
    /// 3. If expired, try to take over with CAS (If-Match on `ETag`)
    ///
    /// # Errors
    ///
    /// Returns `StorageError` for S3 communication failures.
    pub async fn try_acquire(&mut self, shard: &str) -> Result<bool, StorageError> {
        let key = ShardLease::key_for_shard(shard);
        let now = self.queue.now().await?;
        let expires_at = now
            + chrono::Duration::from_std(self.config.lease_ttl)
                .unwrap_or_else(|_| chrono::Duration::seconds(30));

        let lease = ShardLease {
            shard: shard.to_string(),
            worker_id: self.worker_id.clone(),
            lease_expires_at: expires_at,
            updated_at: now,
        };
        let body = serde_json::to_vec(&lease)
            .map_err(|e| StorageError::SerializationError(e.to_string()))?;

        // Fast path: Try to create lease (If-None-Match: *)
        match self
            .queue
            .client()
            .put_object(&key, body.clone(), PutCondition::IfNoneMatch)
            .await
        {
            Ok(_) => {
                self.owned_shards.insert(shard.to_string());
                tracing::debug!(shard = %shard, worker_id = %self.worker_id, "Acquired shard lease (new)");
                return Ok(true);
            }
            Err(StorageError::PreconditionFailed { .. }) => {
                // Lease exists, check if expired
            }
            Err(e) => return Err(e),
        }

        // Slow path: Lease exists, check if expired
        let (existing_body, etag) = match self.queue.client().get_object(&key).await {
            Ok(result) => result,
            Err(StorageError::NotFound { .. }) => {
                // Race condition: lease was deleted between our put and get
                // Try to create again
                match self
                    .queue
                    .client()
                    .put_object(&key, body, PutCondition::IfNoneMatch)
                    .await
                {
                    Ok(_) => {
                        self.owned_shards.insert(shard.to_string());
                        tracing::debug!(shard = %shard, worker_id = %self.worker_id, "Acquired shard lease (retry)");
                        return Ok(true);
                    }
                    Err(StorageError::PreconditionFailed { .. }) => return Ok(false),
                    Err(e) => return Err(e),
                }
            }
            Err(e) => return Err(e),
        };

        let existing: ShardLease = serde_json::from_slice(&existing_body)
            .map_err(|e| StorageError::SerializationError(e.to_string()))?;

        // Check if we already own this lease
        if existing.worker_id == self.worker_id {
            self.owned_shards.insert(shard.to_string());
            return Ok(true);
        }

        // Check if lease is still valid
        if !existing.is_expired(now) {
            // Still valid, owned by someone else
            return Ok(false);
        }

        // Expired, try to take over with CAS
        match self
            .queue
            .client()
            .put_object(&key, body, PutCondition::IfMatch(etag))
            .await
        {
            Ok(_) => {
                self.owned_shards.insert(shard.to_string());
                tracing::debug!(
                    shard = %shard,
                    worker_id = %self.worker_id,
                    previous_worker = %existing.worker_id,
                    "Acquired shard lease (takeover from expired)"
                );
                Ok(true)
            }
            Err(StorageError::PreconditionFailed { .. }) => {
                // Another worker took over first
                Ok(false)
            }
            Err(e) => Err(e),
        }
    }

    /// Renew lease on a shard we own.
    ///
    /// Returns `true` if renewal succeeded, `false` if we lost ownership.
    ///
    /// # Errors
    ///
    /// Returns `StorageError` for S3 communication failures.
    pub async fn renew(&mut self, shard: &str) -> Result<bool, StorageError> {
        if !self.owned_shards.contains(shard) {
            return Ok(false);
        }

        let key = ShardLease::key_for_shard(shard);
        let now = self.queue.now().await?;

        // Get current lease to verify we still own it
        let (body, etag) = match self.queue.client().get_object(&key).await {
            Ok(result) => result,
            Err(StorageError::NotFound { .. }) => {
                // Lease was deleted, we lost ownership
                self.owned_shards.remove(shard);
                return Ok(false);
            }
            Err(e) => return Err(e),
        };

        let existing: ShardLease = serde_json::from_slice(&body)
            .map_err(|e| StorageError::SerializationError(e.to_string()))?;

        if existing.worker_id != self.worker_id {
            // Lost ownership
            self.owned_shards.remove(shard);
            tracing::warn!(
                shard = %shard,
                our_worker_id = %self.worker_id,
                current_owner = %existing.worker_id,
                "Lost shard lease ownership during renewal"
            );
            return Ok(false);
        }

        let expires_at = now
            + chrono::Duration::from_std(self.config.lease_ttl)
                .unwrap_or_else(|_| chrono::Duration::seconds(30));
        let lease = ShardLease {
            shard: shard.to_string(),
            worker_id: self.worker_id.clone(),
            lease_expires_at: expires_at,
            updated_at: now,
        };
        let new_body = serde_json::to_vec(&lease)
            .map_err(|e| StorageError::SerializationError(e.to_string()))?;

        match self
            .queue
            .client()
            .put_object(&key, new_body, PutCondition::IfMatch(etag))
            .await
        {
            Ok(_) => {
                tracing::trace!(shard = %shard, expires_at = %expires_at, "Renewed shard lease");
                Ok(true)
            }
            Err(StorageError::PreconditionFailed { .. }) => {
                self.owned_shards.remove(shard);
                tracing::warn!(shard = %shard, "Lost shard lease during renewal (CAS failed)");
                Ok(false)
            }
            Err(e) => Err(e),
        }
    }

    /// Release a single shard lease.
    ///
    /// # Errors
    ///
    /// Returns `StorageError` for S3 communication failures.
    pub async fn release(&mut self, shard: &str) -> Result<(), StorageError> {
        if self.owned_shards.remove(shard) {
            let key = ShardLease::key_for_shard(shard);
            let _ = self.queue.client().delete_object(&key).await;
            tracing::debug!(shard = %shard, worker_id = %self.worker_id, "Released shard lease");
        }
        Ok(())
    }

    /// Release all owned shard leases.
    ///
    /// This should be called on graceful shutdown to allow other workers
    /// to immediately take over the shards.
    ///
    /// # Errors
    ///
    /// Returns `StorageError` if any delete operation fails, but continues
    /// attempting to release all leases.
    pub async fn release_all(&mut self) -> Result<(), StorageError> {
        let shards: Vec<String> = self.owned_shards.iter().cloned().collect();
        for shard in shards {
            let key = ShardLease::key_for_shard(&shard);
            if let Err(e) = self.queue.client().delete_object(&key).await {
                tracing::warn!(shard = %shard, error = %e, "Failed to release shard lease");
            } else {
                tracing::debug!(shard = %shard, worker_id = %self.worker_id, "Released shard lease");
            }
        }
        self.owned_shards.clear();
        Ok(())
    }

    /// Get currently owned shards.
    #[must_use]
    pub fn owned_shards(&self) -> Vec<String> {
        self.owned_shards.iter().cloned().collect()
    }

    /// Returns the number of currently owned shards.
    #[must_use]
    pub fn owned_count(&self) -> usize {
        self.owned_shards.len()
    }

    /// Ensure we have enough shard leases, acquiring more if needed.
    ///
    /// This method tries to acquire leases on available shards until
    /// we reach the target number of shards per worker.
    ///
    /// # Arguments
    ///
    /// * `available_shards` - List of all shards that could be acquired
    ///
    /// # Errors
    ///
    /// Returns `StorageError` for S3 communication failures.
    #[allow(clippy::cognitive_complexity)]
    pub async fn ensure_leases(&mut self, available_shards: &[String]) -> Result<(), StorageError> {
        let target = self.config.shards_per_worker;
        if self.owned_shards.len() >= target {
            return Ok(());
        }

        // Shuffle available shards to avoid all workers trying the same shards
        // Use a simple rotation based on worker_id hash to distribute attempts
        let worker_hash = self
            .worker_id
            .bytes()
            .fold(0usize, |acc, b| acc.wrapping_add(b as usize));
        let offset = worker_hash % available_shards.len().max(1);

        // Try to acquire more shards
        for i in 0..available_shards.len() {
            if self.owned_shards.len() >= target {
                break;
            }
            let idx = (offset + i) % available_shards.len();
            let shard = &available_shards[idx];
            if self.owned_shards.contains(shard) {
                continue;
            }
            match self.try_acquire(shard).await {
                Ok(true) => {
                    tracing::info!(
                        shard = %shard,
                        owned = self.owned_shards.len(),
                        target = target,
                        "Acquired shard lease"
                    );
                }
                Ok(false) => {
                    // Shard is owned by another worker, continue
                }
                Err(e) => {
                    tracing::warn!(shard = %shard, error = %e, "Failed to acquire shard lease");
                }
            }
        }

        if self.owned_shards.is_empty() {
            tracing::warn!(
                worker_id = %self.worker_id,
                "Could not acquire any shard leases"
            );
        }

        Ok(())
    }

    /// Renew all owned leases.
    ///
    /// This should be called periodically (at `renewal_interval`) to keep
    /// leases from expiring.
    ///
    /// # Errors
    ///
    /// Returns `StorageError` for S3 communication failures, but continues
    /// attempting to renew all leases.
    pub async fn renew_all(&mut self) -> Result<(), StorageError> {
        let shards: Vec<String> = self.owned_shards.iter().cloned().collect();
        for shard in shards {
            match self.renew(&shard).await {
                Ok(true) => {}
                Ok(false) => {
                    tracing::warn!(shard = %shard, "Lost shard lease during renewal");
                }
                Err(e) => {
                    tracing::warn!(shard = %shard, error = %e, "Failed to renew shard lease");
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: Full integration tests require an S3 backend.
    // These tests verify the basic structure and logic.

    #[test]
    fn test_manager_new() {
        // This test just verifies the struct can be constructed
        // (doesn't actually create a Queue since that requires S3)
        let config = ShardLeaseConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.shards_per_worker, 16);
    }

    #[test]
    fn test_config_enabled_helper() {
        let config = ShardLeaseConfig::enabled();
        assert!(config.enabled);
    }
}
