//! Shard lease types for dynamic shard ownership.
//!
//! This module provides the core types for shard leasing, which allows workers
//! to dynamically acquire and manage ownership of shards rather than statically
//! assigning shards to workers.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// A shard lease stored in S3.
///
/// Shard leases are stored at `shard-leases/{shard}.json` and contain
/// information about which worker currently owns a shard and when the
/// lease expires.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardLease {
    /// The shard identifier (e.g., "0", "a", "f").
    pub shard: String,
    /// The worker ID that owns this lease.
    pub worker_id: String,
    /// When this lease expires (based on S3 authoritative time).
    pub lease_expires_at: DateTime<Utc>,
    /// When this lease was last updated.
    pub updated_at: DateTime<Utc>,
}

impl ShardLease {
    /// Returns the S3 key for this shard lease.
    #[must_use]
    pub fn key(&self) -> String {
        format!("shard-leases/{}.json", self.shard)
    }

    /// Returns the S3 key for a shard lease given a shard identifier.
    #[must_use]
    pub fn key_for_shard(shard: &str) -> String {
        format!("shard-leases/{shard}.json")
    }

    /// Returns true if this lease has expired.
    #[must_use]
    pub fn is_expired(&self, now: DateTime<Utc>) -> bool {
        self.lease_expires_at <= now
    }
}

/// Configuration for shard leasing.
///
/// Shard leasing is opt-in and disabled by default. When enabled, workers
/// will dynamically acquire leases on shards rather than polling all assigned
/// shards.
#[derive(Debug, Clone)]
pub struct ShardLeaseConfig {
    /// Whether shard leasing is enabled.
    pub enabled: bool,
    /// How many shards each worker should try to lease.
    pub shards_per_worker: usize,
    /// Lease duration (time until expiry).
    pub lease_ttl: Duration,
    /// How often to renew leases.
    pub renewal_interval: Duration,
}

impl Default for ShardLeaseConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            shards_per_worker: 16,
            lease_ttl: Duration::from_secs(30),
            renewal_interval: Duration::from_secs(10),
        }
    }
}

impl ShardLeaseConfig {
    /// Creates a new enabled shard lease configuration with default settings.
    #[must_use]
    pub fn enabled() -> Self {
        Self {
            enabled: true,
            ..Default::default()
        }
    }

    /// Creates a configuration from environment variables.
    ///
    /// Environment variables:
    /// - `BUQUET_SHARD_LEASING`: Set to "1" or "true" to enable
    /// - `BUQUET_SHARD_LEASING_SHARDS_PER_WORKER`: Number of shards per worker (default: 16)
    /// - `BUQUET_SHARD_LEASING_TTL_SECS`: Lease TTL in seconds (default: 30)
    /// - `BUQUET_SHARD_LEASING_RENEW_SECS`: Renewal interval in seconds (default: 10)
    #[must_use]
    pub fn from_env() -> Self {
        let enabled = std::env::var("BUQUET_SHARD_LEASING")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);

        let shards_per_worker = std::env::var("BUQUET_SHARD_LEASING_SHARDS_PER_WORKER")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(16);

        let lease_ttl_secs = std::env::var("BUQUET_SHARD_LEASING_TTL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(30);

        let renewal_interval_secs = std::env::var("BUQUET_SHARD_LEASING_RENEW_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(10);

        Self {
            enabled,
            shards_per_worker,
            lease_ttl: Duration::from_secs(lease_ttl_secs),
            renewal_interval: Duration::from_secs(renewal_interval_secs),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_shard_lease_key() {
        let lease = ShardLease {
            shard: "a".to_string(),
            worker_id: "worker-1".to_string(),
            lease_expires_at: Utc::now(),
            updated_at: Utc::now(),
        };
        assert_eq!(lease.key(), "shard-leases/a.json");
    }

    #[test]
    fn test_shard_lease_key_for_shard() {
        assert_eq!(ShardLease::key_for_shard("0"), "shard-leases/0.json");
        assert_eq!(ShardLease::key_for_shard("f"), "shard-leases/f.json");
    }

    #[test]
    fn test_shard_lease_is_expired() {
        let now = Utc::now();
        let past = now - chrono::Duration::seconds(10);
        let future = now + chrono::Duration::seconds(10);

        let expired_lease = ShardLease {
            shard: "0".to_string(),
            worker_id: "worker-1".to_string(),
            lease_expires_at: past,
            updated_at: past,
        };
        assert!(expired_lease.is_expired(now));

        let valid_lease = ShardLease {
            shard: "0".to_string(),
            worker_id: "worker-1".to_string(),
            lease_expires_at: future,
            updated_at: now,
        };
        assert!(!valid_lease.is_expired(now));
    }

    #[test]
    fn test_shard_lease_config_default() {
        let config = ShardLeaseConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.shards_per_worker, 16);
        assert_eq!(config.lease_ttl, Duration::from_secs(30));
        assert_eq!(config.renewal_interval, Duration::from_secs(10));
    }

    #[test]
    fn test_shard_lease_config_enabled() {
        let config = ShardLeaseConfig::enabled();
        assert!(config.enabled);
        assert_eq!(config.shards_per_worker, 16);
    }

    #[test]
    fn test_shard_lease_serialization() {
        let lease = ShardLease {
            shard: "0".to_string(),
            worker_id: "worker-abc".to_string(),
            lease_expires_at: Utc::now(),
            updated_at: Utc::now(),
        };

        let json = serde_json::to_string(&lease).unwrap();
        let deserialized: ShardLease = serde_json::from_str(&json).unwrap();
        assert_eq!(lease, deserialized);
    }
}
