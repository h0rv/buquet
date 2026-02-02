//! Tests for shard lease functionality.
//!
//! Note: Full integration tests with S3 are in the integration test suite.
//! These tests verify the basic logic and configuration without requiring S3.

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod shard_lease_config_tests {
    use crate::worker::ShardLeaseConfig;
    use std::sync::Mutex;
    use std::time::Duration;

    // Mutex to serialize env var tests (they share global state)
    static ENV_MUTEX: Mutex<()> = Mutex::new(());

    fn clear_env_vars() {
        std::env::remove_var("BUQUET_SHARD_LEASING");
        std::env::remove_var("BUQUET_SHARD_LEASING_SHARDS_PER_WORKER");
        std::env::remove_var("BUQUET_SHARD_LEASING_TTL_SECS");
        std::env::remove_var("BUQUET_SHARD_LEASING_RENEW_SECS");
    }

    #[test]
    fn test_config_default_is_disabled() {
        let config = ShardLeaseConfig::default();
        assert!(!config.enabled);
    }

    #[test]
    fn test_config_enabled_helper() {
        let config = ShardLeaseConfig::enabled();
        assert!(config.enabled);
        assert_eq!(config.shards_per_worker, 16);
        assert_eq!(config.lease_ttl, Duration::from_secs(30));
        assert_eq!(config.renewal_interval, Duration::from_secs(10));
    }

    #[test]
    fn test_config_from_env_defaults() {
        let _lock = ENV_MUTEX.lock().unwrap();
        clear_env_vars();

        let config = ShardLeaseConfig::from_env();
        assert!(!config.enabled);
        assert_eq!(config.shards_per_worker, 16);
        assert_eq!(config.lease_ttl, Duration::from_secs(30));
        assert_eq!(config.renewal_interval, Duration::from_secs(10));
    }

    #[test]
    fn test_config_from_env_enabled() {
        let _lock = ENV_MUTEX.lock().unwrap();
        clear_env_vars();

        std::env::set_var("BUQUET_SHARD_LEASING", "1");
        std::env::set_var("BUQUET_SHARD_LEASING_SHARDS_PER_WORKER", "8");
        std::env::set_var("BUQUET_SHARD_LEASING_TTL_SECS", "60");
        std::env::set_var("BUQUET_SHARD_LEASING_RENEW_SECS", "20");

        let config = ShardLeaseConfig::from_env();
        assert!(config.enabled);
        assert_eq!(config.shards_per_worker, 8);
        assert_eq!(config.lease_ttl, Duration::from_secs(60));
        assert_eq!(config.renewal_interval, Duration::from_secs(20));

        clear_env_vars();
    }

    #[test]
    fn test_config_from_env_enabled_true_string() {
        let _lock = ENV_MUTEX.lock().unwrap();
        clear_env_vars();

        std::env::set_var("BUQUET_SHARD_LEASING", "true");

        let config = ShardLeaseConfig::from_env();
        assert!(config.enabled);

        clear_env_vars();
    }

    #[test]
    fn test_config_from_env_disabled_false_string() {
        let _lock = ENV_MUTEX.lock().unwrap();
        clear_env_vars();

        std::env::set_var("BUQUET_SHARD_LEASING", "false");

        let config = ShardLeaseConfig::from_env();
        assert!(!config.enabled);

        clear_env_vars();
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod shard_lease_model_tests {
    use crate::worker::ShardLease;
    use chrono::{Duration, Utc};

    #[test]
    fn test_lease_key() {
        let lease = ShardLease {
            shard: "a".to_string(),
            worker_id: "worker-1".to_string(),
            lease_expires_at: Utc::now(),
            updated_at: Utc::now(),
        };
        assert_eq!(lease.key(), "shard-leases/a.json");
    }

    #[test]
    fn test_lease_key_for_shard() {
        assert_eq!(ShardLease::key_for_shard("0"), "shard-leases/0.json");
        assert_eq!(ShardLease::key_for_shard("f"), "shard-leases/f.json");
        assert_eq!(ShardLease::key_for_shard("10"), "shard-leases/10.json");
    }

    #[test]
    fn test_lease_is_expired() {
        let now = Utc::now();

        // Expired lease (expires_at is in the past)
        let expired = ShardLease {
            shard: "0".to_string(),
            worker_id: "worker-1".to_string(),
            lease_expires_at: now - Duration::seconds(10),
            updated_at: now - Duration::seconds(20),
        };
        assert!(expired.is_expired(now));

        // Valid lease (expires_at is in the future)
        let valid = ShardLease {
            shard: "0".to_string(),
            worker_id: "worker-1".to_string(),
            lease_expires_at: now + Duration::seconds(10),
            updated_at: now,
        };
        assert!(!valid.is_expired(now));

        // Exactly at expiry is considered expired
        let exact = ShardLease {
            shard: "0".to_string(),
            worker_id: "worker-1".to_string(),
            lease_expires_at: now,
            updated_at: now,
        };
        assert!(exact.is_expired(now));
    }

    #[test]
    fn test_lease_serialization_roundtrip() {
        let now = Utc::now();
        let lease = ShardLease {
            shard: "5".to_string(),
            worker_id: "worker-abc-123".to_string(),
            lease_expires_at: now + Duration::seconds(30),
            updated_at: now,
        };

        let json = serde_json::to_string(&lease).expect("serialization should succeed");
        let deserialized: ShardLease =
            serde_json::from_str(&json).expect("deserialization should succeed");

        assert_eq!(lease.shard, deserialized.shard);
        assert_eq!(lease.worker_id, deserialized.worker_id);
        assert_eq!(lease.lease_expires_at, deserialized.lease_expires_at);
        assert_eq!(lease.updated_at, deserialized.updated_at);
    }

    #[test]
    fn test_lease_json_format() {
        let now = Utc::now();
        let lease = ShardLease {
            shard: "0".to_string(),
            worker_id: "worker-1".to_string(),
            lease_expires_at: now,
            updated_at: now,
        };

        let json = serde_json::to_string(&lease).expect("serialization should succeed");

        // Verify the JSON contains expected fields
        assert!(json.contains("\"shard\":\"0\""));
        assert!(json.contains("\"worker_id\":\"worker-1\""));
        assert!(json.contains("\"lease_expires_at\""));
        assert!(json.contains("\"updated_at\""));
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod runner_config_tests {
    use crate::worker::{RunnerConfig, ShardLeaseConfig};
    use std::time::Duration;

    #[test]
    fn test_runner_config_default_has_no_shard_leasing() {
        let config = RunnerConfig::default();
        assert!(config.shard_lease_config.is_none());
    }

    #[test]
    #[allow(deprecated)]
    fn test_runner_config_with_shard_leasing() {
        use crate::worker::PollingStrategy;

        let config = RunnerConfig {
            polling: PollingStrategy::default(),
            poll_interval: Duration::from_millis(100),
            max_poll_interval: Duration::from_secs(5),
            backoff_multiplier: 1.5,
            shutdown_grace_period: Duration::from_secs(30),
            heartbeat_interval: Duration::from_secs(10),
            index_mode: crate::worker::IndexMode::Hybrid,
            ready_page_size: 100,
            log_scan_page_size: 50,
            shard_lease_config: Some(ShardLeaseConfig::enabled()),
        };

        assert!(config.shard_lease_config.is_some());
        let lease_config = config.shard_lease_config.unwrap();
        assert!(lease_config.enabled);
    }
}
