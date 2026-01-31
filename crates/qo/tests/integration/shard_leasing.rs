//! Integration tests for shard leasing feature.
//!
//! Tests for acquiring, renewing, and taking over shard leases.

use qo::worker::{ShardLeaseConfig, ShardLeaseManager};
use std::time::Duration;
use uuid::Uuid;

use crate::common::test_queue;

/// Generates a unique shard name for test isolation.
fn unique_shard() -> String {
    format!("test-shard-{}", Uuid::new_v4())
}

/// Test that a worker can acquire a shard lease.
#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_acquire_shard_lease() {
    let queue = test_queue().await;
    let shard = unique_shard();
    let worker_id = format!("worker-{}", Uuid::new_v4());

    let config = ShardLeaseConfig {
        enabled: true,
        shards_per_worker: 1,
        lease_ttl: Duration::from_secs(30),
        renewal_interval: Duration::from_secs(10),
    };

    let mut manager = ShardLeaseManager::new(queue, worker_id.clone(), config);

    // Initially, we own no shards
    assert_eq!(manager.owned_count(), 0);

    // Acquire the shard
    let acquired = manager
        .try_acquire(&shard)
        .await
        .expect("Failed to acquire shard");
    assert!(acquired, "Should successfully acquire available shard");

    // Now we own the shard
    assert_eq!(manager.owned_count(), 1);
    assert!(manager.owned_shards().contains(&shard));

    // Cleanup - release the lease
    manager
        .release(&shard)
        .await
        .expect("Failed to release shard");
    assert_eq!(manager.owned_count(), 0);
}

/// Test that the same worker can re-acquire a lease it already owns.
#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_reacquire_own_lease() {
    let queue = test_queue().await;
    let shard = unique_shard();
    let worker_id = format!("worker-{}", Uuid::new_v4());

    let config = ShardLeaseConfig {
        enabled: true,
        shards_per_worker: 1,
        lease_ttl: Duration::from_secs(30),
        renewal_interval: Duration::from_secs(10),
    };

    let mut manager = ShardLeaseManager::new(queue, worker_id.clone(), config);

    // Acquire the shard
    let acquired = manager
        .try_acquire(&shard)
        .await
        .expect("Failed to acquire shard");
    assert!(acquired);

    // Try to acquire again - should succeed because we already own it
    let reacquired = manager
        .try_acquire(&shard)
        .await
        .expect("Failed to reacquire shard");
    assert!(reacquired, "Should succeed when reacquiring own lease");

    // Still own exactly one shard
    assert_eq!(manager.owned_count(), 1);

    // Cleanup
    manager.release_all().await.expect("Failed to release all");
}

/// Test that lease renewal works.
#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_lease_renewal() {
    let queue = test_queue().await;
    let shard = unique_shard();
    let worker_id = format!("worker-{}", Uuid::new_v4());

    let config = ShardLeaseConfig {
        enabled: true,
        shards_per_worker: 1,
        lease_ttl: Duration::from_secs(30),
        renewal_interval: Duration::from_secs(10),
    };

    let mut manager = ShardLeaseManager::new(queue, worker_id.clone(), config);

    // Acquire the shard
    let acquired = manager
        .try_acquire(&shard)
        .await
        .expect("Failed to acquire shard");
    assert!(acquired);

    // Renew the lease
    let renewed = manager.renew(&shard).await.expect("Failed to renew");
    assert!(renewed, "Renewal should succeed");

    // Still own the shard
    assert_eq!(manager.owned_count(), 1);

    // Cleanup
    manager.release_all().await.expect("Failed to release all");
}

/// Test that renew_all renews all owned leases.
#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_renew_all() {
    let queue = test_queue().await;
    let shard1 = unique_shard();
    let shard2 = unique_shard();
    let worker_id = format!("worker-{}", Uuid::new_v4());

    let config = ShardLeaseConfig {
        enabled: true,
        shards_per_worker: 2,
        lease_ttl: Duration::from_secs(30),
        renewal_interval: Duration::from_secs(10),
    };

    let mut manager = ShardLeaseManager::new(queue, worker_id.clone(), config);

    // Acquire both shards
    manager
        .try_acquire(&shard1)
        .await
        .expect("Failed to acquire shard1");
    manager
        .try_acquire(&shard2)
        .await
        .expect("Failed to acquire shard2");

    assert_eq!(manager.owned_count(), 2);

    // Renew all leases
    manager.renew_all().await.expect("Failed to renew all");

    // Still own both shards
    assert_eq!(manager.owned_count(), 2);

    // Cleanup
    manager.release_all().await.expect("Failed to release all");
}

/// Test that a second worker cannot acquire a lease held by another worker.
#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_cannot_acquire_active_lease() {
    let queue1 = test_queue().await;
    let queue2 = test_queue().await;
    let shard = unique_shard();
    let worker1_id = format!("worker-1-{}", Uuid::new_v4());
    let worker2_id = format!("worker-2-{}", Uuid::new_v4());

    let config = ShardLeaseConfig {
        enabled: true,
        shards_per_worker: 1,
        lease_ttl: Duration::from_secs(30),
        renewal_interval: Duration::from_secs(10),
    };

    let mut manager1 = ShardLeaseManager::new(queue1, worker1_id.clone(), config.clone());
    let mut manager2 = ShardLeaseManager::new(queue2, worker2_id.clone(), config);

    // Worker 1 acquires the shard
    let acquired = manager1
        .try_acquire(&shard)
        .await
        .expect("Failed to acquire shard");
    assert!(acquired, "Worker 1 should acquire the shard");

    // Worker 2 tries to acquire the same shard - should fail
    let acquired2 = manager2
        .try_acquire(&shard)
        .await
        .expect("Failed to attempt acquire");
    assert!(
        !acquired2,
        "Worker 2 should not acquire shard held by worker 1"
    );

    // Cleanup
    manager1.release_all().await.expect("Failed to release all");
}

/// Test that expired leases can be taken over by another worker.
#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_takeover_expired_lease() {
    let queue1 = test_queue().await;
    let queue2 = test_queue().await;
    let shard = unique_shard();
    let worker1_id = format!("worker-1-{}", Uuid::new_v4());
    let worker2_id = format!("worker-2-{}", Uuid::new_v4());

    // Worker 1 uses a very short TTL so lease expires quickly
    let config1 = ShardLeaseConfig {
        enabled: true,
        shards_per_worker: 1,
        lease_ttl: Duration::from_secs(2), // Very short TTL
        renewal_interval: Duration::from_secs(1),
    };

    let config2 = ShardLeaseConfig {
        enabled: true,
        shards_per_worker: 1,
        lease_ttl: Duration::from_secs(30),
        renewal_interval: Duration::from_secs(10),
    };

    let mut manager1 = ShardLeaseManager::new(queue1, worker1_id.clone(), config1);
    let mut manager2 = ShardLeaseManager::new(queue2, worker2_id.clone(), config2);

    // Worker 1 acquires the shard
    let acquired = manager1
        .try_acquire(&shard)
        .await
        .expect("Failed to acquire shard");
    assert!(acquired, "Worker 1 should acquire the shard");

    // Worker 2 cannot acquire yet (lease is active)
    let acquired2 = manager2
        .try_acquire(&shard)
        .await
        .expect("Failed to attempt acquire");
    assert!(!acquired2, "Worker 2 should not acquire active lease");

    // Wait for worker 1's lease to expire
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Now worker 2 should be able to take over the expired lease
    let acquired3 = manager2
        .try_acquire(&shard)
        .await
        .expect("Failed to acquire expired lease");
    assert!(
        acquired3,
        "Worker 2 should take over expired lease from worker 1"
    );

    // Verify worker 2 now owns the shard
    assert_eq!(manager2.owned_count(), 1);
    assert!(manager2.owned_shards().contains(&shard));

    // Cleanup
    manager2.release_all().await.expect("Failed to release all");
}

/// Test that releasing a lease allows another worker to acquire it immediately.
#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_release_allows_immediate_acquire() {
    let queue1 = test_queue().await;
    let queue2 = test_queue().await;
    let shard = unique_shard();
    let worker1_id = format!("worker-1-{}", Uuid::new_v4());
    let worker2_id = format!("worker-2-{}", Uuid::new_v4());

    let config = ShardLeaseConfig {
        enabled: true,
        shards_per_worker: 1,
        lease_ttl: Duration::from_secs(30),
        renewal_interval: Duration::from_secs(10),
    };

    let mut manager1 = ShardLeaseManager::new(queue1, worker1_id.clone(), config.clone());
    let mut manager2 = ShardLeaseManager::new(queue2, worker2_id.clone(), config);

    // Worker 1 acquires the shard
    let acquired = manager1
        .try_acquire(&shard)
        .await
        .expect("Failed to acquire shard");
    assert!(acquired);

    // Worker 1 releases the shard
    manager1
        .release(&shard)
        .await
        .expect("Failed to release shard");
    assert_eq!(manager1.owned_count(), 0);

    // Worker 2 should now be able to acquire the shard immediately
    let acquired2 = manager2
        .try_acquire(&shard)
        .await
        .expect("Failed to acquire released shard");
    assert!(
        acquired2,
        "Worker 2 should acquire the released shard immediately"
    );

    // Cleanup
    manager2.release_all().await.expect("Failed to release all");
}

/// Test ensure_leases acquires up to the target number of shards.
#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_ensure_leases() {
    let queue = test_queue().await;
    let worker_id = format!("worker-{}", Uuid::new_v4());

    let config = ShardLeaseConfig {
        enabled: true,
        shards_per_worker: 3, // Target 3 shards
        lease_ttl: Duration::from_secs(30),
        renewal_interval: Duration::from_secs(10),
    };

    let mut manager = ShardLeaseManager::new(queue, worker_id.clone(), config);

    // Create a list of available shards
    let available_shards: Vec<String> = (0..5).map(|_| unique_shard()).collect();

    // Initially own nothing
    assert_eq!(manager.owned_count(), 0);

    // Ensure we have enough leases
    manager
        .ensure_leases(&available_shards)
        .await
        .expect("Failed to ensure leases");

    // Should have acquired up to shards_per_worker (3) shards
    assert_eq!(
        manager.owned_count(),
        3,
        "Should own exactly shards_per_worker shards"
    );

    // Cleanup
    manager.release_all().await.expect("Failed to release all");
}

/// Test that renewing a shard we don't own returns false.
#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_renew_unowned_shard() {
    let queue = test_queue().await;
    let shard = unique_shard();
    let worker_id = format!("worker-{}", Uuid::new_v4());

    let config = ShardLeaseConfig {
        enabled: true,
        shards_per_worker: 1,
        lease_ttl: Duration::from_secs(30),
        renewal_interval: Duration::from_secs(10),
    };

    let mut manager = ShardLeaseManager::new(queue, worker_id.clone(), config);

    // Try to renew a shard we never acquired
    let renewed = manager
        .renew(&shard)
        .await
        .expect("Failed to attempt renew");
    assert!(!renewed, "Should not renew a shard we don't own");
}
