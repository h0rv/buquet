//! Test: Worker registration and heartbeat tracking.

use buquet::models::WorkerInfo;
use buquet::storage::PutCondition;
use chrono::Duration;

use crate::common::test_queue;

#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_worker_registration() {
    let queue = test_queue().await;

    // Create and register a worker
    let worker_info = WorkerInfo::new("test-worker-reg", vec!["0".to_string(), "1".to_string()]);

    let body = serde_json::to_vec(&worker_info).expect("serialize");
    queue
        .client()
        .put_object(&worker_info.key(), body, PutCondition::None)
        .await
        .expect("put worker");

    // Verify worker exists
    let (body, _) = queue
        .client()
        .get_object(&worker_info.key())
        .await
        .expect("get worker");
    let retrieved: WorkerInfo = serde_json::from_slice(&body).expect("deserialize");

    assert_eq!(retrieved.worker_id, "test-worker-reg");
    assert_eq!(retrieved.shards, vec!["0", "1"]);
    assert!(retrieved.is_healthy_at(chrono::Utc::now(), Duration::seconds(60)));

    // Cleanup
    queue
        .client()
        .delete_object(&worker_info.key())
        .await
        .expect("delete");
}

#[tokio::test]
async fn test_worker_health_detection() {
    let mut worker = WorkerInfo::new("health-test", vec![]);

    // Fresh worker should be healthy
    assert!(worker.is_healthy_at(chrono::Utc::now(), Duration::seconds(60)));

    // Manually set old heartbeat
    worker.last_heartbeat = chrono::Utc::now() - Duration::seconds(120);

    // Now should be unhealthy
    assert!(!worker.is_healthy_at(chrono::Utc::now(), Duration::seconds(60)));
}
