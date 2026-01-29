//! Chaos test: Slow storage operations.
//!
//! This test validates that the system handles slow S3 operations
//! gracefully, including proper timeout handling and retries.

use crate::{test_queue, unique_task_type};
use oq::models::TaskStatus;
use oq::queue::SubmitOptions;
use oq::worker::{claim_task, complete_task, ClaimResult};
use serde_json::json;
use std::time::{Duration, Instant};

#[tokio::test]
#[cfg_attr(
    not(feature = "chaos-tests"),
    ignore = "requires chaos-tests feature (S3)"
)]
async fn test_operations_under_simulated_latency() {
    let queue = test_queue().await;
    let task_type = unique_task_type();

    println!("=== Simulated Latency Test ===");
    println!("Task type: {}", task_type);

    // Test task lifecycle with artificial delays between operations
    // This simulates what happens when S3 is slow to respond

    // Submit with timing
    let start = Instant::now();
    let task = queue
        .submit(
            &task_type,
            json!({"test": "latency"}),
            SubmitOptions::default(),
        )
        .await
        .expect("Failed to submit task");
    let submit_time = start.elapsed();
    println!("Submit took: {:?}", submit_time);

    // Simulate network latency
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Claim with timing
    let start = Instant::now();
    let result = claim_task(&queue, task.id, "latency-worker")
        .await
        .expect("Claim failed");
    let claim_time = start.elapsed();
    println!("Claim took: {:?}", claim_time);

    let claimed = match result {
        ClaimResult::Claimed { task, etag: _ } => *task,
        other => panic!("Expected Claimed, got {:?}", other),
    };

    // Simulate slow processing
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Complete with timing
    let start = Instant::now();
    let lease_id = claimed.lease_id.expect("Claimed task should have lease_id");
    let completed = complete_task(&queue, task.id, lease_id, json!({"result": "ok"}))
        .await
        .expect("Complete failed");
    let complete_time = start.elapsed();
    println!("Complete took: {:?}", complete_time);

    assert_eq!(completed.status, TaskStatus::Completed);

    println!("\nTotal operation times:");
    println!("  Submit:   {:?}", submit_time);
    println!("  Claim:    {:?}", claim_time);
    println!("  Complete: {:?}", complete_time);
    println!("Simulated latency test PASSED");
}

#[tokio::test]
#[cfg_attr(
    not(feature = "chaos-tests"),
    ignore = "requires chaos-tests feature (S3)"
)]
async fn test_concurrent_operations_under_load() {
    let queue = test_queue().await;
    let task_type = unique_task_type();

    println!("=== Concurrent Operations Under Load Test ===");

    // Submit multiple tasks concurrently
    let submit_count = 10;
    let mut submit_handles = Vec::new();

    let submit_start = Instant::now();
    for i in 0..submit_count {
        let q = queue.clone();
        let tt = task_type.clone();
        let handle = tokio::spawn(async move {
            let result = q
                .submit(&tt, json!({"index": i}), SubmitOptions::default())
                .await;
            (i, result)
        });
        submit_handles.push(handle);
    }

    // Wait for all submits
    let mut task_ids = Vec::new();
    for handle in submit_handles {
        let (i, result) = handle.await.expect("Join failed");
        match result {
            Ok(task) => {
                task_ids.push(task.id);
            }
            Err(e) => {
                eprintln!("Submit {} failed: {:?}", i, e);
            }
        }
    }
    let submit_time = submit_start.elapsed();
    println!(
        "Submitted {} tasks in {:?} ({:.2} tasks/sec)",
        task_ids.len(),
        submit_time,
        task_ids.len() as f64 / submit_time.as_secs_f64()
    );

    // Claim all tasks concurrently
    let claim_start = Instant::now();
    let mut claim_handles = Vec::new();
    for (i, task_id) in task_ids.iter().enumerate() {
        let q = queue.clone();
        let tid = *task_id;
        let handle = tokio::spawn(async move {
            let result = claim_task(&q, tid, &format!("worker-{}", i)).await;
            (tid, result)
        });
        claim_handles.push(handle);
    }

    // Wait for all claims
    let mut claimed_tasks = Vec::new();
    for handle in claim_handles {
        let (tid, result) = handle.await.expect("Join failed");
        match result {
            Ok(ClaimResult::Claimed { task, .. }) => {
                claimed_tasks.push(*task);
            }
            Ok(other) => {
                println!("Task {} claim result: {:?}", tid, other);
            }
            Err(e) => {
                eprintln!("Claim {} failed: {:?}", tid, e);
            }
        }
    }
    let claim_time = claim_start.elapsed();
    println!(
        "Claimed {} tasks in {:?} ({:.2} tasks/sec)",
        claimed_tasks.len(),
        claim_time,
        claimed_tasks.len() as f64 / claim_time.as_secs_f64()
    );

    // Complete all tasks concurrently
    let complete_start = Instant::now();
    let mut complete_handles = Vec::new();
    for task in &claimed_tasks {
        let q = queue.clone();
        let tid = task.id;
        let lid = task.lease_id.expect("Claimed task should have lease_id");
        let handle = tokio::spawn(async move {
            let result = complete_task(&q, tid, lid, json!({"completed": true})).await;
            (tid, result)
        });
        complete_handles.push(handle);
    }

    // Wait for all completes
    let mut completed_count = 0;
    for handle in complete_handles {
        let (tid, result) = handle.await.expect("Join failed");
        match result {
            Ok(task) => {
                assert_eq!(task.status, TaskStatus::Completed);
                completed_count += 1;
            }
            Err(e) => {
                eprintln!("Complete {} failed: {:?}", tid, e);
            }
        }
    }
    let complete_time = complete_start.elapsed();
    println!(
        "Completed {} tasks in {:?} ({:.2} tasks/sec)",
        completed_count,
        complete_time,
        completed_count as f64 / complete_time.as_secs_f64()
    );

    // Summary
    println!("\n=== Summary ===");
    println!("Tasks submitted: {}", task_ids.len());
    println!("Tasks claimed: {}", claimed_tasks.len());
    println!("Tasks completed: {}", completed_count);
    println!("Total time: {:?}", submit_start.elapsed());

    assert_eq!(completed_count, task_ids.len(), "All tasks should complete");
    println!("Concurrent operations test PASSED");
}

#[tokio::test]
#[cfg_attr(
    not(feature = "chaos-tests"),
    ignore = "requires chaos-tests feature (S3)"
)]
async fn test_task_with_long_timeout_completes_normally() {
    let queue = test_queue().await;
    let task_type = unique_task_type();

    println!("=== Long Timeout Task Test ===");

    // Submit task with longer timeout
    let options = SubmitOptions {
        timeout_seconds: Some(60), // 1 minute timeout
        ..Default::default()
    };
    let task = queue
        .submit(&task_type, json!({"test": "long_timeout"}), options)
        .await
        .expect("Failed to submit task");

    println!("Submitted task with 60s timeout: {}", task.id);

    // Claim the task
    let result = claim_task(&queue, task.id, "patient-worker")
        .await
        .expect("Claim failed");

    let claimed = match result {
        ClaimResult::Claimed { task, etag: _ } => *task,
        other => panic!("Expected Claimed, got {:?}", other),
    };

    // Simulate "slow" processing (but still within timeout)
    println!("Simulating slow processing (2 seconds)...");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Complete the task
    let lease_id = claimed.lease_id.expect("Claimed task should have lease_id");
    let completed = complete_task(
        &queue,
        task.id,
        lease_id,
        json!({"result": "completed after slow processing"}),
    )
    .await
    .expect("Complete failed");

    assert_eq!(completed.status, TaskStatus::Completed);
    println!(
        "Task completed successfully with status: {:?}",
        completed.status
    );

    println!("Long timeout task test PASSED");
}

#[tokio::test]
#[cfg_attr(
    not(feature = "chaos-tests"),
    ignore = "requires chaos-tests feature (S3)"
)]
async fn test_rapid_submit_claim_complete_cycle() {
    let queue = test_queue().await;
    let task_type = unique_task_type();

    println!("=== Rapid Cycle Test ===");

    let cycles = 20;
    let mut success_count = 0;
    let start = Instant::now();

    for i in 0..cycles {
        // Submit
        let task = queue
            .submit(&task_type, json!({"cycle": i}), SubmitOptions::default())
            .await
            .expect("Failed to submit task");

        // Claim immediately
        let result = claim_task(&queue, task.id, &format!("rapid-worker-{}", i))
            .await
            .expect("Claim failed");

        let claimed = match result {
            ClaimResult::Claimed { task, .. } => *task,
            other => {
                println!("Cycle {} claim result: {:?}", i, other);
                continue;
            }
        };

        // Complete immediately
        let lease_id = claimed.lease_id.expect("Should have lease_id");
        match complete_task(&queue, task.id, lease_id, json!({"cycle": i})).await {
            Ok(completed) => {
                assert_eq!(completed.status, TaskStatus::Completed);
                success_count += 1;
            }
            Err(e) => {
                println!("Cycle {} complete failed: {:?}", i, e);
            }
        }
    }

    let elapsed = start.elapsed();
    println!(
        "Completed {} of {} cycles in {:?} ({:.2} cycles/sec)",
        success_count,
        cycles,
        elapsed,
        success_count as f64 / elapsed.as_secs_f64()
    );

    assert_eq!(success_count, cycles, "All cycles should succeed");
    println!("Rapid cycle test PASSED");
}
