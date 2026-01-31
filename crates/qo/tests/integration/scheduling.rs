//! Integration tests for qo scheduling features.
//!
//! Tests for one-shot scheduling (schedule_at) and recurring schedules.

use chrono::{DateTime, Duration, Utc};
use qo::models::TaskStatus;
use qo::queue::{run_scheduler_tick, Schedule, ScheduleLastRun, ScheduleManager, SubmitOptions};
use qo::worker::{claim_task, ClaimResult};
use serde_json::json;
use uuid::Uuid;

use crate::common::{test_queue, unique_task_type};

// ============================================================================
// One-shot scheduling tests (schedule_at)
// ============================================================================

/// Test that a task with a future schedule_at is not immediately claimable.
#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_submit_with_schedule_at() {
    let queue = test_queue().await;
    let task_type = unique_task_type();

    // Schedule task 1 hour in the future
    let future_time = Utc::now() + Duration::hours(1);
    let options = SubmitOptions {
        schedule_at: Some(future_time),
        ..Default::default()
    };

    let task = queue
        .submit(&task_type, json!({"scheduled": true}), options)
        .await
        .expect("Failed to submit scheduled task");

    assert_eq!(task.status, TaskStatus::Pending);
    assert_eq!(task.available_at, future_time);

    // Try to claim the task - should fail because it's not yet available
    let result = claim_task(&queue, task.id, "test-worker")
        .await
        .expect("Failed to attempt claim");

    match result {
        ClaimResult::NotAvailable => {
            // Expected - task is scheduled for the future
        }
        other => panic!("Expected NotAvailable, got {:?}", other),
    }
}

/// Test that a task with a past schedule_at is immediately claimable.
#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_submit_with_schedule_at_past() {
    let queue = test_queue().await;
    let task_type = unique_task_type();

    // Schedule task 1 hour in the past
    let past_time = Utc::now() - Duration::hours(1);
    let options = SubmitOptions {
        schedule_at: Some(past_time),
        ..Default::default()
    };

    let task = queue
        .submit(&task_type, json!({"scheduled_past": true}), options)
        .await
        .expect("Failed to submit task with past schedule");

    assert_eq!(task.status, TaskStatus::Pending);

    // Try to claim the task - should succeed because schedule_at is in the past
    let result = claim_task(&queue, task.id, "test-worker")
        .await
        .expect("Failed to claim");

    match result {
        ClaimResult::Claimed { task, .. } => {
            assert_eq!(task.status, TaskStatus::Running);
            assert_eq!(task.worker_id.as_deref(), Some("test-worker"));
        }
        other => panic!("Expected Claimed, got {:?}", other),
    }
}

/// Test that a scheduled task becomes available after the scheduled time passes.
#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_scheduled_task_becomes_available() {
    let queue = test_queue().await;
    let task_type = unique_task_type();

    // Schedule task 2 seconds in the future
    let schedule_time = Utc::now() + Duration::seconds(2);
    let options = SubmitOptions {
        schedule_at: Some(schedule_time),
        ..Default::default()
    };

    let task = queue
        .submit(&task_type, json!({"will_become_available": true}), options)
        .await
        .expect("Failed to submit scheduled task");

    // Verify it's not claimable yet
    let result = claim_task(&queue, task.id, "test-worker")
        .await
        .expect("Failed to attempt claim");
    assert!(
        matches!(result, ClaimResult::NotAvailable),
        "Task should not be available yet"
    );

    // Wait for the schedule time to pass
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    // Now the task should be claimable
    let result = claim_task(&queue, task.id, "test-worker")
        .await
        .expect("Failed to claim after wait");

    match result {
        ClaimResult::Claimed { task, .. } => {
            assert_eq!(task.status, TaskStatus::Running);
        }
        other => panic!(
            "Expected Claimed after schedule time passed, got {:?}",
            other
        ),
    }
}

// ============================================================================
// Recurring schedule tests
// ============================================================================

/// Helper to create a unique schedule ID for test isolation.
fn unique_schedule_id() -> String {
    format!("test-schedule-{}", Uuid::new_v4())
}

/// Test creating a schedule and verifying it's stored.
#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_create_schedule() {
    let queue = test_queue().await;
    let manager = ScheduleManager::new(queue.client().clone());
    let schedule_id = unique_schedule_id();
    let task_type = unique_task_type();

    let schedule = Schedule::new(
        &schedule_id,
        &task_type,
        json!({"recurring": true}),
        "0 9 * * *", // Daily at 9 AM
    );

    // Create the schedule
    manager
        .create(&schedule)
        .await
        .expect("Failed to create schedule");

    // Retrieve and verify
    let result = manager
        .get(&schedule_id)
        .await
        .expect("Failed to get schedule");
    assert!(result.is_some(), "Schedule should exist");

    let (retrieved, _etag) = result.unwrap();
    assert_eq!(retrieved.id, schedule_id);
    assert_eq!(retrieved.task_type, task_type);
    assert_eq!(retrieved.cron, "0 9 * * *");
    assert!(retrieved.enabled);

    // Cleanup
    manager
        .delete(&schedule_id)
        .await
        .expect("Failed to delete schedule");
}

/// Test listing multiple schedules.
#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_list_schedules() {
    let queue = test_queue().await;
    let manager = ScheduleManager::new(queue.client().clone());

    // Create several unique schedules
    let schedule_ids: Vec<String> = (0..3).map(|_| unique_schedule_id()).collect();

    for (i, schedule_id) in schedule_ids.iter().enumerate() {
        let schedule = Schedule::new(
            schedule_id,
            &format!("task-type-{}", i),
            json!({"index": i}),
            "*/5 * * * *", // Every 5 minutes
        );
        manager
            .create(&schedule)
            .await
            .expect("Failed to create schedule");
    }

    // List all schedules
    let all_schedules = manager.list().await.expect("Failed to list schedules");

    // Verify our schedules are in the list
    for schedule_id in &schedule_ids {
        assert!(
            all_schedules.iter().any(|s| s.id == *schedule_id),
            "Schedule {} should be in the list",
            schedule_id
        );
    }

    // Cleanup
    for schedule_id in &schedule_ids {
        manager
            .delete(schedule_id)
            .await
            .expect("Failed to delete schedule");
    }
}

/// Test deleting a schedule.
#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_delete_schedule() {
    let queue = test_queue().await;
    let manager = ScheduleManager::new(queue.client().clone());
    let schedule_id = unique_schedule_id();

    let schedule = Schedule::new(
        &schedule_id,
        "delete-test-task",
        json!({}),
        "0 0 * * *", // Daily at midnight
    );

    // Create the schedule
    manager
        .create(&schedule)
        .await
        .expect("Failed to create schedule");

    // Verify it exists
    let result = manager
        .get(&schedule_id)
        .await
        .expect("Failed to get schedule");
    assert!(result.is_some());

    // Delete it
    manager
        .delete(&schedule_id)
        .await
        .expect("Failed to delete schedule");

    // Verify it's gone
    let result = manager
        .get(&schedule_id)
        .await
        .expect("Failed to get schedule");
    assert!(result.is_none(), "Schedule should be deleted");
}

/// Test enabling and disabling a schedule.
#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_enable_disable_schedule() {
    let queue = test_queue().await;
    let manager = ScheduleManager::new(queue.client().clone());
    let schedule_id = unique_schedule_id();

    let schedule = Schedule::new(
        &schedule_id,
        "toggle-test-task",
        json!({}),
        "30 * * * *", // Every hour at :30
    );

    manager
        .create(&schedule)
        .await
        .expect("Failed to create schedule");

    // Verify initially enabled
    let (retrieved, _) = manager
        .get(&schedule_id)
        .await
        .expect("Failed to get schedule")
        .expect("Schedule should exist");
    assert!(retrieved.enabled, "Schedule should be enabled by default");

    // Disable it
    manager
        .disable(&schedule_id)
        .await
        .expect("Failed to disable schedule");

    let (retrieved, _) = manager
        .get(&schedule_id)
        .await
        .expect("Failed to get schedule")
        .expect("Schedule should exist");
    assert!(!retrieved.enabled, "Schedule should be disabled");

    // Enable it again
    manager
        .enable(&schedule_id)
        .await
        .expect("Failed to enable schedule");

    let (retrieved, _) = manager
        .get(&schedule_id)
        .await
        .expect("Failed to get schedule")
        .expect("Schedule should exist");
    assert!(retrieved.enabled, "Schedule should be enabled again");

    // Cleanup
    manager
        .delete(&schedule_id)
        .await
        .expect("Failed to delete schedule");
}

/// Test the is_due() logic with different times.
#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_schedule_is_due() {
    let queue = test_queue().await;
    let manager = ScheduleManager::new(queue.client().clone());
    let schedule_id = unique_schedule_id();

    // Create a schedule that runs every minute
    let schedule = Schedule::new(
        &schedule_id,
        "due-test-task",
        json!({}),
        "* * * * *", // Every minute
    );

    manager
        .create(&schedule)
        .await
        .expect("Failed to create schedule");

    // Get the schedule
    let (schedule, _) = manager
        .get(&schedule_id)
        .await
        .expect("Failed to get schedule")
        .expect("Schedule should exist");

    // With no last run, the schedule should be due (since it runs every minute)
    let now = Utc::now();
    assert!(
        schedule.is_due(now, None),
        "Schedule with no last run should be due"
    );

    // If we just ran it, it should not be due until the next minute
    let last_run = ScheduleLastRun {
        schedule_id: schedule_id.clone(),
        last_run_at: now,
        last_task_id: Uuid::new_v4(),
        next_run_at: schedule.next_run_after(now).unwrap_or(now),
    };
    assert!(
        !schedule.is_due(now, Some(&last_run)),
        "Schedule that just ran should not be due immediately"
    );

    // If the last run was 2 minutes ago, it should be due
    let old_last_run = ScheduleLastRun {
        schedule_id: schedule_id.clone(),
        last_run_at: now - Duration::minutes(2),
        last_task_id: Uuid::new_v4(),
        next_run_at: now - Duration::minutes(1),
    };
    assert!(
        schedule.is_due(now, Some(&old_last_run)),
        "Schedule with old last run should be due"
    );

    // Disabled schedule should never be due
    let mut disabled_schedule = schedule.clone();
    disabled_schedule.enabled = false;
    assert!(
        !disabled_schedule.is_due(now, None),
        "Disabled schedule should not be due"
    );

    // Cleanup
    manager
        .delete(&schedule_id)
        .await
        .expect("Failed to delete schedule");
}

/// Test manually triggering a schedule by submitting a task.
#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_trigger_schedule() {
    let queue = test_queue().await;
    let manager = ScheduleManager::new(queue.client().clone());
    let schedule_id = unique_schedule_id();
    let task_type = unique_task_type();

    let schedule = Schedule::new(
        &schedule_id,
        &task_type,
        json!({"trigger_data": "from_schedule"}),
        "0 0 1 1 *", // Once a year (Jan 1 at midnight) - won't auto-trigger
    );

    manager
        .create(&schedule)
        .await
        .expect("Failed to create schedule");

    // Manually trigger by submitting a task based on the schedule
    let (schedule, _) = manager
        .get(&schedule_id)
        .await
        .expect("Failed to get schedule")
        .expect("Schedule should exist");

    let task = queue
        .submit(
            &schedule.task_type,
            schedule.input.clone(),
            SubmitOptions::default(),
        )
        .await
        .expect("Failed to submit task from schedule");

    assert_eq!(task.task_type, task_type);
    assert_eq!(task.input, json!({"trigger_data": "from_schedule"}));
    assert_eq!(task.status, TaskStatus::Pending);

    // Update last run tracking
    let now = Utc::now();
    let last_run = ScheduleLastRun {
        schedule_id: schedule_id.clone(),
        last_run_at: now,
        last_task_id: task.id,
        next_run_at: schedule.next_run_after(now).unwrap_or(now),
    };

    manager
        .update_last_run(&last_run, None)
        .await
        .expect("Failed to update last run");

    // Verify last run was recorded
    let (retrieved_last_run, _) = manager
        .get_last_run(&schedule_id)
        .await
        .expect("Failed to get last run")
        .expect("Last run should exist");

    assert_eq!(retrieved_last_run.schedule_id, schedule_id);
    assert_eq!(retrieved_last_run.last_task_id, task.id);

    // Verify the task is claimable
    let result = claim_task(&queue, task.id, "test-worker")
        .await
        .expect("Failed to claim task");

    match result {
        ClaimResult::Claimed {
            task: claimed_task, ..
        } => {
            assert_eq!(claimed_task.status, TaskStatus::Running);
            assert_eq!(claimed_task.input, json!({"trigger_data": "from_schedule"}));
        }
        other => panic!("Expected Claimed, got {:?}", other),
    }

    // Cleanup
    manager
        .delete(&schedule_id)
        .await
        .expect("Failed to delete schedule");
}

/// Test that a scheduler tick updates last_task_id when a task is created.
#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_scheduler_tick_updates_last_task_id() {
    let queue = test_queue().await;
    let manager = ScheduleManager::new(queue.client().clone());
    let schedule_id = unique_schedule_id();
    let task_type = unique_task_type();

    let schedule = Schedule::new(
        &schedule_id,
        &task_type,
        json!({"scheduled": true}),
        "* * * * *", // Every minute
    );

    // Best-effort cleanup for retries or stale idempotency records.
    let _ = manager.delete(&schedule_id).await;
    if let Ok(keys) = queue
        .client()
        .list_objects_paginated(&format!("idempotency/{task_type}/"), 10_000)
        .await
    {
        for key in keys {
            let _ = queue.client().delete_object(&key).await;
        }
    }

    manager
        .create(&schedule)
        .await
        .expect("Failed to create schedule");

    let mut schedule_visible = false;
    for _ in 0..10 {
        let schedules = manager.list().await.expect("Failed to list schedules");
        if schedules.iter().any(|s| s.id == schedule_id) {
            schedule_visible = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(schedule_visible, "Schedule should be visible in list");

    let mut last_run = None;
    for _ in 0..10 {
        let now = queue
            .now()
            .await
            .expect("Failed to fetch authoritative queue time");
        let report = run_scheduler_tick(&queue, &manager, now)
            .await
            .expect("Failed to run scheduler tick");
        let relevant_errors: Vec<_> = report
            .errors
            .iter()
            .filter(|err| err.schedule_id == schedule_id)
            .collect();
        if !relevant_errors.is_empty() {
            panic!("Scheduler tick reported errors: {:?}", relevant_errors);
        }
        if let Ok(Some(found)) = manager.get_last_run(&schedule_id).await {
            last_run = Some(found);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    let (last_run, _) = last_run.expect("Expected last_run to be recorded");

    let (task, _) = queue
        .get(last_run.last_task_id)
        .await
        .expect("Failed to fetch last_run task")
        .expect("Task from last_run should exist");
    assert_eq!(task.task_type, task_type);
    assert_eq!(task.input, schedule.input);

    manager
        .delete(&schedule_id)
        .await
        .expect("Failed to delete schedule");
}

/// Test that a scheduler submit failure does not update last_run.
#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_scheduler_tick_submit_failure_does_not_update_last_run() {
    let queue = test_queue().await;
    let manager = ScheduleManager::new(queue.client().clone());
    let schedule_id = unique_schedule_id();
    let task_type = unique_task_type();

    let schedule = Schedule::new(
        &schedule_id,
        &task_type,
        json!({"value": 1}),
        "* * * * *", // Every minute
    );

    manager
        .create(&schedule)
        .await
        .expect("Failed to create schedule");

    let due_at = schedule
        .next_run_after(DateTime::from_timestamp(0, 0).unwrap())
        .expect("Expected a next run time");
    let idempotency_key = format!("schedule:{}:{}", schedule.id, due_at.timestamp());

    queue
        .submit(
            &task_type,
            json!({"value": 2}),
            SubmitOptions {
                idempotency_key: Some(idempotency_key),
                ..Default::default()
            },
        )
        .await
        .expect("Failed to create conflicting task");

    let now = queue
        .now()
        .await
        .expect("Failed to fetch authoritative queue time");
    let report = run_scheduler_tick(&queue, &manager, now)
        .await
        .expect("Failed to run scheduler tick");

    assert!(
        report
            .errors
            .iter()
            .any(|err| err.schedule_id == schedule_id),
        "Expected scheduler tick error for the schedule due to idempotency conflict"
    );

    let last_run = manager
        .get_last_run(&schedule_id)
        .await
        .expect("Failed to fetch last run");
    assert!(
        last_run.is_none(),
        "Last run should not be recorded on submit failure"
    );

    manager
        .delete(&schedule_id)
        .await
        .expect("Failed to delete schedule");
}
