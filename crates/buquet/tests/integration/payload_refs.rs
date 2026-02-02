//! Integration tests for payload references feature.
//!
//! Tests that large payloads are offloaded to separate S3 objects
//! and can be loaded back transparently.

use buquet::queue::SubmitOptions;
use serde_json::json;

use crate::common::{test_queue, unique_task_type};

/// Test that small payloads stay inline when payload refs are enabled.
#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_small_payload_stays_inline() {
    let queue = test_queue().await;
    let task_type = unique_task_type();

    let small_input = json!({"key": "value"});

    let options = SubmitOptions {
        use_payload_refs: true,
        payload_ref_threshold_bytes: Some(1024), // 1KB threshold
        ..Default::default()
    };

    let task = queue
        .submit(&task_type, small_input.clone(), options)
        .await
        .expect("submit should succeed");

    // Small payload should stay inline
    assert!(
        task.input_ref.is_none(),
        "small payload should not have input_ref"
    );
    assert_eq!(task.input, small_input);

    // load_task_input should return the same data
    let loaded_input = queue
        .load_task_input(&task)
        .await
        .expect("load_task_input should succeed");
    assert_eq!(loaded_input, small_input);
}

/// Test that large payloads are offloaded when payload refs are enabled.
#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_large_payload_offloaded() {
    let queue = test_queue().await;
    let task_type = unique_task_type();

    // Create a payload larger than threshold
    let large_string = "x".repeat(2000);
    let large_input = json!({"data": large_string});

    let options = SubmitOptions {
        use_payload_refs: true,
        payload_ref_threshold_bytes: Some(1024), // 1KB threshold
        ..Default::default()
    };

    let task = queue
        .submit(&task_type, large_input.clone(), options)
        .await
        .expect("submit should succeed");

    // Large payload should be offloaded
    assert!(
        task.input_ref.is_some(),
        "large payload should have input_ref"
    );
    assert!(
        task.input_ref.as_ref().unwrap().starts_with("payloads/"),
        "input_ref should be a payloads/ path"
    );
    assert!(
        task.input_ref.as_ref().unwrap().ends_with("/input.json"),
        "input_ref should end with /input.json"
    );
    assert_eq!(
        task.input,
        serde_json::Value::Null,
        "inline input should be null"
    );

    // load_task_input should return the full data
    let loaded_input = queue
        .load_task_input(&task)
        .await
        .expect("load_task_input should succeed");
    assert_eq!(loaded_input, large_input);
}

/// Test that payload refs are disabled by default.
#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_payload_refs_disabled_by_default() {
    let queue = test_queue().await;
    let task_type = unique_task_type();

    // Create a large payload
    let large_string = "x".repeat(500_000); // 500KB
    let large_input = json!({"data": large_string});

    // Default options - payload refs should be disabled
    let options = SubmitOptions::default();

    let task = queue
        .submit(&task_type, large_input.clone(), options)
        .await
        .expect("submit should succeed");

    // Without payload refs, the input should stay inline
    assert!(
        task.input_ref.is_none(),
        "input_ref should be None when payload refs disabled"
    );
    assert_eq!(task.input, large_input);
}

/// Test that task can be retrieved and input loaded after submission.
#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_get_task_with_payload_ref() {
    let queue = test_queue().await;
    let task_type = unique_task_type();

    let large_string = "y".repeat(2000);
    let large_input = json!({"data": large_string});

    let options = SubmitOptions {
        use_payload_refs: true,
        payload_ref_threshold_bytes: Some(1024),
        ..Default::default()
    };

    let submitted_task = queue
        .submit(&task_type, large_input.clone(), options)
        .await
        .expect("submit should succeed");

    // Get the task
    let (retrieved_task, _etag) = queue
        .get(submitted_task.id)
        .await
        .expect("get should succeed")
        .expect("task should exist");

    assert_eq!(retrieved_task.id, submitted_task.id);
    assert_eq!(retrieved_task.input_ref, submitted_task.input_ref);

    // Load input from retrieved task
    let loaded_input = queue
        .load_task_input(&retrieved_task)
        .await
        .expect("load_task_input should succeed");
    assert_eq!(loaded_input, large_input);
}

/// Test load_task_output with no output.
#[tokio::test]
#[cfg_attr(
    not(feature = "integration"),
    ignore = "requires S3 (enable feature integration)"
)]
async fn test_load_output_no_output() {
    let queue = test_queue().await;
    let task_type = unique_task_type();

    let task = queue
        .submit(
            &task_type,
            json!({"key": "value"}),
            SubmitOptions::default(),
        )
        .await
        .expect("submit should succeed");

    // Task has no output yet
    let loaded_output = queue
        .load_task_output(&task)
        .await
        .expect("load_task_output should succeed");
    assert!(loaded_output.is_none());
}
