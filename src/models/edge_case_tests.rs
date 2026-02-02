//! Edge case tests for the models module.
//!
//! These tests verify boundary conditions, edge cases, and potential bugs
//! in task state management, retry logic, and shard extraction.

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::field_reassign_with_default
)]
mod task_edge_cases {
    use crate::models::{Task, TaskStatus};
    use chrono::{Duration, TimeZone, Utc};
    use uuid::Uuid;

    // =========================================================================
    // Lease Expiration Boundary Tests
    // These catch off-by-one errors in comparison operators (< vs <=)
    // =========================================================================

    /// Test: Lease expires EXACTLY at the boundary (now == `lease_expires_at`)
    /// Bug this catches: Using `<` instead of `<=` or vice versa for expiration check.
    /// The implementation uses `now > expires_at`, so exactly-at-boundary should NOT be expired.
    #[test]
    fn test_lease_expires_exactly_at_boundary_not_expired() {
        let mut task = Task::default();
        let now = Utc::now();
        task.lease_expires_at = Some(now);

        // When now == lease_expires_at, the lease should NOT be considered expired
        // because `is_lease_expired_at` uses `now > expires_at` (strict greater than)
        assert!(
            !task.is_lease_expired_at(now),
            "Lease exactly at boundary should NOT be expired (uses strict >)"
        );
    }

    /// Test: Lease expires 1 nanosecond before now
    /// Bug this catches: Precision issues in timestamp comparisons.
    #[test]
    fn test_lease_expired_by_one_nanosecond() {
        let mut task = Task::default();
        let now = Utc::now();
        // Set lease to expire 1 nanosecond before now
        task.lease_expires_at = Some(now - Duration::nanoseconds(1));

        assert!(
            task.is_lease_expired_at(now),
            "Lease expired by 1ns should be considered expired"
        );
    }

    /// Test: Lease expires 1 nanosecond after now
    /// Bug this catches: Precision issues causing premature expiration.
    #[test]
    fn test_lease_not_expired_by_one_nanosecond() {
        let mut task = Task::default();
        let now = Utc::now();
        // Set lease to expire 1 nanosecond after now
        task.lease_expires_at = Some(now + Duration::nanoseconds(1));

        assert!(
            !task.is_lease_expired_at(now),
            "Lease expiring in 1ns should NOT be expired yet"
        );
    }

    // =========================================================================
    // available_at Boundary Tests
    // =========================================================================

    /// Test: Task available EXACTLY at now
    /// Bug this catches: Off-by-one in availability check (should use >=)
    #[test]
    fn test_task_available_exactly_at_now() {
        let mut task = Task::default();
        let now = Utc::now();
        task.available_at = now;

        // When now == available_at, task should be available (uses >=)
        assert!(
            task.is_available_at(now),
            "Task with available_at == now should be available"
        );
    }

    /// Test: Task available 1 nanosecond in the past
    #[test]
    fn test_task_available_one_nanosecond_ago() {
        let mut task = Task::default();
        let now = Utc::now();
        task.available_at = now - Duration::nanoseconds(1);

        assert!(
            task.is_available_at(now),
            "Task available 1ns ago should be available"
        );
    }

    /// Test: Task available 1 nanosecond in the future
    #[test]
    fn test_task_not_available_one_nanosecond_future() {
        let mut task = Task::default();
        let now = Utc::now();
        task.available_at = now + Duration::nanoseconds(1);

        assert!(
            !task.is_available_at(now),
            "Task available in 1ns should NOT be available yet"
        );
    }

    // =========================================================================
    // Retry Count Edge Cases
    // =========================================================================

    /// Test: `retry_count` > `max_retries` (should not allow retry)
    /// Bug this catches: Logic error allowing retries when count exceeds max.
    #[test]
    fn test_retry_count_exceeds_max_retries() {
        let mut task = Task::default();
        task.max_retries = 3;
        task.retry_count = 4; // More than max

        assert!(
            !task.can_retry(),
            "Task with retry_count > max_retries should NOT be retryable"
        );
    }

    /// Test: `retry_count` == `max_retries` (boundary - should not allow retry)
    /// Bug this catches: Off-by-one error (using <= instead of <)
    #[test]
    fn test_retry_count_equals_max_retries() {
        let mut task = Task::default();
        task.max_retries = 3;
        task.retry_count = 3; // Exactly at max

        assert!(
            !task.can_retry(),
            "Task with retry_count == max_retries should NOT be retryable"
        );
    }

    /// Test: `retry_count` == `max_retries` - 1 (one under - should allow retry)
    #[test]
    fn test_retry_count_one_under_max() {
        let mut task = Task::default();
        task.max_retries = 3;
        task.retry_count = 2; // One under max

        assert!(
            task.can_retry(),
            "Task with retry_count == max_retries - 1 should be retryable"
        );
    }

    /// Test: `max_retries` = 0 (no retries allowed)
    /// Bug this catches: Division by zero or incorrect handling of zero retries.
    #[test]
    fn test_zero_max_retries() {
        let mut task = Task::default();
        task.max_retries = 0;
        task.retry_count = 0;

        assert!(
            !task.can_retry(),
            "Task with max_retries = 0 should never be retryable"
        );
    }

    /// Test: Large retry values (`u32::MAX`)
    /// Bug this catches: Overflow issues with large retry counts.
    #[test]
    fn test_very_large_retry_values() {
        let mut task = Task::default();
        task.max_retries = u32::MAX;
        task.retry_count = u32::MAX - 1;

        assert!(
            task.can_retry(),
            "Should handle large retry values correctly"
        );

        task.retry_count = u32::MAX;
        assert!(
            !task.can_retry(),
            "retry_count at u32::MAX with max_retries at u32::MAX should NOT be retryable"
        );
    }

    // =========================================================================
    // Shard Extraction Edge Cases
    // =========================================================================

    /// Test: All 16 possible shard values (0-f)
    /// Bug this catches: Missing shard values or incorrect hex conversion.
    #[test]
    fn test_all_shard_values() {
        let expected_shards = [
            "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f",
        ];

        for expected in &expected_shards {
            // Create a UUID that starts with each hex digit
            let uuid_str = format!("{expected}1234567-89ab-cdef-0123-456789abcdef");
            let id = Uuid::parse_str(&uuid_str).expect("valid UUID");
            let shard = Task::shard_from_id(&id);
            assert_eq!(
                &shard, *expected,
                "UUID starting with {expected} should have shard {expected}"
            );
        }
    }

    /// Test: Uppercase UUID string handling
    /// Bug this catches: Case sensitivity issues in UUID to shard conversion.
    #[test]
    fn test_shard_from_uppercase_uuid() {
        // UUID.to_string() always returns lowercase, but let's verify
        let id = Uuid::parse_str("A1234567-89AB-CDEF-0123-456789ABCDEF").expect("valid UUID");
        let shard = Task::shard_from_id(&id);
        // UUID::to_string() normalizes to lowercase
        assert_eq!(
            shard, "a",
            "Shard should be lowercase regardless of input case"
        );
    }

    /// Test: Nil UUID (all zeros)
    /// Bug this catches: Special case handling for nil UUID.
    #[test]
    fn test_shard_from_nil_uuid() {
        let id = Uuid::nil();
        let shard = Task::shard_from_id(&id);
        assert_eq!(shard, "0", "Nil UUID should have shard '0'");
    }

    /// Test: Max UUID (all ones)
    /// Bug this catches: Special case handling for max UUID.
    #[test]
    fn test_shard_from_max_uuid() {
        let id = Uuid::max();
        let shard = Task::shard_from_id(&id);
        assert_eq!(shard, "f", "Max UUID should have shard 'f'");
    }

    // =========================================================================
    // Epoch Minute Bucket Edge Cases
    // =========================================================================

    /// Test: Epoch time (1970-01-01 00:00:00)
    /// Bug this catches: Incorrect handling of timestamp = 0.
    #[test]
    fn test_epoch_minute_bucket_at_epoch() {
        let epoch = Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap();
        let bucket = Task::epoch_minute_bucket_value(epoch);
        assert_eq!(bucket, 0, "Epoch time should have bucket value 0");

        let bucket_str = Task::epoch_minute_bucket_string(epoch);
        assert_eq!(
            bucket_str, "0000000000",
            "Bucket string should be zero-padded to 10 digits"
        );
    }

    /// Test: Negative timestamp (before epoch)
    /// Bug this catches: Incorrect handling of pre-epoch timestamps.
    #[test]
    fn test_epoch_minute_bucket_before_epoch() {
        let before_epoch = Utc.with_ymd_and_hms(1969, 12, 31, 23, 59, 0).unwrap();
        let bucket = Task::epoch_minute_bucket_value(before_epoch);
        // One minute before epoch should be -1
        assert_eq!(
            bucket, -1,
            "One minute before epoch should have bucket value -1"
        );
    }

    /// Test: Far future timestamp
    /// Bug this catches: Overflow in bucket calculation.
    #[test]
    fn test_epoch_minute_bucket_far_future() {
        let far_future = Utc.with_ymd_and_hms(2100, 1, 1, 0, 0, 0).unwrap();
        let bucket = Task::epoch_minute_bucket_value(far_future);
        // Should not panic and should return a reasonable value
        assert!(
            bucket > 0,
            "Far future timestamp should have positive bucket"
        );

        let bucket_str = Task::epoch_minute_bucket_string(far_future);
        // Verify it's a valid string (doesn't panic)
        assert!(!bucket_str.is_empty(), "Bucket string should not be empty");
    }

    /// Test: Boundary between minutes (59 seconds vs 60 seconds)
    /// Bug this catches: Off-by-one in minute calculation.
    #[test]
    fn test_epoch_minute_bucket_minute_boundary() {
        let at_59_seconds = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 59).unwrap();
        let at_0_seconds = Utc.with_ymd_and_hms(2024, 1, 15, 10, 31, 0).unwrap();

        let bucket_59 = Task::epoch_minute_bucket_value(at_59_seconds);
        let bucket_0 = Task::epoch_minute_bucket_value(at_0_seconds);

        assert_eq!(
            bucket_0,
            bucket_59 + 1,
            "Bucket should increment at the minute boundary"
        );
    }

    // =========================================================================
    // Task Key Generation Edge Cases
    // =========================================================================

    /// Test: Task key format is consistent
    /// Bug this catches: Inconsistent key format leading to storage issues.
    #[test]
    fn test_task_key_format_consistency() {
        let id = Uuid::parse_str("a1234567-89ab-cdef-0123-456789abcdef").unwrap();
        let mut task = Task::default();
        task.id = id;
        task.shard = Task::shard_from_id(&id);

        let key = task.key();
        assert_eq!(key, "tasks/a/a1234567-89ab-cdef-0123-456789abcdef.json");
        assert!(key.starts_with("tasks/"));
        assert!(key.ends_with(".json"));
        assert!(key.contains(&task.shard));
        assert!(key.contains(&task.id.to_string()));
    }

    // =========================================================================
    // Task Serialization Edge Cases
    // =========================================================================

    /// Test: Roundtrip with all optional fields None
    /// Bug this catches: Serialization issues with optional fields.
    #[test]
    fn test_task_serialization_minimal() {
        let task = Task::default();
        let json = serde_json::to_string(&task).expect("serialize should succeed");
        let deserialized: Task = serde_json::from_str(&json).expect("deserialize should succeed");

        assert_eq!(task.id, deserialized.id);
        assert_eq!(task.status, deserialized.status);
        assert!(deserialized.output.is_none());
        assert!(deserialized.lease_expires_at.is_none());
        assert!(deserialized.lease_id.is_none());
        assert!(deserialized.worker_id.is_none());
        assert!(deserialized.last_error.is_none());
    }

    /// Test: Roundtrip with all optional fields populated
    /// Bug this catches: Serialization issues with populated optional fields.
    #[test]
    fn test_task_serialization_maximal() {
        let mut task = Task::default();
        task.output = Some(serde_json::json!({"result": "success"}));
        task.lease_expires_at = Some(Utc::now());
        task.lease_id = Some(Uuid::new_v4());
        task.worker_id = Some("worker-1".to_string());
        task.last_error = Some("previous error".to_string());
        task.completed_at = Some(Utc::now());
        task.status = TaskStatus::Completed;

        let json = serde_json::to_string(&task).expect("serialize should succeed");
        let deserialized: Task = serde_json::from_str(&json).expect("deserialize should succeed");

        assert_eq!(task.id, deserialized.id);
        assert_eq!(task.status, deserialized.status);
        assert!(deserialized.output.is_some());
        assert!(deserialized.lease_expires_at.is_some());
        assert!(deserialized.lease_id.is_some());
        assert!(deserialized.worker_id.is_some());
        assert!(deserialized.last_error.is_some());
        assert!(deserialized.completed_at.is_some());
    }

    /// Test: Unicode in task fields
    /// Bug this catches: Encoding issues with non-ASCII characters.
    #[test]
    fn test_task_unicode_content() {
        let mut task = Task::default();
        task.task_type = "send_email_\u{1F4E7}".to_string(); // envelope emoji
        task.input = serde_json::json!({"message": "Hello \u{4E16}\u{754C}"}); // "Hello World" in Chinese
        task.last_error = Some("\u{26A0} Warning: \u{00E9}chec".to_string()); // warning sign, French

        let json = serde_json::to_string(&task).expect("serialize with unicode should succeed");
        let deserialized: Task =
            serde_json::from_str(&json).expect("deserialize with unicode should succeed");

        assert_eq!(task.task_type, deserialized.task_type);
        assert_eq!(task.input, deserialized.input);
        assert_eq!(task.last_error, deserialized.last_error);
    }

    /// Test: Very large input JSON
    /// Bug this catches: Memory issues or truncation with large payloads.
    #[test]
    fn test_task_large_input() {
        let large_string = "x".repeat(100_000);
        let task = Task::new("large_task", serde_json::json!({"data": large_string}));

        let json = serde_json::to_string(&task).expect("serialize large input should succeed");
        let deserialized: Task =
            serde_json::from_str(&json).expect("deserialize large input should succeed");

        let original_data = task.input["data"].as_str().unwrap();
        let deser_data = deserialized.input["data"].as_str().unwrap();
        assert_eq!(original_data.len(), deser_data.len());
        assert_eq!(original_data, deser_data);
    }

    /// Test: Deeply nested input JSON
    /// Bug this catches: Stack overflow or recursion limits.
    #[test]
    fn test_task_deeply_nested_input() {
        // Create a deeply nested JSON structure (100 levels)
        let mut nested = serde_json::json!({"value": "leaf"});
        for _ in 0..100 {
            nested = serde_json::json!({"nested": nested});
        }

        let task = Task::new("nested_task", nested);

        let json = serde_json::to_string(&task).expect("serialize nested input should succeed");
        let deserialized: Task =
            serde_json::from_str(&json).expect("deserialize nested input should succeed");

        // Verify the structure is preserved (check a few levels deep)
        assert!(deserialized.input.get("nested").is_some());
    }

    // =========================================================================
    // Timeout Seconds Edge Cases
    // =========================================================================

    /// Test: Zero timeout
    /// Bug this catches: Division by zero or immediate timeout issues.
    #[test]
    fn test_task_zero_timeout() {
        let mut task = Task::default();
        task.timeout_seconds = 0;

        // Should serialize without issues
        let json = serde_json::to_string(&task).expect("serialize should succeed");
        let deserialized: Task = serde_json::from_str(&json).expect("deserialize should succeed");
        assert_eq!(deserialized.timeout_seconds, 0);
    }

    /// Test: Very large timeout (`u64::MAX`)
    /// Bug this catches: Overflow when calculating `lease_expires_at`.
    #[test]
    fn test_task_max_timeout() {
        let mut task = Task::default();
        task.timeout_seconds = u64::MAX;

        let json = serde_json::to_string(&task).expect("serialize should succeed");
        let deserialized: Task = serde_json::from_str(&json).expect("deserialize should succeed");
        assert_eq!(deserialized.timeout_seconds, u64::MAX);
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::manual_range_contains,
    clippy::float_cmp,
    clippy::cast_precision_loss
)]
mod retry_policy_edge_cases {
    use crate::models::RetryPolicy;
    use std::time::Duration;

    // =========================================================================
    // Backoff Calculation Edge Cases
    // =========================================================================

    /// Test: Very high retry count (potential overflow)
    /// Bug this catches: Integer overflow in exponential calculation (2^31+).
    #[test]
    fn test_backoff_very_high_retry_count() {
        let policy = RetryPolicy::default();

        // Attempt count that would overflow if not capped (2^100)
        let delay = policy.calculate_delay_without_jitter(100);

        // Should be capped at max_interval_ms (60000ms)
        assert_eq!(
            delay,
            Duration::from_millis(60000),
            "Very high retry count should be capped at max_interval_ms"
        );
    }

    /// Test: `u32::MAX` retry count
    /// Bug this catches: Overflow or panic with maximum retry count.
    #[test]
    fn test_backoff_u32_max_retry_count() {
        let policy = RetryPolicy::default();

        // Should not panic
        let delay = policy.calculate_delay_without_jitter(u32::MAX);

        // Should be capped at max_interval_ms
        assert_eq!(
            delay,
            Duration::from_millis(60000),
            "u32::MAX retry count should be capped at max_interval_ms"
        );
    }

    /// Test: Attempt 30 (2^30 = 1 billion, near `MAX_EXP` limit)
    /// Bug this catches: Boundary issues at the `MAX_EXP` constant.
    #[test]
    fn test_backoff_at_max_exp_boundary() {
        let policy = RetryPolicy::new(1, 60000, 2.0, 0.0);

        // Attempt 30: 1 * 2^30 = 1,073,741,824 ms, capped at 60000
        let delay = policy.calculate_delay_without_jitter(30);
        assert_eq!(delay, Duration::from_millis(60000));

        // Attempt 31: should still work (capped exponent)
        let delay = policy.calculate_delay_without_jitter(31);
        assert_eq!(delay, Duration::from_millis(60000));
    }

    // =========================================================================
    // Jitter Edge Cases
    // =========================================================================

    /// Test: 0% jitter produces deterministic results
    /// Bug this catches: Jitter applied incorrectly when `jitter_percent` is 0.
    #[test]
    fn test_zero_jitter_is_deterministic() {
        let policy = RetryPolicy::new(1000, 60000, 2.0, 0.0);

        // Run multiple times - should always get the same result
        let expected = Duration::from_millis(1000);
        for _ in 0..10 {
            let delay = policy.calculate_delay(0);
            assert_eq!(delay, expected, "0% jitter should be deterministic");
        }
    }

    /// Test: 100% jitter stays within bounds [0, 2x]
    /// Bug this catches: Jitter exceeding expected bounds.
    #[test]
    fn test_full_jitter_bounds() {
        let policy = RetryPolicy::new(1000, 60000, 2.0, 1.0); // 100% jitter

        for _ in 0..100 {
            let delay = policy.calculate_delay(0);
            let delay_ms = delay.as_millis() as f64;

            // With 100% jitter, range should be [0, 2000]
            assert!(
                delay_ms >= 0.0 && delay_ms <= 2000.0,
                "100% jitter delay {delay_ms} should be in [0, 2000]"
            );
        }
    }

    /// Test: Negative jitter percentage is clamped to 0.
    /// Bug this catches: Panic with invalid jitter (was a bug, now fixed).
    #[test]
    fn test_negative_jitter_percent_clamped_to_zero() {
        let policy = RetryPolicy::new(1000, 60000, 2.0, -0.5);

        // Negative jitter is clamped to 0.0, so delay is deterministic
        assert_eq!(policy.jitter_percent, 0.0);
        for _ in 0..10 {
            let delay = policy.calculate_delay(0);
            assert_eq!(delay.as_millis(), 1000);
        }
    }

    /// Test: Jitter > 1.0 is clamped to 1.0 (100%)
    /// Bug this catches: Unexpected behavior with jitter > 100%.
    #[test]
    fn test_jitter_greater_than_one_clamped() {
        let policy = RetryPolicy::new(1000, 60000, 2.0, 2.0); // Would be 200%, clamped to 100%

        assert_eq!(policy.jitter_percent, 1.0);
        for _ in 0..100 {
            let delay = policy.calculate_delay(0);
            // With 100% jitter, delay ranges from 0ms to 2000ms
            assert!(delay.as_millis() <= 2000, "Delay should be <= 2000ms");
        }
    }

    // =========================================================================
    // Initial Interval Edge Cases
    // =========================================================================

    /// Test: Zero initial interval
    /// Bug this catches: Division by zero or stuck at zero delay.
    #[test]
    fn test_zero_initial_interval() {
        let policy = RetryPolicy::new(0, 60000, 2.0, 0.0);

        let delay = policy.calculate_delay_without_jitter(0);
        assert_eq!(
            delay,
            Duration::from_millis(0),
            "Zero initial interval should give zero delay"
        );

        // Even with retries, 0 * 2^n = 0
        let delay = policy.calculate_delay_without_jitter(5);
        assert_eq!(delay, Duration::from_millis(0), "Zero * anything = zero");
    }

    /// Test: Very small initial interval (1ms)
    /// Bug this catches: Precision loss with very small values.
    #[test]
    fn test_minimum_initial_interval() {
        let policy = RetryPolicy::new(1, 60000, 2.0, 0.0);

        // Attempt 0: 1ms
        assert_eq!(
            policy.calculate_delay_without_jitter(0),
            Duration::from_millis(1)
        );
        // Attempt 1: 2ms
        assert_eq!(
            policy.calculate_delay_without_jitter(1),
            Duration::from_millis(2)
        );
        // Attempt 10: 1024ms
        assert_eq!(
            policy.calculate_delay_without_jitter(10),
            Duration::from_millis(1024)
        );
    }

    /// Test: `u32::MAX` initial interval
    /// Bug this catches: Overflow with large initial values.
    #[test]
    fn test_max_initial_interval() {
        let policy = RetryPolicy::new(u32::MAX, u32::MAX, 2.0, 0.0);

        // Even attempt 0 should be capped at max_interval_ms
        let delay = policy.calculate_delay_without_jitter(0);
        assert_eq!(delay, Duration::from_millis(u64::from(u32::MAX)));
    }

    // =========================================================================
    // Multiplier Edge Cases
    // =========================================================================

    /// Test: Multiplier of 1.0 (linear, no growth)
    /// Bug this catches: Division or special case handling for multiplier = 1.
    #[test]
    fn test_multiplier_one() {
        let policy = RetryPolicy::new(1000, 60000, 1.0, 0.0);

        // All attempts should give the same delay
        assert_eq!(
            policy.calculate_delay_without_jitter(0),
            Duration::from_millis(1000)
        );
        assert_eq!(
            policy.calculate_delay_without_jitter(1),
            Duration::from_millis(1000)
        );
        assert_eq!(
            policy.calculate_delay_without_jitter(10),
            Duration::from_millis(1000)
        );
    }

    /// Test: Multiplier less than 1 (decay instead of growth)
    /// Bug this catches: Unexpected behavior with decay multipliers.
    #[test]
    fn test_multiplier_less_than_one() {
        let policy = RetryPolicy::new(1000, 60000, 0.5, 0.0);

        // Attempt 0: 1000ms
        assert_eq!(
            policy.calculate_delay_without_jitter(0),
            Duration::from_millis(1000)
        );
        // Attempt 1: 500ms
        assert_eq!(
            policy.calculate_delay_without_jitter(1),
            Duration::from_millis(500)
        );
        // Attempt 2: 250ms
        assert_eq!(
            policy.calculate_delay_without_jitter(2),
            Duration::from_millis(250)
        );
    }

    /// Test: Zero multiplier
    /// Bug this catches: Division by zero or stuck at zero after first attempt.
    #[test]
    fn test_zero_multiplier() {
        let policy = RetryPolicy::new(1000, 60000, 0.0, 0.0);

        // Attempt 0: 1000 * 0^0 = 1000 * 1 = 1000ms (0^0 = 1 in floating point)
        let delay0 = policy.calculate_delay_without_jitter(0);
        assert_eq!(delay0, Duration::from_millis(1000));

        // Attempt 1: 1000 * 0^1 = 0ms
        let delay1 = policy.calculate_delay_without_jitter(1);
        assert_eq!(delay1, Duration::from_millis(0));
    }

    /// Test: Very large multiplier
    /// Bug this catches: Overflow with large multipliers.
    #[test]
    fn test_large_multiplier() {
        let policy = RetryPolicy::new(1000, 60000, 10.0, 0.0);

        // Should quickly hit the max cap
        let delay = policy.calculate_delay_without_jitter(5);
        assert_eq!(
            delay,
            Duration::from_millis(60000),
            "Large multiplier should be capped"
        );
    }

    /// Test: Negative multiplier
    /// Bug this catches: Undefined behavior with negative multipliers.
    #[test]
    fn test_negative_multiplier() {
        let policy = RetryPolicy::new(1000, 60000, -2.0, 0.0);

        // Negative numbers raised to integer powers can be positive or negative
        // (-2)^0 = 1, (-2)^1 = -2, (-2)^2 = 4, etc.
        // The implementation should handle this gracefully
        let delay = policy.calculate_delay_without_jitter(0);
        // 1000 * (-2)^0 = 1000 * 1 = 1000
        assert_eq!(delay, Duration::from_millis(1000));
    }

    /// Test: Infinity and NaN handling
    /// Bug this catches: Crash or undefined behavior with special float values.
    #[test]
    fn test_special_float_values() {
        // These should not panic
        let policy_inf = RetryPolicy::new(1000, 60000, f64::INFINITY, 0.0);
        let _delay = policy_inf.calculate_delay_without_jitter(1);

        let policy_nan = RetryPolicy::new(1000, 60000, f64::NAN, 0.0);
        let _delay = policy_nan.calculate_delay_without_jitter(1);
    }

    // =========================================================================
    // Max Interval Edge Cases
    // =========================================================================

    /// Test: `max_interval` less than `initial_interval`
    /// Bug this catches: Incorrect capping when max < initial.
    #[test]
    fn test_max_less_than_initial() {
        let policy = RetryPolicy::new(10000, 5000, 2.0, 0.0);

        // Even attempt 0 should be capped at max
        let delay = policy.calculate_delay_without_jitter(0);
        assert_eq!(
            delay,
            Duration::from_millis(5000),
            "Should be capped at max even on first attempt"
        );
    }

    /// Test: Zero max interval
    /// Bug this catches: Always returning zero delay.
    #[test]
    fn test_zero_max_interval() {
        let policy = RetryPolicy::new(1000, 0, 2.0, 0.0);

        let delay = policy.calculate_delay_without_jitter(0);
        assert_eq!(
            delay,
            Duration::from_millis(0),
            "Zero max interval should cap everything to zero"
        );
    }

    // =========================================================================
    // Serialization Edge Cases
    // =========================================================================

    /// Test: Serialization roundtrip preserves precision
    /// Bug this catches: Floating point precision loss in serialization.
    #[test]
    fn test_retry_policy_serialization_precision() {
        let policy = RetryPolicy::new(1000, 60000, 1.999_999_999_999_9, 0.123_456_789_012_3);

        let json = serde_json::to_string(&policy).expect("serialize should succeed");
        let deserialized: RetryPolicy =
            serde_json::from_str(&json).expect("deserialize should succeed");

        assert_eq!(policy.initial_interval_ms, deserialized.initial_interval_ms);
        assert_eq!(policy.max_interval_ms, deserialized.max_interval_ms);
        // Floating point comparison within epsilon
        assert!((policy.multiplier - deserialized.multiplier).abs() < 1e-10);
        assert!((policy.jitter_percent - deserialized.jitter_percent).abs() < 1e-10);
    }
}
