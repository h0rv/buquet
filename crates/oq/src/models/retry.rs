use rand::Rng;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Retry policy configuration for exponential backoff with jitter.
///
/// Uses u32 for millisecond values which fit exactly in f64 (max ~49 days).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RetryPolicy {
    /// Initial delay between retries in milliseconds.
    pub initial_interval_ms: u32,
    /// Maximum delay between retries in milliseconds.
    pub max_interval_ms: u32,
    /// Multiplier for exponential backoff (must be positive).
    pub multiplier: f64,
    /// Jitter percentage (0.0 to 1.0). E.g., 0.25 means +/-25% randomness.
    pub jitter_percent: f64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            initial_interval_ms: 1000, // 1 second
            max_interval_ms: 60000,    // 60 seconds
            multiplier: 2.0,
            jitter_percent: 0.25,
        }
    }
}

impl RetryPolicy {
    /// Creates a new `RetryPolicy` with the specified parameters.
    ///
    /// Note: `jitter_percent` is clamped to `[0.0, 1.0]` range.
    #[must_use]
    pub const fn new(
        initial_interval_ms: u32,
        max_interval_ms: u32,
        multiplier: f64,
        jitter_percent: f64,
    ) -> Self {
        Self {
            initial_interval_ms,
            max_interval_ms,
            multiplier,
            // Clamp to valid range: negative makes no sense, >1.0 could cause negative delays
            jitter_percent: jitter_percent.clamp(0.0, 1.0),
        }
    }

    /// Calculates the delay for a given retry attempt using exponential backoff with jitter.
    ///
    /// # Arguments
    /// * `attempt` - The retry attempt number (0-indexed, so first retry is attempt 0)
    ///
    /// # Returns
    /// The duration to wait before the next retry attempt.
    #[must_use]
    pub fn calculate_delay(&self, attempt: u32) -> Duration {
        let base_ms = self.compute_base_delay_ms(attempt);
        let capped_ms = base_ms.min(f64::from(self.max_interval_ms));

        // Apply jitter: random value in range [1 - jitter, 1 + jitter]
        // Use abs() to handle negative jitter_percent gracefully
        let jitter = self.jitter_percent.abs();
        let jitter_factor = if jitter == 0.0 {
            1.0
        } else {
            let mut rng = rand::thread_rng();
            1.0 + rng.gen_range(-jitter..=jitter)
        };
        let final_ms = (capped_ms * jitter_factor).max(0.0);

        Duration::from_millis(f64_to_u64_saturating(final_ms))
    }

    /// Calculates the delay without jitter (for testing deterministic behavior).
    #[must_use]
    pub fn calculate_delay_without_jitter(&self, attempt: u32) -> Duration {
        let base_ms = self.compute_base_delay_ms(attempt);
        let capped_ms = base_ms.min(f64::from(self.max_interval_ms));
        Duration::from_millis(f64_to_u64_saturating(capped_ms))
    }

    /// Computes base delay with exponential backoff.
    fn compute_base_delay_ms(&self, attempt: u32) -> f64 {
        // Cap exponent to reasonable value (30 attempts with multiplier 2 = 2^30 = 1 billion)
        // This avoids any overflow concerns and is well within i32 range
        const MAX_EXP: i32 = 30;
        let exp = i32::try_from(attempt).map_or(MAX_EXP, |e| e.min(MAX_EXP));
        f64::from(self.initial_interval_ms) * self.multiplier.powi(exp)
    }
}

/// Converts f64 milliseconds to u64 with saturation.
#[inline]
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn f64_to_u64_saturating(val: f64) -> u64 {
    if !val.is_finite() || val < 0.0 {
        0
    } else if val >= f64::from(u32::MAX) {
        u64::from(u32::MAX)
    } else {
        // SAFETY: We've verified val >= 0.0 (no sign loss) and val < u32::MAX (no truncation beyond u64)
        val as u64
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_default_values() {
        let policy = RetryPolicy::default();
        assert_eq!(policy.initial_interval_ms, 1000);
        assert_eq!(policy.max_interval_ms, 60000);
        assert_eq!(policy.multiplier, 2.0);
        assert_eq!(policy.jitter_percent, 0.25);
    }

    #[test]
    fn test_exponential_growth_without_jitter() {
        let policy = RetryPolicy::default();

        // Attempt 0: 1000ms
        assert_eq!(
            policy.calculate_delay_without_jitter(0),
            Duration::from_millis(1000)
        );

        // Attempt 1: 1000 * 2 = 2000ms
        assert_eq!(
            policy.calculate_delay_without_jitter(1),
            Duration::from_millis(2000)
        );

        // Attempt 2: 1000 * 4 = 4000ms
        assert_eq!(
            policy.calculate_delay_without_jitter(2),
            Duration::from_millis(4000)
        );

        // Attempt 3: 1000 * 8 = 8000ms
        assert_eq!(
            policy.calculate_delay_without_jitter(3),
            Duration::from_millis(8000)
        );

        // Attempt 4: 1000 * 16 = 16000ms
        assert_eq!(
            policy.calculate_delay_without_jitter(4),
            Duration::from_millis(16000)
        );

        // Attempt 5: 1000 * 32 = 32000ms
        assert_eq!(
            policy.calculate_delay_without_jitter(5),
            Duration::from_millis(32000)
        );
    }

    #[test]
    fn test_max_interval_cap() {
        let policy = RetryPolicy::default();

        // Attempt 6: 1000 * 64 = 64000ms, but capped at 60000ms
        assert_eq!(
            policy.calculate_delay_without_jitter(6),
            Duration::from_millis(60000)
        );

        // Attempt 10: should still be capped at 60000ms
        assert_eq!(
            policy.calculate_delay_without_jitter(10),
            Duration::from_millis(60000)
        );
    }

    #[test]
    fn test_jitter_bounds() {
        let policy = RetryPolicy::default();
        let base_delay_ms = 1000.0; // First attempt base delay

        // Run multiple iterations to test jitter bounds
        for _ in 0..100 {
            let delay = policy.calculate_delay(0);
            let delay_ms = delay.as_millis() as f64;

            // With 25% jitter, delay should be in range [750, 1250]
            let min_expected = base_delay_ms * (1.0 - policy.jitter_percent);
            let max_expected = base_delay_ms * (1.0 + policy.jitter_percent);

            assert!(
                delay_ms >= min_expected && delay_ms <= max_expected,
                "Delay {} should be in range [{}, {}]",
                delay_ms,
                min_expected,
                max_expected
            );
        }
    }

    #[test]
    fn test_jitter_bounds_at_max_cap() {
        let policy = RetryPolicy::default();

        // At attempt 10, base delay would be far above max, so capped at 60000ms
        let base_delay_ms = 60000.0;

        for _ in 0..100 {
            let delay = policy.calculate_delay(10);
            let delay_ms = delay.as_millis() as f64;

            // With 25% jitter on 60000ms, delay should be in range [45000, 75000]
            let min_expected = base_delay_ms * (1.0 - policy.jitter_percent);
            let max_expected = base_delay_ms * (1.0 + policy.jitter_percent);

            assert!(
                delay_ms >= min_expected && delay_ms <= max_expected,
                "Delay {} should be in range [{}, {}]",
                delay_ms,
                min_expected,
                max_expected
            );
        }
    }

    #[test]
    fn test_custom_policy() {
        let policy = RetryPolicy::new(500, 10000, 3.0, 0.1);

        // Attempt 0: 500ms
        assert_eq!(
            policy.calculate_delay_without_jitter(0),
            Duration::from_millis(500)
        );

        // Attempt 1: 500 * 3 = 1500ms
        assert_eq!(
            policy.calculate_delay_without_jitter(1),
            Duration::from_millis(1500)
        );

        // Attempt 2: 500 * 9 = 4500ms
        assert_eq!(
            policy.calculate_delay_without_jitter(2),
            Duration::from_millis(4500)
        );

        // Attempt 3: 500 * 27 = 13500ms, capped at 10000ms
        assert_eq!(
            policy.calculate_delay_without_jitter(3),
            Duration::from_millis(10000)
        );
    }

    #[test]
    fn test_serialization() {
        let policy = RetryPolicy::default();
        let json = serde_json::to_string(&policy).unwrap();
        let deserialized: RetryPolicy = serde_json::from_str(&json).unwrap();
        assert_eq!(policy, deserialized);
    }

    #[test]
    fn test_zero_jitter() {
        let policy = RetryPolicy::new(1000, 60000, 2.0, 0.0);

        // With zero jitter, delay should be exactly the base delay
        for _ in 0..10 {
            let delay = policy.calculate_delay(0);
            assert_eq!(delay, Duration::from_millis(1000));
        }
    }
}
