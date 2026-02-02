//! Polling strategy for the worker loop.
//!
//! This module provides configurable polling strategies to balance
//! cost and latency:
//!
//! - `Fixed`: Constant interval polling (predictable latency, higher cost)
//! - `Adaptive`: Exponential backoff when idle (lower cost, variable latency)

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Polling strategy for the worker loop.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "strategy", rename_all = "snake_case")]
pub enum PollingStrategy {
    /// Fixed interval polling. Predictable latency, higher cost.
    Fixed {
        /// Poll interval in milliseconds.
        interval_ms: u64,
    },

    /// Adaptive polling with exponential backoff. Lower cost, variable latency.
    Adaptive {
        /// Minimum interval in milliseconds (used when tasks are available).
        min_interval_ms: u64,
        /// Maximum interval in milliseconds (used when queue is idle).
        max_interval_ms: u64,
        /// Backoff multiplier (typically 2.0).
        backoff_multiplier: f64,
    },
}

impl Default for PollingStrategy {
    fn default() -> Self {
        Self::Adaptive {
            min_interval_ms: 100,
            max_interval_ms: 5000,
            backoff_multiplier: 2.0,
        }
    }
}

impl PollingStrategy {
    /// Creates a fixed polling strategy.
    #[must_use]
    pub const fn fixed(interval_ms: u64) -> Self {
        Self::Fixed { interval_ms }
    }

    /// Creates an adaptive polling strategy with default backoff multiplier.
    #[must_use]
    pub const fn adaptive(min_interval_ms: u64, max_interval_ms: u64) -> Self {
        Self::Adaptive {
            min_interval_ms,
            max_interval_ms,
            backoff_multiplier: 2.0,
        }
    }

    /// Creates an adaptive polling strategy with custom backoff multiplier.
    #[must_use]
    pub const fn adaptive_with_backoff(
        min_interval_ms: u64,
        max_interval_ms: u64,
        backoff_multiplier: f64,
    ) -> Self {
        Self::Adaptive {
            min_interval_ms,
            max_interval_ms,
            backoff_multiplier,
        }
    }

    /// Returns the initial polling interval.
    #[must_use]
    pub const fn initial_interval(&self) -> Duration {
        match self {
            Self::Fixed { interval_ms } => Duration::from_millis(*interval_ms),
            Self::Adaptive {
                min_interval_ms, ..
            } => Duration::from_millis(*min_interval_ms),
        }
    }

    /// Calculates the next polling interval based on whether tasks were found.
    ///
    /// For fixed strategy, always returns the same interval.
    /// For adaptive strategy:
    /// - If tasks were found: resets to minimum interval
    /// - If no tasks: backs off by multiplier, capped at maximum
    #[must_use]
    pub fn next_interval(&self, current: Duration, found_tasks: bool) -> Duration {
        match self {
            Self::Fixed { interval_ms } => Duration::from_millis(*interval_ms),
            Self::Adaptive {
                min_interval_ms,
                max_interval_ms,
                backoff_multiplier,
            } => {
                if found_tasks {
                    Duration::from_millis(*min_interval_ms)
                } else {
                    #[allow(
                        clippy::cast_possible_truncation,
                        clippy::cast_sign_loss,
                        clippy::cast_precision_loss
                    )]
                    let next_ms = (current.as_millis() as f64 * backoff_multiplier) as u64;
                    Duration::from_millis(next_ms.min(*max_interval_ms))
                }
            }
        }
    }

    /// Applies jitter to an interval to prevent thundering herd.
    ///
    /// Adds random jitter of +/-10% to the interval.
    #[must_use]
    pub fn apply_jitter(&self, interval: Duration) -> Duration {
        let jitter_factor = rand::random::<f64>().mul_add(0.2, 0.9); // +/-10%
        Duration::from_secs_f64(interval.as_secs_f64() * jitter_factor)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_default_strategy() {
        let strategy = PollingStrategy::default();
        match strategy {
            PollingStrategy::Adaptive {
                min_interval_ms,
                max_interval_ms,
                backoff_multiplier,
            } => {
                assert_eq!(min_interval_ms, 100);
                assert_eq!(max_interval_ms, 5000);
                assert!((backoff_multiplier - 2.0).abs() < f64::EPSILON);
            }
            PollingStrategy::Fixed { .. } => panic!("Expected Adaptive strategy"),
        }
    }

    #[test]
    fn test_fixed_strategy() {
        let strategy = PollingStrategy::fixed(500);
        match strategy {
            PollingStrategy::Fixed { interval_ms } => {
                assert_eq!(interval_ms, 500);
            }
            PollingStrategy::Adaptive { .. } => panic!("Expected Fixed strategy"),
        }
    }

    #[test]
    fn test_adaptive_strategy() {
        let strategy = PollingStrategy::adaptive(100, 30000);
        match strategy {
            PollingStrategy::Adaptive {
                min_interval_ms,
                max_interval_ms,
                backoff_multiplier,
            } => {
                assert_eq!(min_interval_ms, 100);
                assert_eq!(max_interval_ms, 30000);
                assert!((backoff_multiplier - 2.0).abs() < f64::EPSILON);
            }
            PollingStrategy::Fixed { .. } => panic!("Expected Adaptive strategy"),
        }
    }

    #[test]
    fn test_adaptive_with_backoff() {
        let strategy = PollingStrategy::adaptive_with_backoff(100, 30000, 1.5);
        match strategy {
            PollingStrategy::Adaptive {
                min_interval_ms,
                max_interval_ms,
                backoff_multiplier,
            } => {
                assert_eq!(min_interval_ms, 100);
                assert_eq!(max_interval_ms, 30000);
                assert!((backoff_multiplier - 1.5).abs() < f64::EPSILON);
            }
            PollingStrategy::Fixed { .. } => panic!("Expected Adaptive strategy"),
        }
    }

    #[test]
    fn test_initial_interval_fixed() {
        let strategy = PollingStrategy::fixed(500);
        assert_eq!(strategy.initial_interval(), Duration::from_millis(500));
    }

    #[test]
    fn test_initial_interval_adaptive() {
        let strategy = PollingStrategy::adaptive(100, 5000);
        assert_eq!(strategy.initial_interval(), Duration::from_millis(100));
    }

    #[test]
    fn test_next_interval_fixed_ignores_found_tasks() {
        let strategy = PollingStrategy::fixed(500);
        let current = Duration::from_millis(1000);

        // Fixed strategy always returns the same interval
        assert_eq!(
            strategy.next_interval(current, true),
            Duration::from_millis(500)
        );
        assert_eq!(
            strategy.next_interval(current, false),
            Duration::from_millis(500)
        );
    }

    #[test]
    fn test_next_interval_adaptive_resets_on_task_found() {
        let strategy = PollingStrategy::adaptive(100, 5000);
        let current = Duration::from_millis(2000);

        // Should reset to min interval when tasks found
        assert_eq!(
            strategy.next_interval(current, true),
            Duration::from_millis(100)
        );
    }

    #[test]
    fn test_next_interval_adaptive_backs_off() {
        let strategy = PollingStrategy::adaptive_with_backoff(100, 5000, 2.0);

        // Start at 100ms
        let interval = Duration::from_millis(100);

        // Should back off: 100ms * 2 = 200ms
        let interval = strategy.next_interval(interval, false);
        assert_eq!(interval, Duration::from_millis(200));

        // Should back off: 200ms * 2 = 400ms
        let interval = strategy.next_interval(interval, false);
        assert_eq!(interval, Duration::from_millis(400));

        // Should back off: 400ms * 2 = 800ms
        let interval = strategy.next_interval(interval, false);
        assert_eq!(interval, Duration::from_millis(800));
    }

    #[test]
    fn test_next_interval_adaptive_caps_at_max() {
        let strategy = PollingStrategy::adaptive_with_backoff(100, 500, 2.0);
        let interval = Duration::from_millis(400);

        // Should cap at max: 400ms * 2 = 800ms -> capped to 500ms
        let interval = strategy.next_interval(interval, false);
        assert_eq!(interval, Duration::from_millis(500));

        // Should stay at max
        let interval = strategy.next_interval(interval, false);
        assert_eq!(interval, Duration::from_millis(500));
    }

    #[test]
    fn test_jitter_bounds() {
        let strategy = PollingStrategy::default();
        let base_interval = Duration::from_secs(1);

        // Run multiple times to test jitter is within bounds
        for _ in 0..100 {
            let jittered = strategy.apply_jitter(base_interval);
            let jittered_ms = jittered.as_millis();

            // Should be within +/-10% of 1000ms (900ms to 1100ms)
            assert!(
                (900..=1100).contains(&jittered_ms),
                "Jittered interval {jittered_ms}ms is outside +/-10% bounds"
            );
        }
    }

    #[test]
    fn test_jitter_produces_variation() {
        let strategy = PollingStrategy::default();
        let base_interval = Duration::from_secs(1);

        // Collect several jittered values
        let jittered: Vec<Duration> = (0..10)
            .map(|_| strategy.apply_jitter(base_interval))
            .collect();

        // Check that we get some variation (not all the same)
        let first = jittered[0];
        let has_variation = jittered.iter().any(|&d| d != first);
        assert!(has_variation, "Jitter should produce variation");
    }

    #[test]
    fn test_serde_roundtrip_fixed() {
        let strategy = PollingStrategy::fixed(500);
        let json = serde_json::to_string(&strategy).expect("serialize");
        let parsed: PollingStrategy = serde_json::from_str(&json).expect("deserialize");

        match parsed {
            PollingStrategy::Fixed { interval_ms } => {
                assert_eq!(interval_ms, 500);
            }
            PollingStrategy::Adaptive { .. } => panic!("Expected Fixed strategy"),
        }
    }

    #[test]
    fn test_serde_roundtrip_adaptive() {
        let strategy = PollingStrategy::adaptive_with_backoff(100, 30000, 1.5);
        let json = serde_json::to_string(&strategy).expect("serialize");
        let parsed: PollingStrategy = serde_json::from_str(&json).expect("deserialize");

        match parsed {
            PollingStrategy::Adaptive {
                min_interval_ms,
                max_interval_ms,
                backoff_multiplier,
            } => {
                assert_eq!(min_interval_ms, 100);
                assert_eq!(max_interval_ms, 30000);
                assert!((backoff_multiplier - 1.5).abs() < f64::EPSILON);
            }
            PollingStrategy::Fixed { .. } => panic!("Expected Adaptive strategy"),
        }
    }

    #[test]
    fn test_serde_from_json_fixed() {
        let json = r#"{"strategy": "fixed", "interval_ms": 500}"#;
        let parsed: PollingStrategy = serde_json::from_str(json).expect("deserialize");

        match parsed {
            PollingStrategy::Fixed { interval_ms } => {
                assert_eq!(interval_ms, 500);
            }
            PollingStrategy::Adaptive { .. } => panic!("Expected Fixed strategy"),
        }
    }

    #[test]
    fn test_serde_from_json_adaptive() {
        let json = r#"{"strategy": "adaptive", "min_interval_ms": 100, "max_interval_ms": 30000, "backoff_multiplier": 2.0}"#;
        let parsed: PollingStrategy = serde_json::from_str(json).expect("deserialize");

        match parsed {
            PollingStrategy::Adaptive {
                min_interval_ms,
                max_interval_ms,
                backoff_multiplier,
            } => {
                assert_eq!(min_interval_ms, 100);
                assert_eq!(max_interval_ms, 30000);
                assert!((backoff_multiplier - 2.0).abs() < f64::EPSILON);
            }
            PollingStrategy::Fixed { .. } => panic!("Expected Adaptive strategy"),
        }
    }

    #[test]
    fn test_backoff_sequence_reaches_max() {
        let strategy = PollingStrategy::adaptive_with_backoff(100, 5000, 2.0);
        let mut interval = strategy.initial_interval();
        let mut iterations = 0;

        // Keep backing off until we reach max
        while interval < Duration::from_millis(5000) && iterations < 20 {
            interval = strategy.next_interval(interval, false);
            iterations += 1;
        }

        // With 2x backoff: 100 -> 200 -> 400 -> 800 -> 1600 -> 3200 -> 5000 (capped)
        // Should take 6 iterations
        assert_eq!(interval, Duration::from_millis(5000));
        assert!(
            iterations <= 10,
            "Should reach max in reasonable iterations"
        );
    }
}
