# Feature: Adaptive Polling

## Status: Implemented

## Overview

Adaptive polling dynamically adjusts the worker's poll interval based on queue activity. When tasks are available, polling is fast. When the queue is idle, polling backs off to reduce S3 API costs.

## Problem

With fixed-interval polling:
- A worker polling every 500ms makes **5.2 million requests/month**
- At AWS S3 GET pricing ($0.0004/1000), that's **$2.08/worker/month** just for polling
- 50 workers = **$104/month** in polling costs alone
- Most of this is wasted when the queue is idle

## Solution

Exponential backoff when idle, immediate reset when busy:

```
┌─────────────────────────────────────────────────────────────┐
│  Poll Interval Over Time (idle queue)                       │
│                                                             │
│  30s ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ●───●───●───●───  │
│                                        ╱                    │
│  15s ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ●                      │
│                                      ╱                      │
│   8s ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ●                        │
│                                    ╱                        │
│   4s ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ●                          │
│                                  ╱                          │
│   2s ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ●                            │
│                                ╱                            │
│   1s ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ●                              │
│                              ╱                              │
│ 500ms ●───●───●───●───●───●                                 │
│       ↑                   ↑                                 │
│     start            no tasks found                         │
│                      (backs off)                            │
└─────────────────────────────────────────────────────────────┘

When a task IS found:

│ 500ms ●───●───●───●  ← immediately resets
│                    ↑
│               task found!
```

## Configuration

### Rust

```rust
use qo::worker::{RunnerConfig, PollingStrategy};

let config = RunnerConfig {
    polling: PollingStrategy::Adaptive {
        min_interval: Duration::from_millis(100),  // Floor when busy
        max_interval: Duration::from_secs(30),     // Ceiling when idle
        backoff_multiplier: 2.0,                   // How fast to back off
    },
    ..Default::default()
};

// Or use fixed polling (current behavior)
let config = RunnerConfig {
    polling: PollingStrategy::Fixed {
        interval: Duration::from_millis(500),
    },
    ..Default::default()
};
```

### Python

```python
from qo import Worker, WorkerRunOptions, PollingStrategy

# Adaptive (recommended)
options = WorkerRunOptions(
    polling=PollingStrategy.adaptive(
        min_interval_ms=100,   # Fast when busy
        max_interval_ms=30000, # Slow when idle (30s)
        backoff_multiplier=2.0,
    )
)

# Fixed (predictable latency, higher cost)
options = WorkerRunOptions(
    polling=PollingStrategy.fixed(interval_ms=500)
)

await worker.run(options)
```

### Config File (qo.toml)

```toml
[worker.polling]
strategy = "adaptive"  # or "fixed"

# Adaptive settings
min_interval_ms = 100
max_interval_ms = 30000
backoff_multiplier = 2.0

# Fixed settings (used if strategy = "fixed")
# interval_ms = 500
```

### Environment Variables

```bash
QO_POLLING_STRATEGY=adaptive
QO_POLLING_MIN_INTERVAL_MS=100
QO_POLLING_MAX_INTERVAL_MS=30000
QO_POLLING_BACKOFF_MULTIPLIER=2.0
```

## Trade-offs (Be Explicit With Users)

| Setting | Cost | Latency | Use Case |
|---------|------|---------|----------|
| `min=100ms, max=30s` | **Lowest** | Up to 30s when idle | Batch jobs, cost-sensitive |
| `min=100ms, max=5s` | Low | Up to 5s when idle | General purpose (default) |
| `min=100ms, max=1s` | Medium | Up to 1s | Near-real-time |
| `fixed=100ms` | **Highest** | Consistent 100ms | Real-time, latency-critical |

### Cost Estimates (per worker/month, AWS S3)

| Configuration | Polls/month | Cost |
|---------------|-------------|------|
| Fixed 500ms | 5,200,000 | $2.08 |
| Fixed 100ms | 26,000,000 | $10.40 |
| Adaptive (mostly idle) | ~300,000 | $0.12 |
| Adaptive (50% busy) | ~2,600,000 | $1.04 |
| Adaptive (always busy) | ~26,000,000 | $10.40 |

**Key insight**: Adaptive polling costs almost nothing when idle, and matches fixed polling cost when busy. You only pay for what you use.

## Algorithm

```
current_interval = min_interval

loop:
    tasks = poll_for_tasks()

    if tasks.is_empty():
        # Back off: multiply interval, cap at max
        current_interval = min(current_interval * backoff_multiplier, max_interval)
    else:
        # Reset: immediately return to fast polling
        current_interval = min_interval
        process(tasks)

    sleep(current_interval)
```

### Jitter

To prevent thundering herd when multiple workers back off in sync, add ±10% jitter:

```rust
let jittered = current_interval * (0.9 + rand::random::<f64>() * 0.2);
```

## Defaults

```rust
PollingStrategy::Adaptive {
    min_interval: Duration::from_millis(100),
    max_interval: Duration::from_secs(5),
    backoff_multiplier: 2.0,
}
```

**Rationale for defaults:**
- `100ms` min: Fast enough for most use cases, not so fast it hammers S3
- `5s` max: Reasonable worst-case latency for general workloads
- `2.0` multiplier: Standard exponential backoff, reaches max in ~6 empty polls

## Implementation

### Files to Modify

```
crates/qo/src/worker/
├── config.rs      # Add PollingStrategy enum
├── runner.rs      # Implement adaptive loop
└── mod.rs         # Re-export

crates/qo/src/python/
└── worker.rs      # Python bindings for PollingStrategy

crates/qo/src/config.rs      # Config file parsing for polling settings
```

### New Types

```rust
/// Polling strategy for the worker loop.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "strategy", rename_all = "snake_case")]
pub enum PollingStrategy {
    /// Fixed interval polling. Predictable latency, higher cost.
    Fixed {
        /// Poll interval.
        #[serde(with = "humantime_serde")]
        interval: Duration,
    },

    /// Adaptive polling with exponential backoff. Lower cost, variable latency.
    Adaptive {
        /// Minimum interval (used when tasks are available).
        #[serde(with = "humantime_serde")]
        min_interval: Duration,

        /// Maximum interval (used when queue is idle).
        #[serde(with = "humantime_serde")]
        max_interval: Duration,

        /// Backoff multiplier (typically 2.0).
        backoff_multiplier: f64,
    },
}

impl Default for PollingStrategy {
    fn default() -> Self {
        Self::Adaptive {
            min_interval: Duration::from_millis(100),
            max_interval: Duration::from_secs(5),
            backoff_multiplier: 2.0,
        }
    }
}
```

### Runner Loop Changes

```rust
impl PollingStrategy {
    /// Calculate next interval based on whether tasks were found.
    pub fn next_interval(&self, current: Duration, found_tasks: bool) -> Duration {
        match self {
            Self::Fixed { interval } => *interval,
            Self::Adaptive { min_interval, max_interval, backoff_multiplier } => {
                if found_tasks {
                    *min_interval
                } else {
                    let backed_off = current.mul_f64(*backoff_multiplier);
                    backed_off.min(*max_interval)
                }
            }
        }
    }

    /// Get the initial interval.
    pub fn initial_interval(&self) -> Duration {
        match self {
            Self::Fixed { interval } => *interval,
            Self::Adaptive { min_interval, .. } => *min_interval,
        }
    }

    /// Apply jitter to prevent thundering herd.
    pub fn apply_jitter(&self, interval: Duration) -> Duration {
        let jitter = 0.9 + rand::random::<f64>() * 0.2; // ±10%
        interval.mul_f64(jitter)
    }
}
```

## Observability

Expose metrics for monitoring:

```rust
// Prometheus metrics
worker_poll_interval_seconds  // Gauge: current poll interval
worker_poll_total             // Counter: total polls
worker_poll_empty_total       // Counter: polls that found no tasks
worker_backoff_total          // Counter: times we backed off
```

Users can alert on:
- `worker_poll_interval_seconds > 10` — workers are idle, might indicate producer issues
- `rate(worker_backoff_total[5m]) > 100` — excessive backoff, queue might be empty

## Documentation Updates

Add to getting-started.md:

```markdown
## Polling Strategy

qo uses adaptive polling by default to minimize S3 costs:

- When tasks are available: polls every 100ms
- When queue is idle: backs off up to 5s between polls

This means idle workers cost almost nothing, while busy workers
remain responsive.

### Configuring Polling

For latency-sensitive workloads:
​```python
options = WorkerRunOptions(
    polling=PollingStrategy.fixed(interval_ms=100)
)
​```

For cost-sensitive workloads:
​```python
options = WorkerRunOptions(
    polling=PollingStrategy.adaptive(
        min_interval_ms=100,
        max_interval_ms=30000,  # 30s max backoff
    )
)
​```
```

## Testing

1. **Unit tests**: Verify `next_interval()` logic
2. **Integration test**: Submit task, verify worker picks it up within `min_interval`
3. **Backoff test**: Verify interval grows correctly when no tasks
4. **Reset test**: Verify interval resets immediately when task found
5. **Jitter test**: Verify jitter stays within ±10% bounds

## Migration

- Default changes from fixed 500ms to adaptive 100ms-5s
- Existing configs with `poll_interval` continue to work (treated as fixed)
- No breaking changes—old configs map to `PollingStrategy::Fixed`

## Estimation

~100 lines of Rust, ~30 lines of Python bindings, ~20 lines of config parsing.
