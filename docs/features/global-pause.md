# Feature: Global Pause / Resume

## Status: Proposed

## Overview

Pause all task processing across the queue without stopping workers. Workers
remain running but skip claiming new tasks until resumed. Useful for
maintenance, deployments, and incident response.

## Philosophy Alignment

| Principle | How This Aligns |
|-----------|-----------------|
| S3 as only dependency | Single control object in S3, no coordination service |
| Simple, boring, reliable | One file check per poll cycle, trivial implementation |
| No coordination services | Workers poll control object, no signals needed |
| Crash-safe | Pause state persisted, survives restarts |
| Debuggable | `buquet status` shows pause state, clear operational status |

## Problem

- Need to stop processing during maintenance
- Want to pause without killing workers
- Deployment needs queue drain before proceeding
- Incident requires immediate stop
- Currently: kill workers or wait for queue to empty

## Solution

Create a control object `control/paused.json`. Workers check before claiming.
When paused, workers sleep but don't exit.

## Control Object

```
control/paused.json
```

```json
{
  "paused_at": "2026-01-28T12:00:00Z",
  "paused_by": "user:alice",
  "reason": "Deploying new version",
  "resume_at": null
}
```

| Field | Type | Description |
|-------|------|-------------|
| `paused_at` | `DateTime` | When pause was activated |
| `paused_by` | `String` | Who/what initiated pause |
| `reason` | `Option<String>` | Why paused |
| `resume_at` | `Option<DateTime>` | Auto-resume time (null = manual) |

When `control/paused.json` exists, queue is paused. Delete to resume.

## API

### Python

```python
from buquet import connect
from datetime import timedelta

queue = await connect(bucket="my-queue")

# Pause the queue
await queue.pause(reason="Deploying v2.0")

# Pause with auto-resume
await queue.pause(
    reason="Database maintenance",
    resume_after=timedelta(hours=1),
)

# Check if paused
status = await queue.status()
if status.is_paused:
    print(f"Paused at {status.paused_at}")
    print(f"Reason: {status.pause_reason}")
    if status.resume_at:
        print(f"Auto-resume at {status.resume_at}")

# Resume
await queue.resume()

# Force resume (even if auto-resume is set)
await queue.resume(force=True)
```

### Rust

```rust
use buquet::Queue;
use chrono::Duration;

let queue = Queue::connect("my-queue").await?;

// Pause
queue.pause()
    .reason("Deploying v2.0")
    .paused_by("deploy-script")
    .send()
    .await?;

// Pause with auto-resume
queue.pause()
    .reason("Maintenance window")
    .resume_after(Duration::hours(2))
    .send()
    .await?;

// Check status
let status = queue.status().await?;
if status.is_paused {
    println!("Queue is paused: {}", status.pause_reason.unwrap_or_default());
}

// Resume
queue.resume().await?;
```

### CLI

```bash
# Pause the queue
buquet pause
buquet pause --reason "Deploying new version"
buquet pause --reason "Maintenance" --paused-by "alice"

# Pause with auto-resume
buquet pause --reason "Quick fix" --resume-after 30m
buquet pause --reason "Maintenance window" --resume-after 2h
buquet pause --reason "Overnight batch" --resume-at "2026-01-29T06:00:00Z"

# Check status
buquet status
# Output:
#   Queue: my-queue
#   Status: PAUSED
#   Paused at: 2026-01-28T12:00:00Z
#   Paused by: alice
#   Reason: Deploying new version
#   Auto-resume: in 25 minutes
#
#   Workers: 5 connected (all idle)
#   Pending tasks: 1,234

# Resume the queue
buquet resume

# Force resume (ignores auto-resume time)
buquet resume --force
```

## Worker Behavior

### Pause Check Flow

```
Worker poll cycle:
    │
    ├── Check pause status
    │   │
    │   ├── Paused: sleep, skip claiming
    │   │   │
    │   │   └── Log: "Queue paused, waiting..."
    │   │
    │   └── Not paused: continue to claim
    │
    ▼
Normal claim flow...
```

### Implementation

```rust
impl Worker {
    async fn poll_once(&mut self) -> Result<Option<Task>> {
        // Check pause status first
        if let Some(pause) = self.queue.get_pause_status().await? {
            // Check auto-resume
            let now = self.queue.now().await?;
            if let Some(resume_at) = pause.resume_at {
                if now >= resume_at {
                    // Auto-resume time reached, resume
                    self.queue.resume().await.ok();
                } else {
                    // Still paused, wait
                    tracing::info!(
                        reason = ?pause.reason,
                        resume_at = %resume_at,
                        "Queue paused, waiting for auto-resume"
                    );
                    return Ok(None);
                }
            } else {
                // Manual resume required
                tracing::info!(
                    reason = ?pause.reason,
                    "Queue paused, waiting for manual resume"
                );
                return Ok(None);
            }
        }

        // Normal polling...
        self.claim_next_task().await
    }
}
```

### Pause Status Caching

To avoid checking S3 on every poll:

```rust
struct PauseCache {
    is_paused: bool,
    checked_at: Instant,
    ttl: Duration,  // e.g., 5 seconds
}

impl Worker {
    async fn is_paused(&mut self) -> Result<bool> {
        let now = Instant::now();

        // Use cached value if fresh
        if now.duration_since(self.pause_cache.checked_at) < self.pause_cache.ttl {
            return Ok(self.pause_cache.is_paused);
        }

        // Refresh cache
        let pause = self.queue.get_pause_status().await?;
        self.pause_cache = PauseCache {
            is_paused: pause.is_some(),
            checked_at: now,
            ttl: Duration::from_secs(5),
        };

        Ok(self.pause_cache.is_paused)
    }
}
```

Cache TTL means pause takes effect within ~5 seconds across all workers.

## Operations

### Pause Implementation

```rust
pub async fn pause(&self, options: PauseOptions) -> Result<()> {
    let now = self.now().await?;

    let pause_state = PauseState {
        paused_at: now,
        paused_by: options.paused_by.unwrap_or_else(|| "unknown".to_string()),
        reason: options.reason,
        resume_at: options.resume_after.map(|d| now + d),
    };

    let body = serde_json::to_vec(&pause_state)?;

    // Use If-None-Match to prevent double-pause
    self.client.put_object_if_none_match("control/paused.json", &body).await?;

    Ok(())
}
```

### Resume Implementation

```rust
pub async fn resume(&self) -> Result<()> {
    self.client.delete_object("control/paused.json").await?;
    Ok(())
}
```

### Get Pause Status

```rust
pub async fn get_pause_status(&self) -> Result<Option<PauseState>> {
    match self.client.get_object("control/paused.json").await {
        Ok((body, _etag)) => {
            let state: PauseState = serde_json::from_slice(&body)?;
            Ok(Some(state))
        }
        Err(StorageError::NotFound { .. }) => Ok(None),
        Err(e) => Err(e.into()),
    }
}
```

## Edge Cases

### Pause While Tasks Running

Running tasks continue to completion. Only new claims are blocked.

### Pause During Claim

If pause activates while worker is mid-claim:
- Claim may succeed (task runs)
- Next poll will see pause

This is acceptable - pause is not instantaneous.

### Multiple Pause Requests

Second pause fails with `AlreadyPaused` error. Use `--force` to overwrite.

```bash
buquet pause --reason "First pause"
buquet pause --reason "Update reason"  # Error: Queue already paused
buquet pause --reason "Update reason" --force  # Overwrites
```

### Auto-Resume While Worker Offline

If auto-resume time passes with no workers polling:
- First worker to poll after resume_at will delete pause object
- Queue effectively resumes

### Pause + Shutdown

If worker receives SIGTERM while paused:
- Normal shutdown (no task to requeue)
- Worker exits cleanly

## Scoped Pause (Future Enhancement)

Pause specific task types or shards:

```bash
# Pause only specific task type
buquet pause --type send_email

# Pause specific shards
buquet pause --shards 0,1,2,3
```

Control objects:
```
control/paused.json           # Global pause
control/paused/send_email     # Task type pause
control/paused/shard/0        # Shard pause
```

**Not in initial implementation** - adds complexity.

## Metrics

```
buquet_queue_paused{bucket}  # Gauge: 1 if paused, 0 if not
buquet_pause_duration_seconds{bucket}  # How long paused
buquet_pause_events_total{bucket, action=pause|resume}
```

## Worker Status Display

```bash
$ buquet workers

WORKER          STATUS    CURRENT TASK    UPTIME
worker-1        idle      -               2h 30m
worker-2        idle      -               2h 28m
worker-3        idle      -               2h 25m

Queue Status: PAUSED (since 5 minutes ago)
Reason: Deploying v2.0
Auto-resume: in 25 minutes
```

## Implementation

### Files to Change

- `crates/buquet/src/queue/control.rs` (new) - Pause/resume operations
- `crates/buquet/src/models/control.rs` (new) - `PauseState` struct
- `crates/buquet/src/worker/runner.rs` - Check pause before claiming
- `crates/buquet/src/python/queue.rs` - Python bindings
- `crates/buquet/src/cli/commands.rs` - Add `Pause`, `Resume` subcommands, update `Status`

### Estimated Effort

| Component | Lines |
|-----------|-------|
| Control module | ~50 |
| PauseState model | ~20 |
| Worker integration | ~30 |
| Pause caching | ~25 |
| Python bindings | ~30 |
| CLI commands | ~40 |
| Tests | ~70 |
| **Total** | ~265 |

## Non-Goals

- **Instant pause**: Workers check periodically, not instant
- **Pause running tasks**: Only affects new claims
- **Pause persistence**: Pause clears on object delete
- **Pause notifications**: Use external alerting
