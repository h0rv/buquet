# Feature: Task Expiration / TTL

## Status: Implemented

## Overview

Tasks can expire if not processed within a deadline. Expired tasks are skipped
by workers and marked with `Expired` status, freeing resources without
counting as failures.

## Philosophy Alignment

| Principle | How This Aligns |
|-----------|-----------------|
| S3 as only dependency | Uses existing `available_at` pattern, just adds `expires_at` |
| Simple, boring, reliable | One new field, simple timestamp comparison |
| No coordination services | Workers check expiration on claim, no external TTL service |
| Crash-safe | Expiration time persisted with task |
| Debuggable | `buquet status` shows expiration, clear why task wasn't processed |

## Problem

- Time-sensitive tasks become useless after a deadline
- Stale tasks accumulate, wasting resources
- Currently: tasks wait forever or require manual cleanup
- Examples:
  - Send a reminder email (useless if sent days late)
  - Process a real-time event (stale after minutes)
  - Generate a report for a meeting (useless after meeting ends)

## Solution

Add `expires_at: Option<DateTime>` field. Workers skip expired tasks. Monitor
marks them as `Expired` during sweeps.

## Task Status Update

```rust
enum TaskStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
    Expired,   // NEW
    Archived,
}
```

## API

### Python

```python
from buquet import connect
from datetime import datetime, timedelta

queue = await connect(bucket="my-queue")

# Submit with absolute expiration
task = await queue.submit(
    "send_reminder",
    {"user_id": "123"},
    expires_at=datetime(2026, 1, 28, 17, 0, 0),  # 5pm today
)

# Submit with relative TTL
task = await queue.submit(
    "process_event",
    {"event_id": "456"},
    ttl=timedelta(hours=1),  # Expires in 1 hour
)

# Submit with TTL in seconds
task = await queue.submit(
    "quick_task",
    {"data": "..."},
    ttl_seconds=300,  # Expires in 5 minutes
)

# Check if expired
task = await queue.get(task_id)
if task.status == TaskStatus.Expired:
    print(f"Task expired at {task.expires_at}")

# Check remaining time
now = await queue.now()
if task.expires_at:
    remaining = task.expires_at - now
    print(f"Expires in {remaining}")
```

### Rust

```rust
use buquet::Queue;
use chrono::{Duration, Utc};

let queue = Queue::connect("my-queue").await?;

// Submit with absolute expiration
let task = queue
    .submit("send_reminder")
    .input(json!({"user_id": "123"}))
    .expires_at(Utc::now() + Duration::hours(2))
    .send()
    .await?;

// Submit with TTL
let task = queue
    .submit("process_event")
    .input(json!({"event_id": "456"}))
    .ttl(Duration::minutes(30))
    .send()
    .await?;
```

### CLI

```bash
# Submit with absolute expiration
buquet submit -t send_reminder -i '{}' --expires-at "2026-01-28T17:00:00Z"

# Submit with TTL
buquet submit -t process_event -i '{}' --ttl 1h
buquet submit -t quick_task -i '{}' --ttl 5m
buquet submit -t batch_job -i '{}' --ttl 7d

# Check task expiration
buquet status abc123
# Output includes: expires_at: 2026-01-28T17:00:00Z (in 2h 30m)

# List expired tasks
buquet list --status expired
```

## Task Object

```json
{
  "id": "abc123",
  "task_type": "send_reminder",
  "status": "pending",
  "input": {"user_id": "123"},
  "expires_at": "2026-01-28T17:00:00Z",
  "expired_at": null,
  ...
}
```

| Field | Type | Description |
|-------|------|-------------|
| `expires_at` | `Option<DateTime>` | When task expires (null = never) |
| `expired_at` | `Option<DateTime>` | When task was marked expired |

## Behavior

### Expiration Check Flow

```
Worker claim attempt:
    │
    ├── Is task expired? (expires_at < now)
    │   │
    │   ├── Yes: Skip task, mark as Expired (best-effort)
    │   │
    │   └── No: Proceed with claim
    │
    ▼
Normal claim flow...
```

### Worker Behavior

Workers check expiration before claiming:

```rust
// In worker claim loop
let (task, etag) = queue.get(task_id).await?;
let now = queue.now().await?;

// Check expiration
if let Some(expires_at) = task.expires_at {
    if now >= expires_at {
        // Task expired, mark it (best-effort)
        queue.mark_expired(task_id).await.ok();
        continue;
    }
}

// Check availability
if !task.is_available_at(now) {
    continue;
}

// Proceed with claim...
```

### Monitor Behavior

Monitor sweeps for expired tasks during regular checks:

```rust
// In monitor sweep
for task_id in ready_tasks {
    let task = queue.get(task_id).await?;
    let now = queue.now().await?;

    if let Some(expires_at) = task.expires_at {
        if now >= expires_at && task.status == TaskStatus::Pending {
            queue.mark_expired(task_id).await?;
            metrics.expired_tasks.inc();
        }
    }
}
```

### Mark Expired Implementation

```rust
pub async fn mark_expired(&self, task_id: Uuid) -> Result<Task> {
    let (task, etag) = self.get(task_id).await?.ok_or(NotFound)?;

    if task.status != TaskStatus::Pending {
        return Err(CannotExpire { status: task.status });
    }

    let now = self.now().await?;
    let updated = Task {
        status: TaskStatus::Expired,
        expired_at: Some(now),
        updated_at: now,
        ..task
    };

    self.put_task_if_match(&updated, &etag).await?;
    self.delete_ready_index(task_id).await.ok(); // best-effort

    Ok(updated)
}
```

## Edge Cases

### Expiration During Execution

If task expires while running, it continues to completion. Expiration only
affects pending tasks.

**Rationale:** Once work starts, let it finish. Handler can check expiration
internally if needed.

```python
@worker.task("process")
async def handle(input, context):
    # Optional: check if task is about to expire
    task = context.task
    now = await context.queue.now()

    if task.expires_at and (task.expires_at - now).total_seconds() < 60:
        # Less than 1 minute left, abort early
        raise PermanentError("Task about to expire, aborting")

    # Normal processing...
```

### Retry with Expiration

When a task fails and retries:
- `available_at` is set to backoff time
- `expires_at` remains unchanged

If `available_at > expires_at`, task will expire before becoming available.
This is intentional - the task had its chance.

### Reschedule with Expiration

When `RescheduleError` is raised:
- Worker can optionally extend expiration

```python
# Extend expiration when rescheduling
raise RescheduleError(
    delay_seconds=60,
    extend_expiration_seconds=120,  # Add 2 minutes to expires_at
)
```

### Zero TTL

`ttl=0` means "expire immediately" - task is submitted already expired.
Use case: testing expiration handling.

### No Expiration (Default)

`expires_at=None` means task never expires. This is the default.

## Interaction with Other Features

### Scheduling + Expiration

```python
# Task scheduled for 5pm, expires at 6pm
task = await queue.submit(
    "meeting_reminder",
    data,
    schedule_at=datetime(2026, 1, 28, 17, 0),  # Run at 5pm
    expires_at=datetime(2026, 1, 28, 18, 0),   # Expire at 6pm
)
```

### Cancellation vs Expiration

| Aspect | Cancellation | Expiration |
|--------|--------------|------------|
| Trigger | Explicit user action | Time-based automatic |
| Status | `Cancelled` | `Expired` |
| Running tasks | Cooperative cancel | Continues to completion |
| Retry | Can retry cancelled | Cannot retry expired |

## Metrics

```
buquet_tasks_expired_total{task_type}
buquet_task_ttl_seconds{task_type}  # Histogram of TTL values
buquet_tasks_near_expiration{task_type}  # Gauge of tasks expiring soon
```

## Version History

Expiration appears in task history:

```bash
$ buquet history abc123

Version 1: pending (created, expires_at: 2026-01-28T17:00:00Z)
Version 2: expired (expired_at: 2026-01-28T17:00:05Z)
```

## Index Cleanup

Expired tasks have their ready index removed:

```
DELETE ready/{shard}/{bucket}/{task_id}
```

## Implementation

### Files to Change

- `crates/buquet/src/models/task.rs` - Add `Expired` status, `expires_at`, `expired_at` fields
- `crates/buquet/src/queue/ops.rs` - Add `mark_expired()`, TTL handling in submit
- `crates/buquet/src/queue/submit.rs` - Add `expires_at`, `ttl` to `SubmitOptions`
- `crates/buquet/src/worker/runner.rs` - Check expiration before claim
- `crates/buquet/src/worker/monitor.rs` - Sweep for expired tasks
- `crates/buquet/src/python/queue.rs` - Python bindings
- `crates/buquet/src/cli/commands.rs` - Add `--expires-at`, `--ttl` flags

### Estimated Effort

| Component | Lines |
|-----------|-------|
| Task model | ~15 |
| Submit with TTL | ~25 |
| Mark expired | ~30 |
| Worker check | ~15 |
| Monitor sweep | ~25 |
| Python bindings | ~30 |
| CLI | ~25 |
| Tests | ~80 |
| **Total** | ~245 |

## Implementation Notes

All core features from this spec are fully implemented:

- `Expired` status in TaskStatus enum
- Task fields: `expires_at`, `expired_at`
- `queue.submit(..., ttl_seconds=N)` - submit with TTL in seconds
- `queue.submit(..., expires_at=datetime)` - submit with absolute expiration
- `queue.submit_many(..., ttl_seconds=N)` - bulk submit with TTL
- `queue.submit_many(..., expires_at=datetime)` - bulk submit with expiration
- `task.is_expired_at(timestamp)` - check if task is expired at a given time
- `queue.mark_expired(task_id)` - manually mark a task as expired
- Workers check expiration before claiming and mark expired tasks
- Monitor/sweeper marks expired tasks during index sweeps
- Expiration works with scheduled tasks
- Python bindings with full support
- Rust core implementation
- CLI flags for `buquet submit`:
  - `--ttl 1h` / `--ttl 30m` / `--ttl 7d` - relative TTL
  - `--expires-at 2026-01-28T17:00:00Z` - absolute expiration

### Not Yet Implemented

- `RescheduleError` with `extend_expiration_seconds` (optional feature)

## Non-Goals

- **Automatic retry of expired tasks**: Expired means "too late"
- **Sliding expiration**: Expiration doesn't extend on claim
- **Per-type default TTL**: Configure in application code
- **Expiration warnings**: Use metrics/alerts externally
