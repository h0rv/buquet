# Feature: Task Cancellation

## Status: Implemented

## Overview

Allow tasks to be cancelled before or during execution. Cancelled tasks stop
processing and move to a terminal `Cancelled` status without counting as failures.

## Philosophy Alignment

| Principle | How This Aligns |
|-----------|-----------------|
| S3 as only dependency | Uses existing task object, just a status change |
| Simple, boring, reliable | One new status, trivial implementation |
| No coordination services | Workers check status on claim, no signals needed |
| Crash-safe | Cancelled status is persisted, survives restarts |
| Debuggable | `qo status` shows cancelled, history shows who cancelled |

## Problem

- User submits task by mistake
- Business logic changes and task is no longer needed
- Batch job should stop early
- Currently: wait for task to fail or complete, no way to stop it

## Solution

Add `Cancelled` status. Workers skip cancelled tasks. Running tasks check
cancellation periodically (cooperative cancellation).

## Task Status Update

```rust
enum TaskStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,  // NEW
    Archived,
}
```

## API

### Python

```python
from qo import connect, TaskStatus

queue = await connect(bucket="my-queue")

# Cancel a pending task
await queue.cancel(task_id)

# Cancel with reason (stored in last_error)
await queue.cancel(task_id, reason="User requested cancellation")

# Check if cancelled
task = await queue.get(task_id)
if task.status == TaskStatus.Cancelled:
    print(f"Cancelled: {task.last_error}")

# Batch cancel
await queue.cancel_many([task_id1, task_id2, task_id3])

# Cancel by task type (all pending tasks of a type)
cancelled_count = await queue.cancel_by_type("old_task_type")
```

### Rust

```rust
use qo::{Queue, TaskStatus};

let queue = Queue::connect("my-queue").await?;

// Cancel a task
queue.cancel(task_id).await?;

// Cancel with reason
queue.cancel_with_reason(task_id, "No longer needed").await?;

// Check status
let task = queue.get(task_id).await?;
match task.status {
    TaskStatus::Cancelled => println!("Was cancelled"),
    _ => {}
}
```

### CLI

```bash
# Cancel a task
qo cancel abc123

# Cancel with reason
qo cancel abc123 --reason "Duplicate submission"

# Cancel multiple tasks
qo cancel abc123 def456 ghi789

# Cancel all pending tasks of a type
qo cancel --type old_task_type

# Dry run (show what would be cancelled)
qo cancel --type old_task_type --dry-run
```

## Behavior

### Cancellation Flow

```
                    cancel()
                       │
                       ▼
┌─────────┐       ┌─────────┐
│ Pending │──────►│Cancelled│
└─────────┘       └─────────┘
     │
     │ claim()
     ▼
┌─────────┐       ┌─────────┐
│ Running │──────►│Cancelled│  (cooperative)
└─────────┘       └─────────┘
```

### Cancelling Pending Tasks

Immediate: CAS update from `Pending` to `Cancelled`.

```rust
// Cancel implementation (simplified)
pub async fn cancel(&self, task_id: Uuid, reason: Option<&str>) -> Result<Task> {
    let (task, etag) = self.get(task_id).await?.ok_or(NotFound)?;

    if task.status != TaskStatus::Pending {
        return Err(CannotCancel { status: task.status });
    }

    let updated = Task {
        status: TaskStatus::Cancelled,
        last_error: reason.map(String::from),
        updated_at: self.now().await?,
        ..task
    };

    self.put_task_if_match(&updated, &etag).await?;
    self.delete_ready_index(task_id).await.ok(); // best-effort

    Ok(updated)
}
```

### Cancelling Running Tasks (Cooperative)

Running tasks cannot be force-killed (no signals in S3). Instead:

1. Mark task as `cancel_requested: true`
2. Handler checks periodically and exits early
3. Worker transitions to `Cancelled` on handler exit

```python
@worker.task("long_job")
async def handle(input, context):
    for i, item in enumerate(input["items"]):
        # Check for cancellation periodically
        if await context.is_cancellation_requested():
            return None  # Worker will mark as Cancelled

        await process(item)

    return {"processed": len(input["items"])}
```

### Task Fields

```json
{
  "id": "abc123",
  "status": "cancelled",
  "cancel_requested": false,
  "cancelled_at": "2026-01-28T12:00:00Z",
  "cancelled_by": "user:alice",
  "last_error": "User requested cancellation"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `cancel_requested` | `bool` | True if cancellation requested for running task |
| `cancelled_at` | `Option<DateTime>` | When task was cancelled |
| `cancelled_by` | `Option<String>` | Who/what cancelled (user, system, etc.) |

## Edge Cases

### Cancel Already Completed Task

Returns error. Cannot cancel completed/failed/archived tasks.

```python
try:
    await queue.cancel(completed_task_id)
except CannotCancelError as e:
    print(f"Cannot cancel: task is {e.status}")
```

### Cancel Already Cancelled Task

Idempotent success. Returns current task state.

### Race: Cancel vs Claim

If cancel and claim race:
- Cancel wins if it CAS-updates first
- Claim wins if it CAS-updates first
- Loser gets 412 and retries/skips

Either outcome is correct.

### Race: Cancel vs Complete

If task completes before cancel is processed:
- Cancel fails with `CannotCancel { status: Completed }`
- Task remains completed

### Cancellation During Retry Backoff

Task in `Pending` with future `available_at` can still be cancelled.

## Worker Behavior

Workers already skip non-Pending tasks. Additional check for `cancel_requested`:

```rust
// In worker claim loop
let task = queue.get(task_id).await?;

if task.status == TaskStatus::Cancelled {
    // Already cancelled, skip
    continue;
}

if task.status == TaskStatus::Pending && !task.is_available_at(now) {
    // Not ready yet, skip
    continue;
}

// Proceed with claim...
```

After claiming, if handler returns `None` and `cancel_requested`:

```rust
// In worker result handling
match handler_result {
    Ok(Some(output)) => self.complete(task, output).await,
    Ok(None) if task.cancel_requested => self.mark_cancelled(task).await,
    Ok(None) => self.complete(task, json!(null)).await,
    Err(e) => self.handle_error(task, e).await,
}
```

## Index Cleanup

On cancellation, delete ready index (best-effort):

```
DELETE ready/{shard}/{bucket}/{task_id}
```

No lease index to clean (task was Pending, not Running).

## Metrics

```
qo_tasks_cancelled_total{task_type}
qo_cancel_requests_total{task_type, outcome=success|already_cancelled|not_found|invalid_status}
```

## Version History

Cancellations appear in task history:

```bash
$ qo history abc123

Version 1: pending (created)
Version 2: cancelled (cancelled_by: user:alice, reason: "No longer needed")
```

## Implementation

### Files to Change

- `crates/qo/src/models/task.rs` - Add `Cancelled` status, `cancel_requested`, `cancelled_at`, `cancelled_by`
- `crates/qo/src/queue/ops.rs` - Add `cancel()`, `cancel_many()`, `cancel_by_type()`
- `crates/qo/src/worker/runner.rs` - Check `cancel_requested` after handler returns
- `crates/qo/src/python/queue.rs` - Python bindings for cancel
- `crates/qo/src/python/worker.rs` - Add `context.is_cancellation_requested()`
- `crates/qo/src/cli/commands.rs` - Add `Cancel` subcommand

### Estimated Effort

| Component | Lines |
|-----------|-------|
| Task model changes | ~15 |
| Queue cancel methods | ~60 |
| Worker integration | ~30 |
| Python bindings | ~40 |
| CLI | ~30 |
| Tests | ~100 |
| **Total** | ~275 |

## Implementation Notes

All features from this spec are fully implemented:

- `Cancelled` status in TaskStatus enum
- Task fields: `cancel_requested`, `cancelled_at`, `cancelled_by`
- `queue.cancel(task_id)` - cancel a pending task
- `queue.cancel(task_id, reason=...)` - cancel with reason stored in `last_error`
- `queue.cancel_many([task_ids])` - batch cancellation (parallel execution)
- `queue.cancel_by_type(task_type)` - cancel all pending tasks of a type
- `queue.request_cancellation(task_id)` - request cooperative cancellation for running tasks
- `context.is_cancellation_requested()` - check if cancellation was requested in handlers
- Workers skip cancelled tasks
- Cannot cancel completed/failed tasks (returns error)
- Python bindings with full support
- Rust core implementation
- CLI `qo cancel` command with:
  - Multiple task IDs: `qo cancel uuid1 uuid2 uuid3`
  - Cancel by type: `qo cancel --task-type old_task_type`
  - Reason tracking: `qo cancel uuid --reason "No longer needed"`
  - Metadata: `qo cancel uuid --cancelled-by "user:alice"`
  - JSON output: `qo cancel uuid --json`

## Non-Goals

- **Force kill running tasks**: No signals, cooperative only
- **Cancel with rollback**: qo doesn't know what to roll back
- **Automatic cancellation on timeout**: That's what Failed status is for
