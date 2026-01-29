# Feature: Progress Reporting

## Status: Proposed

## Overview

Long-running tasks can report progress to provide visibility into execution.
Progress is stored in the task object and visible via API, CLI, and UI.

## Philosophy Alignment

| Principle | How This Aligns |
|-----------|-----------------|
| S3 as only dependency | Progress stored in task object, no external state |
| Simple, boring, reliable | Just a field update, uses existing CAS pattern |
| No coordination services | No pub/sub, polling-based progress checks |
| Crash-safe | Progress persisted, visible after restart |
| Debuggable | `oq status` shows progress, history shows updates |

## Problem

- Long jobs run for minutes/hours with no visibility
- Users don't know if task is stuck or progressing
- Hard to estimate completion time
- Currently: check logs or wait for completion

## Solution

Add `progress` field to task. Handlers call `context.report_progress()` to
update. Observers poll for progress.

## Task Fields

```json
{
  "id": "abc123",
  "task_type": "batch_process",
  "status": "running",
  "progress": {
    "percent": 45,
    "current": 450,
    "total": 1000,
    "message": "Processing file 450 of 1000",
    "updated_at": "2026-01-28T12:30:00Z"
  },
  ...
}
```

| Field | Type | Description |
|-------|------|-------------|
| `progress.percent` | `Option<u8>` | 0-100 completion percentage |
| `progress.current` | `Option<u64>` | Current item/step number |
| `progress.total` | `Option<u64>` | Total items/steps |
| `progress.message` | `Option<String>` | Human-readable status |
| `progress.updated_at` | `DateTime` | When progress was last updated |

All progress fields are optional. Use what makes sense for your task.

## API

### Python

```python
from oq import connect, Worker

queue = await connect(bucket="my-queue")
worker = Worker(queue, "worker-1", queue.all_shards())

@worker.task("batch_process")
async def handle(input, context):
    items = input["items"]
    total = len(items)

    for i, item in enumerate(items):
        # Report progress
        await context.report_progress(
            percent=int((i / total) * 100),
            current=i,
            total=total,
            message=f"Processing item {i + 1} of {total}",
        )

        await process_item(item)

    return {"processed": total}


# Observer: check progress
task = await queue.get(task_id)
if task.progress:
    print(f"Progress: {task.progress.percent}%")
    print(f"Status: {task.progress.message}")
```

### Progress Reporting Options

```python
# Percentage only
await context.report_progress(percent=50)

# Current/total (percentage calculated automatically)
await context.report_progress(current=50, total=100)

# Message only
await context.report_progress(message="Downloading file...")

# All fields
await context.report_progress(
    percent=75,
    current=750,
    total=1000,
    message="Processing batch 3 of 4",
)

# Throttled reporting (skip if last update < N seconds ago)
await context.report_progress(percent=50, throttle_seconds=5)
```

### Rust

```rust
use oq::{Queue, Worker, TaskHandler, TaskContext};

struct BatchProcessor;

#[async_trait]
impl TaskHandler for BatchProcessor {
    fn task_type(&self) -> &str { "batch_process" }

    async fn handle(&self, input: Value, ctx: &TaskContext) -> Result<Value, TaskError> {
        let items: Vec<Item> = serde_json::from_value(input["items"].clone())?;
        let total = items.len();

        for (i, item) in items.iter().enumerate() {
            ctx.report_progress(Progress {
                percent: Some(((i * 100) / total) as u8),
                current: Some(i as u64),
                total: Some(total as u64),
                message: Some(format!("Processing item {} of {}", i + 1, total)),
            }).await?;

            process_item(item).await?;
        }

        Ok(json!({"processed": total}))
    }
}
```

### CLI

```bash
# Check task progress
oq status abc123
# Output:
#   Task: abc123
#   Type: batch_process
#   Status: running
#   Progress: 45% (450/1000)
#   Message: Processing file 450 of 1000
#   Updated: 30 seconds ago

# Watch progress (polls every 2 seconds)
oq status abc123 --watch
oq status abc123 -w

# Watch with custom interval
oq status abc123 --watch --interval 5

# JSON output for scripting
oq status abc123 --json | jq '.progress'
```

## Implementation

### Progress Update Flow

```
Handler                     Worker                          S3
   │                          │                              │
   │  report_progress(50%)    │                              │
   │─────────────────────────►│                              │
   │                          │                              │
   │                          │  GET task (capture ETag)     │
   │                          │─────────────────────────────►│
   │                          │◄─────────────────────────────│
   │                          │                              │
   │                          │  PUT task with progress      │
   │                          │  If-Match: {etag}            │
   │                          │─────────────────────────────►│
   │                          │◄─────────────────────────────│
   │                          │                              │
   │  (continue processing)   │                              │
   │◄─────────────────────────│                              │
```

### Progress Update Implementation

```rust
impl TaskContext {
    pub async fn report_progress(&self, progress: Progress) -> Result<()> {
        // Check throttling
        if let Some(throttle) = self.throttle_seconds {
            let now = Instant::now();
            if now.duration_since(self.last_progress_update) < Duration::from_secs(throttle) {
                return Ok(()); // Skip this update
            }
        }

        // Read current task state
        let (mut task, etag) = self.queue.get(self.task_id).await?.ok_or(TaskGone)?;

        // Verify we still own the lease
        if task.lease_id != Some(self.lease_id) {
            return Err(LeaseExpired);
        }

        // Update progress
        task.progress = Some(TaskProgress {
            percent: progress.percent,
            current: progress.current,
            total: progress.total,
            message: progress.message,
            updated_at: self.queue.now().await?,
        });

        // Write back with CAS
        self.queue.put_task_if_match(&task, &etag).await?;
        self.last_progress_update = Instant::now();

        Ok(())
    }
}
```

### Throttling

Progress updates hit S3 (GET + PUT). Throttle to avoid excessive requests:

```python
# Default: no throttling (every call writes to S3)
await context.report_progress(percent=50)

# Throttled: skip if last update < 5 seconds ago
await context.report_progress(percent=50, throttle_seconds=5)

# Worker-level default throttle
worker = Worker(queue, "worker-1", shards, progress_throttle_seconds=2)
```

Recommended throttle: 2-5 seconds for most workloads.

## Edge Cases

### Progress After Lease Expires

If task times out and another worker claims it, progress updates from the
old worker fail with `LeaseExpired`. The old worker should stop processing.

```python
try:
    await context.report_progress(percent=50)
except LeaseExpiredError:
    # Our lease expired, stop processing
    return None
```

### Concurrent Progress Updates

If two progress updates race:
- One succeeds (CAS wins)
- One fails with 412 (retry or skip)

This is rare since one worker owns the task. Can happen if handler spawns
concurrent operations that both report progress.

### Progress on Completed Task

Progress is cleared when task completes. Final output is the result.

### Progress Percentage > 100

Clamped to 100. Logged as warning.

## Progress in Version History

Progress updates create new task versions:

```bash
$ oq history abc123

Version 1: pending (created)
Version 2: running (claimed)
Version 3: running (progress: 25%, "Processing batch 1")
Version 4: running (progress: 50%, "Processing batch 2")
Version 5: running (progress: 75%, "Processing batch 3")
Version 6: running (progress: 100%, "Finalizing")
Version 7: completed
```

**Note:** Frequent progress updates increase version history size. Use
throttling and consider version history retention policies.

## Metrics

```
oq_progress_updates_total{task_type}
oq_progress_update_latency_seconds{task_type}
```

## UI Integration

Web UI can show progress bar:

```
Task: abc123 (batch_process)
Status: running
[████████████░░░░░░░░░░░░░] 45%
Processing file 450 of 1000
Last updated: 30 seconds ago
```

## Implementation

### Files to Change

- `crates/oq/src/models/task.rs` - Add `Progress` struct, `progress` field
- `crates/oq/src/worker/context.rs` (new) - `TaskContext` with `report_progress()`
- `crates/oq/src/worker/runner.rs` - Pass context to handlers
- `crates/oq/src/python/worker.rs` - Python bindings for context
- `crates/oq/src/cli/status.rs` - Display progress, add `--watch` flag

### Estimated Effort

| Component | Lines |
|-----------|-------|
| Task model | ~25 |
| TaskContext | ~60 |
| Throttling | ~20 |
| Worker integration | ~30 |
| Python bindings | ~40 |
| CLI display + watch | ~50 |
| Tests | ~80 |
| **Total** | ~305 |

## Non-Goals

- **Real-time streaming**: Polling-based, not push
- **Progress guarantees**: Best-effort, can fail
- **Sub-task progress**: Single progress per task
- **Progress validation**: Handler controls values
