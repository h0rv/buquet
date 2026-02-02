# Bug: Worker Claims Tasks Without Registered Handlers

## Status: Fixed

## Priority: High

## Summary

Both the Rust and Python workers were claiming ALL tasks found in the ready index, regardless of whether they had a handler registered for that task type. This causes:
1. Unhandled tasks to be claimed and immediately failed
2. These claims count against `max_tasks`, blocking legitimate work
3. Test flakiness when stale tasks exist in the queue

## Reproduction

```python
import asyncio
from buquet import connect, Worker, WorkerRunOptions, PollingStrategy

async def demo():
    queue = await connect(endpoint='http://localhost:4566', bucket='buquet-dev')

    # Submit a task of type "task_a"
    await queue.submit("task_a", {"data": 1})

    # Create worker that only handles "task_b"
    worker = Worker(queue, "test-worker", queue.all_shards())

    @worker.task("task_b")  # Note: NOT "task_a"
    async def handle(input):
        return {"ok": True}

    # Worker will claim "task_a" even though it can't handle it!
    # This counts against max_tasks
    await worker.run(WorkerRunOptions(max_tasks=1))

    # The "task_a" task is now failed with "No handler registered"
    task = await queue.get("...")
    print(task.status)  # "failed"

asyncio.run(demo())
```

## Expected Behavior

The worker should only claim tasks for which it has registered handlers. The polling loop should:
1. List ready tasks in a shard
2. Filter tasks by registered task types BEFORE claiming
3. Only claim tasks it can actually handle

## Impact

1. **Test Flakiness**: Integration tests fail when stale tasks from previous runs exist in the queue. The worker claims these stale tasks, which count against `max_tasks`, preventing it from processing the actual test task.

2. **Production Issues**: In a multi-worker deployment where different workers handle different task types, a worker might claim and fail tasks meant for other workers.

3. **Resource Waste**: Unnecessary S3 operations for claim/fail cycles on tasks that shouldn't be claimed.

## Root Cause

Both the Rust worker (`crates/buquet/src/worker/runner.rs`) and Python worker (`crates/buquet/src/python/worker.rs`) had the same bug: they claimed tasks before checking if a handler was registered for the task type.

## Fix Applied

The fix centralizes the "check handler before claiming" logic in a new Rust function `try_claim_for_handler` in `crates/buquet/src/worker/claim.rs`. This function is the **single source of truth** for this pattern, used by both Rust and Python workers.

### Core Function: `try_claim_for_handler`

```rust
/// Attempts to claim a task only if a handler exists for its type.
///
/// This function implements the key safety check: before claiming a task,
/// it first fetches the task to determine its type, then checks if a handler
/// is registered for that type. This prevents workers from claiming tasks
/// they cannot handle.
pub async fn try_claim_for_handler<F>(
    queue: &Queue,
    task_id: Uuid,
    worker_id: &str,
    has_handler: F,
) -> Result<ClaimResult, StorageError>
where
    F: Fn(&str) -> bool,
{
    // First, fetch the task to determine its type
    let task_type = match queue.get(task_id).await? {
        Some((task, _etag)) => task.task_type.clone(),
        None => return Ok(ClaimResult::NotFound),
    };

    // Check if we have a handler for this task type
    if !has_handler(&task_type) {
        tracing::debug!(
            task_id = %task_id,
            task_type = %task_type,
            "Skipping task - no handler registered"
        );
        return Ok(ClaimResult::NotAvailable);
    }

    // Now attempt to claim the task
    claim_task(queue, task_id, worker_id).await
}
```

### Rust Worker Usage

```rust
// In crates/buquet/src/worker/runner.rs
async fn try_claim(&mut self, task_id: Uuid) -> Result<Option<(Task, String, Uuid)>, StorageError> {
    let handlers = &self.handlers;
    match try_claim_for_handler(&self.queue, task_id, &self.info.worker_id, |task_type| {
        handlers.has_handler(task_type)
    }).await? {
        ClaimResult::Claimed { task, etag } => { /* ... */ }
        _ => Ok(None),
    }
}
```

### Python Worker Usage

```rust
// In crates/buquet/src/python/worker.rs
// Snapshot registered handler types once per poll cycle
let registered_types: HashSet<String> = {
    let handlers_guard = handlers.read().await;
    handlers_guard.keys().cloned().collect()
};

// Use the canonical try_claim_for_handler function
match try_claim_for_handler(&queue, task_id, &worker_id, |task_type| {
    registered_types.contains(task_type)
}).await {
    // ...
}
```

## Files Modified

- `crates/buquet/src/worker/claim.rs` - Added `try_claim_for_handler` function (source of truth)
- `crates/buquet/src/worker/mod.rs` - Exported `try_claim_for_handler`
- `crates/buquet/src/worker/runner.rs` - Rust worker now uses `try_claim_for_handler`
- `crates/buquet/src/python/worker.rs` - Python worker now uses `try_claim_for_handler`

## Tests Added

- `crates/buquet/python/tests/test_reschedule.py::TestWorkerHandlerFiltering` - Dedicated test class with 3 tests:
  - `test_worker_does_not_claim_unhandled_task_types` - Verifies unhandled tasks remain pending
  - `test_worker_claims_only_handled_task_types` - Verifies selective claiming works
  - `test_multiple_workers_with_different_handlers` - Verifies multi-worker scenarios

## Architecture Note

The Python worker has its own polling loop (rather than using the Rust `Worker::run()`) because Python async handlers require special GIL handling that can't be directly integrated with Rust's async runtime. However, by extracting the handler-check logic into `try_claim_for_handler`, we ensure **Rust is the source of truth** for this critical safety check.
