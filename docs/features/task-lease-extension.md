# Feature: Task Lease Extension (Long-Running Tasks)

## Status: Implemented

## Overview
Allow workers to extend `lease_expires_at` for in-progress tasks, preventing
premature re-queue by the timeout monitor.

## Problem
Tasks have a fixed lease derived from the initial timeout. Long-running tasks
can be retried while still executing, causing duplicate work.

## Solution
Provide a lease extension API:
- Worker periodically refreshes the task lease while running
- Extension is a CAS update on the task object (If-Match)
- Uses authoritative S3 time for consistency

## API Sketch (Rust)
```rust
worker.extend_lease(&task, &etag, Duration::seconds(60)).await?;
```

## API Sketch (Python)
```python
await task.extend_lease(additional_secs=60)
```

## Behavior
- If the CAS fails, worker should stop the task (another worker took it).
- Extension interval should be < monitor timeout threshold.

## Related Issues
- `docs/bugs/BUG-011-long-running-tasks-no-lease-extension.md`
