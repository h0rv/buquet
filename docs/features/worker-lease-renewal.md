# Feature: Worker Lease Auto-Renewal

## Status: Proposed

## Overview
Automatically extend task leases while a handler is still running to prevent
premature re-queue for long-running work.

## Philosophy Alignment

| Principle | How This Aligns |
|-----------|-----------------|
| Simple, boring, reliable | Periodic CAS updates on task object |
| S3 as only dependency | Uses existing lease fields |
| Recoverable | Fails safe if CAS fails |

## Problem
Even with a lease extension API, many handlers will forget to renew leases,
leading to duplicates for long-running tasks.

## Solution
Add optional worker-level auto-renewal:
- Worker starts a renewal loop when a task is claimed.
- Loop extends `lease_expires_at` every `renew_interval` seconds.
- Loop stops when task completes or on CAS failure.

## API Sketch

### Rust
```rust
WorkerOptions {
    lease_renewal: Some(LeaseRenewalOptions {
        interval_secs: 30,
        extension_secs: 60,
    }),
}
```

### Python
```python
worker = Worker(queue, "w1", queue.all_shards(), lease_renewal={
    "interval_secs": 30,
    "extension_secs": 60,
})
```

## Behavior
- If CAS fails, the worker aborts the task (another worker owns it).
- Renewal uses authoritative S3 time to compute new expiry.
- Opt-in to preserve current behavior.

## Related
- `docs/features/task-lease-extension.md`
- `docs/bugs/BUG-011-long-running-tasks-no-lease-extension.md`
