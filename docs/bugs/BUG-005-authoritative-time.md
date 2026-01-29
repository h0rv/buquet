# BUG-005: Local time dependence in scheduling/leases

Status: Resolved

## Summary
Local time dependence has been removed from correctness-critical scheduling and timeout logic by introducing an authoritative S3-backed clock and time-aware helpers.

## Impact
Clock skew can no longer cause early claims or premature timeouts. All scheduling and lease decisions use S3 time.

## Example
Workers and monitors now compare `available_at` and `lease_expires_at` against S3 time, avoiding local clock skew.

## Time/clock considerations
Local clocks can drift in distributed systems. With skewed clocks, workers can:
- claim tasks early
- time out tasks prematurely

This is now addressed by a dedicated S3-backed clock that is cached briefly and used throughout the system.

## Proposed Fix
Implemented. See "Resolution" below.

## Resolution
- Added S3-backed authoritative clock: `S3Client::now()` and `Queue::now()`.
- All time comparisons now require an explicit `now` from S3.
- Task helpers renamed to `*_at` variants in Rust and Python (no local-time helpers).
- Python API: `Queue.now()` returns RFC3339; use `Task.is_available_at(now)` etc.

Example (Python):
```python
now = await queue.now()
if task.is_available_at(now):
    ...
```

Remaining work for bucket filtering has been split into a new issue: `BUG-010-ready-index-bucket-filtering.md`.
