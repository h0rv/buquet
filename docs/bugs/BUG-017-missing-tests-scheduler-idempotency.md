# BUG-017: Missing regression tests for scheduler and idempotency edge cases

Status: Resolved

## Summary
Key edge cases are untested: scheduler submit failure should not advance `last_run`, `last_task_id` should be updated correctly, and idempotency records that reference missing tasks should allow recovery.

## Impact
Regressions in scheduling and idempotency behavior can ship silently, leading to missed runs or blocked submissions in production.

## Example
There are integration tests for scheduling and idempotency, but none cover these failure paths.

## Location
- `crates/buquet/tests/integration/scheduling.rs`
- `crates/buquet/tests/integration/idempotency.rs`

## Proposed Fix
Add integration tests (or unit tests with a mock S3 client) that cover:
- Scheduler submit failure does not advance `last_run` and retries on next tick.
- `last_task_id` reflects the actual submitted task id.
- Idempotency record with missing task allows a new submit.

## Resolution
- Added integration tests for scheduler tick behavior (submit failure + last_task_id update).
- Added an integration test for missing-task idempotency recovery.
