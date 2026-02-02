# BUG-013: Idempotency record pointing to missing task blocks new submit

Status: Resolved

## Summary
When an idempotency record exists but the referenced task is missing, `check_idempotency` returns `NotFound` and the submit fails. The record remains until TTL expiry, effectively blocking all submissions with that idempotency key.

## Impact
Producers can get stuck for up to the idempotency TTL even though no task exists. This can drop work and break retry loops.

## Example
- A task is created with an idempotency key.
- The task object is deleted or lost.
- A retry with the same idempotency key returns `NotFound` and does not create a new task.
- All subsequent retries fail until TTL expires.

## Location
- `crates/buquet/src/queue/idempotency.rs:390-400`
- `crates/buquet/src/queue/ops.rs:128-205`

## Proposed Fix
Treat "record exists but task missing" as an expired record:
- Attempt to overwrite the idempotency record with CAS (If-Match) and proceed.
- Or delete the record and retry the create path.

Add a regression test that deletes the task after record creation and verifies a new submit succeeds.

## Resolution
- Missing-task idempotency records are now CAS-overwritten and treated as expired.
- Added an integration test to cover the missing-task recovery path.
