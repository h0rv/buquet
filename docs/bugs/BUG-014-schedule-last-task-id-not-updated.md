# BUG-014: Schedule last_task_id never updated after submit

Status: Resolved

## Summary
After a schedule run is claimed, the code tries to update `last_run.json` with the actual task id using `update_last_run(..., None)`. That uses `If-None-Match`, so it always fails once the record exists. As a result, `last_task_id` stays as a placeholder UUID.

## Impact
Schedule inspection and automation that relies on `last_task_id` is incorrect. Observability and admin tooling cannot link a schedule run to the real task.

## Example
- Scheduler creates `last_run.json` with a placeholder UUID.
- Task submission succeeds.
- Post-submit update uses `If-None-Match` and is rejected.
- `last_task_id` never reflects the real task id.

## Location
- `crates/oq/src/main.rs:701-706`
- `crates/oq/src/queue/schedule.rs:236-249`

## Proposed Fix
Update the record with `If-Match` using the ETag from the initial claim, or re-read the record and update with the new ETag. Alternative: generate a task id before submit and include it in the initial `last_run` update.

Add a regression test that verifies `last_task_id` matches the created task id.

## Resolution
- Scheduler now writes `last_task_id` after a successful submit (no placeholder).
