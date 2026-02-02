# BUG-012: Scheduler records last_run before submit and can skip runs

Status: Resolved

## Summary
`run_scheduler` updates `schedules/{id}/last_run.json` via CAS before submitting the task. If `queue.submit` fails, the schedule is still marked as run and will be skipped until the next cron occurrence.

## Impact
Scheduled tasks can be missed permanently for that interval. This violates expected at-least-once behavior for recurring schedules and can cause lost work.

## Example
- Schedule is due at 09:00.
- Scheduler CAS-updates `last_run` at 09:00:05.
- `queue.submit` fails (transient S3 error).
- Next tick sees `last_run_at` set and `is_due` returns false, so the 09:00 run is skipped.

## Location
- `crates/buquet/src/main.rs:670-713`
- `crates/buquet/src/queue/schedule.rs:116-129`

## Proposed Fix
Use a two-step flow that only advances `last_run` after a successful submit:
- Option A: Create a "claim" record with CAS, submit with an idempotency key derived from `(schedule_id, scheduled_time)`, then update `last_run` only on success.
- Option B: Store a "pending_run_at" in `last_run` and retry submission until success, then finalize `last_run_at`.

Add a regression test that forces submit failure and verifies the run is retried.

## Resolution
- Scheduler now submits first using an idempotency key based on `(schedule_id, due_at)`.
- `last_run` is updated only after a successful submit, and `last_task_id` is recorded.
