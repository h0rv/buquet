# BUG-011: No lease extension for long-running tasks

Status: Resolved

## Summary
Tasks have a fixed `lease_expires_at` set at claim time. There is no mechanism
for a worker to extend the lease while work is still in progress.

## Impact
Long-running tasks can be re-queued by the timeout monitor even though the
worker is still actively processing them. This leads to duplicate execution and
wasted work.

## Example
- Task timeout is 5 minutes.
- Worker begins a job that takes 20 minutes.
- After 5 minutes, the monitor re-queues the task.
- A second worker starts executing the same task.

## Fix Applied
Lease extension is now supported:
- Rust and Python APIs allow extending `lease_expires_at` while a task runs.
- Updates use CAS on the task object and authoritative S3 time.
- Integration tests cover the lease extension flow.

## Notes
See `docs/features/task-lease-extension.md` and
`docs/features/worker-lease-renewal.md`.

## Related
- Proposed feature: `docs/features/task-lease-extension.md`
