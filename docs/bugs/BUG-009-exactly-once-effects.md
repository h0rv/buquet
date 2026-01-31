# BUG-009: Exactly-once effects are not achievable

Status: Known limitation

## Summary
The system is at-least-once by design. Timeouts and crashes can lead to duplicate executions even if claims are exactly-once.

## Impact
External side effects can happen more than once unless handlers are idempotent.

## Example
Worker A completes side effects then crashes before marking the task complete; monitor requeues and Worker B repeats the work.

## Proposed Fix
Provide idempotency keys or an outbox pattern; at minimum, document this clearly in the API and README.

## Notes
Workflow specs should not claim exactly-once effects as a guarantee; it is
best-effort and depends on downstream idempotency.
