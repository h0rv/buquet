# ISSUE-001: At-least-once effects are fundamental

Status: Known limitation

## Summary
Even with CAS-based claims, the system cannot guarantee exactly-once **effects**.
A worker can perform a side effect and crash before writing task completion, and
another worker will repeat the task after a timeout.

## Impact
External side effects (charges, emails, writes) can happen more than once unless
handlers are idempotent or use an outbox-style pattern.

## Why This Is Fundamental
S3 provides atomic object updates, not atomic interaction with external systems.
Exactly-once effects would require transactional guarantees across S3 and the
side-effect system, which are outside buquet's scope.

## Mitigations
- Document idempotency expectations in all APIs and examples.
- Provide optional **idempotency keys** or an outbox helper (see `docs/features/future.md`).
- Encourage users to make handlers idempotent or to persist side effects
  separately with dedupe checks.

## References
- `docs/bugs/BUG-009-exactly-once-effects.md`
