# ISSUE-005: No load/chaos testing coverage

Status: Addressed

## Summary
The current test suite focuses on unit and integration correctness, but lacks
system-level load and failure tests. This leaves scale behavior and recovery
paths under-validated.

## Impact
- Unknown performance characteristics under LIST/GET throttling.
- Recovery paths (timeouts, monitor sweeps) are not stress-tested.
- Risk of hidden regressions as scale increases.

## Mitigations
- Add a minimal load test harness (N producers, M workers, randomized delays).
- Add chaos tests for worker crashes and slow object store responses.
- Track LIST/GET/PUT counts and alert on regression.

## Notes
This is not a correctness bug but an assurance gap.
