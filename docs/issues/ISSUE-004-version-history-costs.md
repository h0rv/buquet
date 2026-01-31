# ISSUE-004: Version history cost growth

Status: Open design issue

## Summary
Bucket versioning is required for audit history, but each state transition
creates a new version of the task object. For high-volume queues, storage cost
and `get_history()` latency can grow quickly.

## Impact
- Storage costs increase with retries and long-running queues.
- `get_history()` becomes slower and more expensive for frequently updated tasks.

## Mitigations
- Document lifecycle policies (expire or transition old versions).
- Provide configuration guidance for retaining only N days of history.
- Consider optional history compaction or summary metadata.

## Related Work
- Proposed feature: `docs/features/version-history-retention.md`
