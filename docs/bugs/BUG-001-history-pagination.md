# BUG-001: Task history truncates without pagination

Status: Resolved

## Summary
Task history retrieval used `ListObjectVersions` without pagination, silently truncating histories beyond the first page.

## Impact
Breaks the event-sourcing/auditability guarantee for tasks with many versions. Older versions become invisible.

## Example
A task with many retries or future heartbeat-based updates exceeds the first page size; `get_history()` returns a partial timeline.

## Resolution
`list_object_versions` now paginates with key/version markers until completion and errors if the response is truncated without markers.

## Follow-ups
None.
