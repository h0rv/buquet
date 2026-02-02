# Feature: Task Groups (Fan-In Index)

## Status: Proposed

## Overview
Introduce lightweight task groups to support fan-out/fan-in patterns without
heavy polling. Groups are simple S3 projections that track group membership and
completion.

## Philosophy Alignment

| Principle | How This Aligns |
|-----------|-----------------|
| S3 as only dependency | Group data is stored in S3 objects |
| Simple, boring, reliable | Append-only markers + optional manifest |
| Debuggable | Group progress visible via `aws s3 ls` |

## Problem
Fan-in patterns require checking many task objects to determine completion.
Polling each task is expensive and slow for large groups.

## Solution
- Assign a `group_id` to related tasks.
- Write a group manifest when creating the group (`expected_count`).
- Treat `group_id` as an idempotency key for the batch.
- When a task enters a terminal state, write a small completion marker.
- Fan-in checks count completion markers instead of polling each task.

## Storage Layout
```
groups/
  {group_id}.json               # manifest: expected_count, created_at
  {group_id}/
    tasks/{task_id}.json        # optional membership marker (small)
    done/{task_id}.json         # completion marker (terminal state)
```

## Manifest
```json
{
  "group_id": "grp-123",
  "expected_count": 500,
  "created_at": "2026-01-28T12:00:00Z"
}
```

## API Sketch

### Python
```python
# Create a group for a batch
batch = await queue.submit_many(
    [("process_item", {"id": i}) for i in items],
    group_id="grp-123",
)

# Wait for completion
await queue.wait_for_group("grp-123", timeout=timedelta(hours=1))
```

### Rust
```rust
queue.submit_many(tasks)
    .group_id("grp-123")
    .send()
    .await?;

queue.wait_for_group("grp-123", Duration::hours(1)).await?;
```

## Behavior
- Group markers are projections; tasks remain the source of truth.
- Creating a group with an existing `group_id` returns the existing manifest
  (idempotent batch submission).
- If group markers are missing, a repair path can rebuild them by scanning
  task history.
- Completion marker is written when a task reaches a terminal state
  (completed/failed/cancelled/expired).

## Migration / Compatibility
- Group support is opt-in. Existing tasks unaffected.
- Manifests and markers can be safely ignored by older clients.

## Related
- Fan-in utilities in workflows and result chaining
- `docs/features/result-chaining.md`
