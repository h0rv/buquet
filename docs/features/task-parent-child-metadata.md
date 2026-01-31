# Feature: Parent/Child Task Metadata

## Status: Proposed

## Overview
Add optional metadata fields that link tasks to a parent task or workflow. This
improves traceability and enables simple cancellation and debugging patterns
without introducing new indexes or infrastructure.

## Philosophy Alignment

| Principle | How This Aligns |
|-----------|-----------------|
| Simple, boring, reliable | Just fields on the task object |
| Debuggable | Relationships visible via S3 objects |
| No new services | No new indexes or coordination |

## Problem
- It is hard to trace why a task exists (which workflow or parent spawned it).
- Cancellation or auditing across child tasks requires out-of-band tracking.
- Workflow tooling needs a standard way to link step tasks to a workflow run.

## Solution
Add optional metadata fields to `Task`:
- `parent_task_id: Option<Uuid>`
- `root_task_id: Option<Uuid>`
- `workflow_id: Option<String>`
- `workflow_step: Option<String>`

These fields are purely informational; they do not change scheduling semantics.

## Task Fields
```json
{
  "id": "abc123",
  "task_type": "workflow.step:order_fulfillment:charge_card",
  "parent_task_id": "def456",
  "root_task_id": "def456",
  "workflow_id": "wf-order-123",
  "workflow_step": "charge_card",
  "status": "running",
  ...
}
```

## API Sketch

### Rust
```rust
queue.submit("child_task")
    .input(json!({"x": 1}))
    .parent_task_id(parent_id)
    .root_task_id(root_id)
    .workflow_id("wf-123")
    .workflow_step("charge_card")
    .send()
    .await?;
```

### Python
```python
await queue.submit(
    "child_task",
    {"x": 1},
    parent_task_id=parent_id,
    root_task_id=root_id,
    workflow_id="wf-123",
    workflow_step="charge_card",
)
```

## Behavior
- Fields are stored with the task and preserved across state transitions.
- No new indexes are required.
- Clients can filter or aggregate by these fields using list + get.

## Migration / Compatibility
- All fields are optional. Existing tasks remain valid.

## Related
- `workflows/index.md`
