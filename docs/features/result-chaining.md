# Feature: Result Chaining / Fan-Out Patterns

## Status: Proposed

## Overview

Patterns and helpers for common workflow scenarios: chaining tasks (A → B → C),
fan-out/fan-in (split work, aggregate results), and parent-child relationships.
Not a workflow engine - just simple primitives that compose.

## Philosophy Alignment

| Principle | How This Aligns |
|-----------|-----------------|
| S3 as only dependency | No orchestrator, tasks reference each other via IDs |
| Simple, boring, reliable | Patterns, not frameworks; users control flow |
| No coordination services | Polling-based completion checks |
| Crash-safe | Parent/child IDs persisted in tasks |
| Debuggable | `oq get` shows relationships, easy to trace |

## Problem

- Task A's output needed as Task B's input
- Split work across many tasks, then aggregate
- Track which tasks belong together
- Currently: manual ID passing, no helpers

## Non-Goals (What This Is NOT)

- **Not a workflow engine**: No DAG definitions, no orchestration
- **Not Temporal/Airflow**: No visual workflow builder
- **Not automatic**: User controls when to spawn/wait
- **Not transactional**: No rollback on failure

## Core Pattern: Explicit Chaining

Tasks chain by including output as next task's input:

```python
@worker.task("step_a")
async def step_a(input, context):
    result = await do_work(input)

    # Chain to next step
    await context.queue.submit(
        "step_b",
        {"previous_result": result, "original_input": input},
    )

    return result

@worker.task("step_b")
async def step_b(input, context):
    previous = input["previous_result"]
    return await continue_work(previous)
```

**This already works today.** The feature adds conveniences and tracking.

## New Field: Parent/Child Tracking

Add optional `parent_task_id` for traceability:

```json
{
  "id": "child-123",
  "task_type": "process_item",
  "parent_task_id": "parent-456",
  "input": {"item": "..."},
  ...
}
```

```json
{
  "id": "parent-456",
  "task_type": "batch_process",
  "child_task_ids": ["child-123", "child-124", "child-125"],
  "input": {"items": ["...", "...", "..."]},
  ...
}
```

| Field | Type | Description |
|-------|------|-------------|
| `parent_task_id` | `Option<Uuid>` | Task that spawned this one |
| `child_task_ids` | `Vec<Uuid>` | Tasks spawned by this one |

## API

### Python

#### Spawn Child Tasks

```python
@worker.task("batch_process")
async def batch_process(input, context):
    items = input["items"]

    # Spawn child tasks
    child_ids = []
    for item in items:
        child = await context.spawn(
            "process_item",
            {"item": item},
        )
        child_ids.append(child.id)

    # Store child IDs for later aggregation
    return {"child_task_ids": child_ids, "total": len(items)}
```

`context.spawn()` is sugar for `queue.submit()` with `parent_task_id` set.

#### Wait for Tasks

```python
from oq import wait_for_all, wait_for_any, TaskStatus

# Wait for all to complete (polling)
results = await wait_for_all(queue, child_ids, timeout=300)
# Returns: [Task, Task, Task, ...]

# Wait for any to complete
first_done = await wait_for_any(queue, child_ids, timeout=60)

# Wait with status filter
completed = await wait_for_all(
    queue,
    child_ids,
    statuses=[TaskStatus.Completed, TaskStatus.Failed],
)
```

#### Aggregation Pattern

```python
@worker.task("aggregate_results")
async def aggregate(input, context):
    child_ids = input["child_task_ids"]

    # Wait for all children
    children = await wait_for_all(context.queue, child_ids)

    # Aggregate results
    total = sum(c.output["count"] for c in children if c.status == TaskStatus.Completed)
    failed = [c.id for c in children if c.status == TaskStatus.Failed]

    return {
        "total": total,
        "failed_count": len(failed),
        "failed_task_ids": failed,
    }
```

### Submit with Continuation

Schedule a follow-up task when this one completes:

```python
# Submit with continuation
task = await queue.submit(
    "fetch_data",
    {"url": "..."},
    on_complete="process_data",  # Task type to run on completion
)

# The system will automatically submit process_data with:
# {"previous_task_id": task.id, "previous_output": <output>}
```

**Implementation note:** This is sugar - the worker handles it, not magic.

```python
# Worker internally does:
@worker.task("fetch_data")
async def handle_with_continuation(input, context):
    result = await actual_handler(input)

    if context.task.on_complete:
        await context.spawn(
            context.task.on_complete,
            {"previous_task_id": context.task.id, "previous_output": result},
        )

    return result
```

### Rust

```rust
// Spawn child
let child = ctx.spawn("process_item")
    .input(json!({"item": item}))
    .send()
    .await?;

// Wait for tasks
let results = queue.wait_for_all(&child_ids, WaitOptions::default()).await?;

// Wait with timeout
let results = queue.wait_for_all(&child_ids, WaitOptions {
    timeout: Some(Duration::from_secs(300)),
    poll_interval: Duration::from_secs(5),
    ..Default::default()
}).await?;
```

### CLI

```bash
# View task relationships
oq get abc123 --json | jq '{parent: .parent_task_id, children: .child_task_ids}'

# List children of a task
oq list --parent abc123

# Trace task lineage
oq trace abc123
# Output:
#   batch_process (abc123) [completed]
#   ├── process_item (def456) [completed]
#   ├── process_item (ghi789) [completed]
#   └── process_item (jkl012) [failed]
#       └── (retried as mno345) [completed]
```

## Common Patterns

### Pattern 1: Fan-Out / Fan-In

```python
@worker.task("fan_out")
async def fan_out(input, context):
    items = input["items"]

    # Fan out
    children = []
    for item in items:
        child = await context.spawn("process_item", {"item": item})
        children.append(child.id)

    # Schedule aggregation (runs after children complete)
    await context.spawn(
        "fan_in",
        {"child_task_ids": children},
    )

    return {"spawned": len(children)}

@worker.task("process_item")
async def process_item(input, context):
    return {"count": process(input["item"])}

@worker.task("fan_in")
async def fan_in(input, context):
    children = await wait_for_all(context.queue, input["child_task_ids"])
    return {"total": sum(c.output["count"] for c in children)}
```

### Pattern 2: Pipeline (A → B → C)

```python
@worker.task("step_a")
async def step_a(input, context):
    result = await do_a(input)
    await context.spawn("step_b", {"data": result})
    return result

@worker.task("step_b")
async def step_b(input, context):
    result = await do_b(input["data"])
    await context.spawn("step_c", {"data": result})
    return result

@worker.task("step_c")
async def step_c(input, context):
    return await do_c(input["data"])
```

### Pattern 3: Saga (With Compensation)

```python
@worker.task("saga_start")
async def saga(input, context):
    try:
        # Step 1
        charge = await context.spawn_and_wait("charge_payment", {"amount": 100})

        # Step 2
        reserve = await context.spawn_and_wait("reserve_inventory", {"item": "xyz"})

        # Step 3
        ship = await context.spawn_and_wait("ship_order", {"to": input["address"]})

        return {"success": True}

    except TaskFailedError as e:
        # Compensate
        if "charge" in locals():
            await context.spawn("refund_payment", {"charge_id": charge.output["id"]})
        if "reserve" in locals():
            await context.spawn("release_inventory", {"reservation_id": reserve.output["id"]})

        return {"success": False, "error": str(e)}
```

### Pattern 4: Scatter-Gather with Timeout

```python
@worker.task("scatter_gather")
async def scatter_gather(input, context):
    # Scatter
    children = []
    for endpoint in input["endpoints"]:
        child = await context.spawn("fetch_endpoint", {"url": endpoint})
        children.append(child.id)

    # Gather with timeout (some may not complete)
    try:
        results = await wait_for_all(context.queue, children, timeout=30)
    except TimeoutError:
        # Get whatever completed
        tasks = [await context.queue.get(id) for id in children]
        results = [t for t in tasks if t.status == TaskStatus.Completed]

    return {
        "completed": len(results),
        "total": len(children),
        "data": [r.output for r in results],
    }
```

## Wait Implementation

```python
async def wait_for_all(
    queue: Queue,
    task_ids: list[str],
    timeout: float = None,
    poll_interval: float = 5.0,
    statuses: list[TaskStatus] = None,
) -> list[Task]:
    """Wait for all tasks to reach terminal status."""

    if statuses is None:
        statuses = [TaskStatus.Completed, TaskStatus.Failed, TaskStatus.Cancelled, TaskStatus.Expired]

    start = time.time()
    pending = set(task_ids)
    results = {}

    while pending:
        if timeout and (time.time() - start) > timeout:
            raise TimeoutError(f"{len(pending)} tasks still pending")

        for task_id in list(pending):
            task = await queue.get(task_id)
            if task.status in statuses:
                pending.remove(task_id)
                results[task_id] = task

        if pending:
            await asyncio.sleep(poll_interval)

    # Return in original order
    return [results[id] for id in task_ids]
```

## Task Graph Visualization

```bash
$ oq trace batch-123 --format tree

batch_process (batch-123) [completed, 2m 30s]
├── process_item (item-001) [completed, 45s]
├── process_item (item-002) [completed, 52s]
├── process_item (item-003) [failed, 30s]
│   └── error: Connection timeout
├── process_item (item-004) [completed, 48s]
└── aggregate (agg-456) [completed, 5s]
    └── result: {"total": 150, "failed": 1}
```

```bash
$ oq trace batch-123 --format json
{
  "id": "batch-123",
  "task_type": "batch_process",
  "status": "completed",
  "children": [
    {"id": "item-001", "status": "completed", "duration_ms": 45000},
    ...
  ]
}
```

## Edge Cases

### Child Outlives Parent

Children continue even if parent fails. No automatic cancellation.

To cancel children on parent failure:
```python
@worker.task("parent")
async def parent(input, context):
    children = []
    try:
        for item in input["items"]:
            child = await context.spawn("child", {"item": item})
            children.append(child.id)
        # ...
    except Exception as e:
        # Cancel children on failure
        for child_id in children:
            await context.queue.cancel(child_id).ok()
        raise
```

### Orphaned Children

If parent task is deleted, children remain with `parent_task_id` pointing
to non-existent task. This is fine - children are independent.

### Circular Dependencies

Not prevented. User's responsibility. Could add cycle detection if needed.

### Deep Nesting

No limit on parent→child→grandchild depth. `oq trace` handles recursion.

## Metrics

```
oq_child_tasks_spawned_total{parent_type, child_type}
oq_wait_duration_seconds{task_type}
oq_wait_timeouts_total{task_type}
```

## Implementation

### Files to Change

- `crates/oq/src/models/task.rs` - Add `parent_task_id`, `child_task_ids`, `on_complete`
- `crates/oq/src/queue/ops.rs` - Track parent/child in submit
- `crates/oq/src/queue/wait.rs` (new) - `wait_for_all`, `wait_for_any`
- `crates/oq/src/worker/context.rs` - Add `spawn()`, `spawn_and_wait()`
- `crates/oq/src/python/queue.rs` - Python bindings
- `crates/oq/src/cli/trace.rs` (new) - Task graph visualization

### Estimated Effort

| Component | Lines |
|-----------|-------|
| Task model | ~20 |
| Parent/child tracking | ~40 |
| Wait helpers | ~80 |
| Context spawn | ~30 |
| CLI trace | ~60 |
| Python bindings | ~50 |
| Tests | ~100 |
| **Total** | ~380 |

## Non-Goals

- **Workflow definitions**: No YAML/DSL, code is the definition
- **Automatic retries of children**: Parent controls retry logic
- **Cross-queue dependencies**: Single queue only
- **Distributed transactions**: At-least-once, user handles idempotency
