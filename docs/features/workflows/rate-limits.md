# Feature: Workflow Concurrency Limits

## Status: Proposed

## Overview
Allow workflows to cap the number of in-flight steps they schedule at once.
This prevents runaway fan-out and reduces S3 pressure.

## Philosophy Alignment

| Principle | How This Aligns |
|-----------|-----------------|
| Simple, boring, reliable | Orchestrator gating only |
| No extra infra | Reuses workflow state |
| Explicit > implicit | Limits are opt-in per workflow |

## Problem
Large DAGs can schedule hundreds of tasks at once, causing bursty S3 load and
slower overall processing.

## Solution
Add optional concurrency settings on workflow definitions:
- `max_concurrent_steps` (hard cap on running steps)
- `max_steps_per_tick` (cap per orchestration cycle)

The orchestrator schedules only as many steps as allowed.

## Workflow Definition Fields
```json
{
  "type": "order_fulfillment",
  "max_concurrent_steps": 10,
  "max_steps_per_tick": 5
}
```

## Behavior
- Orchestrator tracks `running_steps` in workflow state.
- New steps are scheduled only when `len(running_steps) < max_concurrent_steps`.
- Limits apply per workflow run, not globally.

### Running Steps Recomputation
The `running_steps` count is **recomputed on each orchestrator tick**, not
maintained incrementally. This ensures correctness after retries, lease expiry,
or orchestrator restarts.

```python
async def compute_running_steps(wf_id, state):
    running = []
    for step_name, step_state in state["steps"].items():
        if step_state["status"] == "running":
            # Verify the step task is actually running
            task = await queue.get(step_state["task_id"])
            if task and task.status == "Running":
                running.append(step_name)
            else:
                # Task completed/failed/expired - update step state
                await reconcile_step_state(wf_id, step_name, task)
    return running
```

This avoids:
- **Deadlock:** If a step task fails without updating workflow state, the count
  stays inflated and blocks new steps. Recomputation clears stale entries.
- **Underutilization:** If a step completes but state update fails, the count
  stays too high. Recomputation finds the actual running count.

### Orchestrator Restart Recovery
After an orchestrator restart, the first tick recomputes `running_steps` from
step states and task statuses. This is idempotent and self-healing.

## Migration / Compatibility
- Defaults to unlimited (current behavior).

## Related
- `../rate-limiting.md`
- `index.md` (main workflow spec)
