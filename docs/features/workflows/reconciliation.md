# Feature: Workflow Sweeper (Stall Recovery)

## Status: Implemented (sweeper library)

## Overview
The workflow sweeper (also called reconciliation) detects stalled workflows
that are stuck in `running` or `waiting_signal` status but have no active
orchestrator task.

> **Note:** This is the detailed spec for the sweeper described in `index.md`.
> The CLI command is `oq workflow sweeper`.

## Philosophy Alignment

| Principle | How This Aligns |
|-----------|-----------------|
| S3 as only dependency | Uses workflow state objects |
| Simple, boring, reliable | Periodic scan + CAS update |
| Recoverable | Matches existing monitor pattern |

## Problem
If the orchestrator task is lost or never scheduled, workflows can stall
indefinitely without explicit recovery.

## Solution
The sweeper periodically:
1. Lists `workflow/*/state.json`
2. Finds workflows in active states (`running`, `waiting_signal`, `compensating`)
3. Checks if `orchestrator_task_id` references a live task
4. If task is missing/completed/failed, reports the workflow as needing recovery

Recovery (submitting a new orchestrator task + CAS update) is performed by the
caller, since the sweeper does not depend on the oq queue directly.

## Detection Logic

```python
async def sweep_workflows():
    for wf_state in list_workflow_states():
        if wf_state["status"] not in ["running", "waiting_signal", "compensating"]:
            continue  # Terminal or paused - skip

        task = await queue.get(wf_state["orchestrator_task_id"])

        needs_recovery = (
            task is None or
            task.status in ["completed", "failed", "cancelled"] or
            (task.status == "running" and task.lease_expires_at < await queue.now())
        )

        if needs_recovery:
            await recover_workflow(wf_state["id"])
```

## Recovery Action

```python
async def recover_workflow(wf_id):
    state, etag = await get_workflow_state(wf_id)

    # Submit new orchestrator task with idempotency
    task = await queue.submit(
        "workflow.orchestrate",
        {"workflow_id": wf_id},
        idempotency_key=f"{wf_id}:orchestrate:{state['updated_at']}",
    )

    # Update state with new orchestrator task ID
    state["orchestrator_task_id"] = task.id
    await update_workflow_state(wf_id, state, etag)
```

## CLI

**Not yet implemented.** Use the `WorkflowSweeper` library from oq-workflows
in Rust or Python and trigger recovery in your own process.

```bash
# Run sweeper continuously
oq workflow sweeper --interval 300  # Check every 5 minutes

# One-shot sweep
oq workflow sweeper --once

# With custom stale threshold
oq workflow sweeper --stale-after 600  # Consider stale after 10 min
```

## Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `--interval` | 300 | Seconds between sweeps |
| `--stale-after` | 600 | Seconds before considering a workflow stale |
| `--once` | false | Run single sweep and exit |

## Behavior
- Uses idempotency key `{wf_id}:orchestrate:{updated_at}` to avoid duplicate
  orchestration tasks.
- Sweeper is idempotent - multiple instances are safe.
- Only modifies `orchestrator_task_id` in workflow state.

## Migration / Compatibility
- Optional and safe to run alongside normal workflows.
- Existing workflows without `orchestrator_task_id` are skipped.

## Related
- `index.md` (main workflow spec, Orchestrator Recovery section)
