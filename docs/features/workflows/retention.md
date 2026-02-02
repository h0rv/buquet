# Feature: Workflow Retention & Pruning

## Status: Proposed

## Overview
Define how workflow artifacts (step results, signals) are retained and pruned to
control storage cost while keeping core state intact.

## Philosophy Alignment

| Principle | How This Aligns |
|-----------|-----------------|
| S3 as only dependency | Uses S3 lifecycle policies |
| Simple, boring, reliable | Retention is explicit, opt-in |
| Debuggable | State remains inspectable |

## Problem
Workflow steps and signals can accumulate large numbers of objects, increasing
storage cost and list latency.

## Solution
- Keep `workflow/{id}/state.json` indefinitely.
- Allow retention policies for:
  - `workflow/{id}/steps/*`
  - `workflow/{id}/signals/*`
- Provide optional `archive.json` summary when pruning.

## API Sketch

### CLI
```bash
buquet workflow prune --older-than 30d
```

### Config
```toml
[workflow]
retention_days = 30
keep_state = true
```

## Behavior
- **Pruning only applies to terminal workflows** (`completed`, `failed`, `cancelled`).
  Active workflows (`running`, `paused`, `waiting_signal`, `compensating`) are
  never pruned, even if they exceed the retention period.
- Pruning only removes step and signal objects.
- State object remains the source of truth.
- Optional archive summary is written before deletion.

### Safety Check
Before deleting any artifact, the pruner verifies the workflow is terminal:

```python
async def prune_workflow(wf_id, retention_days):
    state = await get_workflow_state(wf_id)

    # Guard: never prune active workflows
    if state["status"] not in ["completed", "failed", "cancelled"]:
        log.warning(f"Skipping active workflow: {wf_id} ({state['status']})")
        return

    if state["updated_at"] < await queue.now() - timedelta(days=retention_days):
        await delete_step_artifacts(wf_id)
        await delete_signal_artifacts(wf_id)
```

This prevents breaking `wait_for_signal` or step inspection on running workflows.

## Migration / Compatibility
- Safe to enable after runs complete.

## Related
- `../version-history-retention.md`
- `index.md` (main workflow spec)
