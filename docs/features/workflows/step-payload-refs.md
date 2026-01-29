# Feature: Workflow Step Payload References by Default

## Status: Proposed

## Overview
Default workflow step inputs/outputs to payload references to keep workflow
state small and reduce rewrite costs.

## Philosophy Alignment

| Principle | How This Aligns |
|-----------|-----------------|
| Simple, boring, reliable | Reuses existing payload refs |
| S3 as only dependency | Payloads live in S3 |
| Debuggable | Payload refs are inspectable keys |

## Problem
Workflow state objects can grow large when step inputs/outputs are embedded,
leading to expensive CAS updates and large S3 payloads.

## Solution
- For workflow step tasks, store large inputs/outputs as payload refs by default.
- Keep only a small reference in the workflow state.

## Storage Layout
```
payloads/{task_id}/input.json
payloads/{task_id}/output.json
```

## Step Task Identification
Workflow step tasks are identified by their task type prefix:
- Pattern: `workflow.step:{workflow_type}:{step_name}`
- Example: `workflow.step:order_fulfillment:charge_card`

The orchestrator and workers use this prefix to:
1. Recognize step tasks vs regular tasks
2. Apply workflow-specific payload ref thresholds
3. Route step results back to workflow state

### CLI Dereferencing
Non-workflow clients (CLI, dashboards) automatically dereference payload refs:

```bash
# Shows dereferenced output, not the ref
oq status <task_id>

# Explicit ref handling
oq status <task_id> --raw  # Shows {"$ref": "payloads/..."}
oq payload get <task_id> output  # Fetches payload directly
```

## API Sketch

### Python
```python
@wf.step("process")
async def process(ctx):
    data = await ctx.input()  # loads payload from ref if needed
    result = await do_work(data)
    return result  # stored as payload ref if exceeds threshold
```

## Behavior
- Threshold-based: use refs when payload exceeds N KB (default: 32KB).
- Threshold is configurable per queue or workflow.
- Backward compatible: inline payloads still supported.
- Step tasks use the same payload ref mechanism as regular tasks;
  the only difference is the task type prefix for identification.

## Related
- `../payload-references.md`
- `index.md` (main workflow spec)
