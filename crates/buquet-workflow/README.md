# buquet-workflow

Durable workflow orchestration built on buquet. Same philosophy: object storage only, no databases.

## What It Does

Coordinates multi-step processes with:
- DAG-based step dependencies
- Durable state (survives crashes)
- Signals for external events
- Compensation on failure (sagas)

Each step is just a buquet task. The orchestrator reads workflow state, figures out what's next, and submits child tasks.

## Architecture

```
workflow/{id}/state.json              # workflow state (CAS updates)
workflow/{id}/steps/{name}.json       # step results (immutable)
workflow/{id}/signals/{name}/         # signal inbox
```

## Usage

```python
from buquet-workflow import Workflow, WorkflowClient

wf = Workflow("order_fulfillment")

@wf.step("validate")
async def validate(ctx):
    return {"valid": True}

@wf.step("charge", depends_on=["validate"])
async def charge(ctx):
    return {"charged": True}

@wf.step("ship", depends_on=["charge"])
async def ship(ctx):
    return {"shipped": True}

# Start a workflow
client = WorkflowClient(queue)
run = await client.start("order_fulfillment", {"order_id": "123"})

# Wait for it
result = await client.wait(run.id)
```

### Signals

```python
@wf.step("wait_approval")
async def wait(ctx):
    signal = await ctx.wait_for_signal("approval", timeout=timedelta(days=7))
    return {"approved_by": signal["approver"]}

# External system sends signal
await client.signal(wf_id, "approval", {"approver": "manager@example.com"})
```

## Guarantees

**Does guarantee:**
- Durable state in S3
- At-least-once step execution
- Crash recovery (sweeper detects stalled workflows)
- Steps respect dependencies

**Does not guarantee:**
- Exactly-once (steps may run multiple times — use idempotency keys)
- Sub-second latency (expect 1-5 seconds per step)
- Deterministic replay (no event sourcing)

## When to Use

Good for batch pipelines, order flows, ETL, onboarding — anything that can tolerate seconds of latency and doesn't need Temporal's replay guarantees.

Not for real-time or interactive workflows.

## Docs

See [docs/features/workflows/](../../docs/features/workflows/) for the full spec.
