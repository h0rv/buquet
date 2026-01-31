# Feature: qow (Durable Orchestration Layer)

## Status: Implemented (Separate Package)

**This is not part of core qo.** It is a **durable orchestration layer** built on qo's
existing primitives. Core qo remains a simple task queue.

Implementation lives in `crates/qow` (Rust + Python bindings).

## Overview

qow provides workflow orchestration without introducing a control plane,
external databases, or coordination services. It uses S3 as the only state store,
just like core qo.

## Comparison with Other Systems

Before diving in, let's be clear about what qow provides vs. alternatives:

| Aspect | Temporal | Hatchet | Prefect | qow |
|--------|----------|---------|---------|--------------|
| **Execution Guarantee** | At-least-once (activities) | At-least-once | Exactly-once (cache-based) | At-least-once |
| **Exactly-Once Effects** | Requires idempotent activities | Requires idempotent steps | Via cache keys | Requires idempotent handlers |
| **Code Determinism Required** | Yes (major pain point) | No | No | No |
| **Workflow State** | Event log + replay | PostgreSQL | Cloud-managed | S3 objects (CAS) |
| **Database Required** | PostgreSQL/MySQL/Cassandra | PostgreSQL | PostgreSQL | None (S3 only) |
| **Step Latency** | ~20ms | ~20ms | Varies | 1-5 seconds |
| **Deployment Complexity** | High (versioning required) | Medium | Medium | Low |
| **Debuggability** | Requires tooling | Dashboard | Dashboard | `aws s3 cat` |

### What We Trade Away

**Sub-second latency**: qo is polling-based. Steps take 1-5 seconds to dispatch.
Not suitable for real-time or interactive workflows.

**Deterministic replay**: No event sourcing. If a step fails mid-way, you get
at-least-once retry, not exactly-once replay. Side effects must be idempotent.

**Strong ordering**: No FIFO guarantees. Steps within a workflow run in dependency
order, but concurrent workflows have no ordering.

### What We Gain

**Zero infrastructure**: Just S3. No PostgreSQL, no Redis, no Kubernetes.

**Full inspectability**: Every workflow state is a JSON object. Debug with
`aws s3 cat s3://bucket/workflow/wf-123/state.json`.

**Cost scales with usage**: No fixed infrastructure costs. Pay for S3 requests.

**Crash-safe by design**: All state transitions are CAS-protected. Workers can
crash at any point and workflows resume correctly.

**No code versioning hell**: Unlike Temporal, you don't need to version workflow
code. State is simple JSON, not an event log requiring deterministic replay.

## Philosophy

| Principle | How This Applies |
|-----------|------------------|
| **S3 as only state** | Workflow state, step results, signals - all S3 objects |
| **Reuse existing primitives** | Steps are tasks. Timers use `available_at`. Retries use retry policies |
| **At-least-once execution** | Steps may run multiple times. Handlers must be idempotent for safe effects. |
| **Simple > Automatic** | Explicit step definitions, not magic replay |
| **Debuggable** | `aws s3 ls` shows all workflow state |
| **Honest about limitations** | No false exactly-once claims. You own idempotency. |

## Architecture

```
workflow/{id}/state.json        # Canonical workflow state (CAS updates)
workflow/{id}/steps/{name}.json # Step results (immutable after completion)
workflow/{id}/signals/{name}/   # Signal inbox (list for waiting signals)
```

Each workflow step is a normal qo task. The orchestrator is a special task handler
that reads workflow state, determines the next step, and submits child tasks.

### Workflow State Object

```json
{
  "id": "wf-order-12345",
  "type": "order_fulfillment",
  "definition_hash": "sha256:abc123...",
  "status": "running",
  "current_steps": ["charge_card"],
  "data": {
    "order_id": "ORD-12345",
    "user_id": "user-789"
  },
  "steps": {
    "validate_order": {"status": "completed", "result": {"valid": true}},
    "reserve_inventory": {"status": "completed", "result": {"reserved": true}},
    "charge_card": {"status": "running", "started_at": "2026-01-28T12:00:00Z"}
  },
  "error": null,
  "created_at": "2026-01-28T11:55:00Z",
  "updated_at": "2026-01-28T12:00:00Z",
  "orchestrator_task_id": "task-xyz789"
}
```

| Field | Description |
|-------|-------------|
| `current_steps` | Array of currently executing step names. Supports parallel execution. |
| `definition_hash` | Hash of workflow definition (steps + dependencies). Used to detect incompatible changes. |
| `orchestrator_task_id` | Current orchestrator task ID. Used for stall detection. |
| `error` | Last error if workflow is in `failed` or `paused` status. |

## Execution Model

### Step Execution as Tasks

Each step runs as an qo task with type `workflow.step:{workflow_type}:{step_name}`:

```
tasks/a/abc123.json  →  type: "workflow.step:order_fulfillment:charge_card"
                        input: {"workflow_id": "wf-order-12345", "data": {...}}
```

When a step completes:
1. Step result is written to `workflow/{id}/steps/{name}.json`
2. Workflow state is CAS-updated
3. Next step task is submitted (if any)

### Orchestrator Pattern

The orchestrator is itself a task handler that drives workflow progression:

```python
@worker.task("workflow.orchestrate")
async def orchestrate(input, context):
    wf_id = input["workflow_id"]

    # Read current workflow state (with ETag for CAS)
    state, etag = await get_workflow_state(wf_id)

    # Determine next steps based on DAG and completed steps
    next_steps = compute_next_steps(state)

    if not next_steps:
        # Workflow complete
        state["status"] = "completed"
        await update_workflow_state(wf_id, state, etag)
        return {"completed": True}

    # Submit next step tasks
    for step in next_steps:
        await queue.submit(
            f"workflow.step:{state['type']}:{step}",
            {"workflow_id": wf_id, "step": step, "data": state["data"]},
            idempotency_key=f"{wf_id}:{step}",  # Prevents duplicate steps
        )

    # Update state and schedule next orchestration check
    state["current_steps"] = next_steps
    await update_workflow_state(wf_id, state, etag)
```

## Workflow Versioning

### Definition Hash

Each workflow stores a `definition_hash` computed from its step definitions and
dependencies. This detects incompatible code changes mid-run.

```python
# Hash is computed from step names, dependencies, and structural metadata
definition_hash = sha256(json.dumps({
    "steps": ["validate", "reserve", "charge", "ship"],
    "dependencies": {
        "reserve": ["validate"],
        "charge": ["validate"],
        "ship": ["reserve", "charge"]
    }
}))
```

### Compatibility Rules

When the orchestrator runs, it compares the running workflow's `definition_hash`
against the current code:

| Scenario | Action |
|----------|--------|
| Hash matches | Continue normally |
| New step added (not yet reached) | Continue - compatible change |
| Step removed (already completed) | Continue - compatible change |
| Step removed (not yet run) | **Pause workflow** - requires migration |
| Step dependencies changed | **Pause workflow** - requires migration |
| Step renamed | **Pause workflow** - requires migration |

### Migration

Incompatible changes require explicit migration:

```python
# Migrate a workflow to a new definition
await client.migrate(
    wf_id,
    target_definition_hash="sha256:newdef...",
    step_mapping={
        "old_step_name": "new_step_name",  # Rename
        "removed_step": None,               # Mark as skipped
    }
)
```

Workflows in `paused` status due to version mismatch show:

```json
{
  "status": "paused",
  "error": {
    "type": "VersionMismatch",
    "message": "Workflow definition changed. Expected sha256:abc..., got sha256:def...",
    "incompatible_steps": ["renamed_step"]
  }
}
```

## Failure Semantics

### Step Error → Workflow State Transitions

| Step Outcome | Retries Left | Compensation Defined | Workflow Transition |
|--------------|--------------|----------------------|---------------------|
| Success | - | - | Continue to next step |
| `RetryableError` | Yes | - | Retry step (backoff) |
| `RetryableError` | No | No | **Workflow → `failed`** |
| `RetryableError` | No | Yes | Run compensation chain, then **→ `failed`** |
| `PermanentError` | - | No | **Workflow → `failed`** |
| `PermanentError` | - | Yes | Run compensation chain, then **→ `failed`** |
| Timeout | Yes | - | Retry step |
| Timeout | No | No | **Workflow → `failed`** |
| Timeout | No | Yes | Run compensation chain, then **→ `failed`** |
| Cancelled | - | Yes | Run compensation chain, then **→ `cancelled`** |
| Cancelled | - | No | **Workflow → `cancelled`** |

### Compensation Chain

When a step fails with compensation handlers registered, the workflow runs
compensations in reverse order of completed steps:

```
Step execution:     validate → reserve → charge (FAILS)
Compensation chain: charge_compensate → reserve_compensate → (done)
```

Compensation runs best-effort. If a compensation step fails:
1. Log the failure
2. Continue with remaining compensations
3. Final workflow status is `failed` with `compensation_errors` array

### Workflow Statuses

| Status | Description |
|--------|-------------|
| `pending` | Created, not yet started |
| `running` | Actively executing steps |
| `paused` | Suspended due to version mismatch or manual pause |
| `waiting_signal` | Blocked waiting for external signal |
| `compensating` | Running compensation chain after failure |
| `completed` | All steps finished successfully |
| `failed` | Step failed after retries exhausted |
| `cancelled` | Cancelled by user/system |

## Orchestrator Recovery

### Stall Detection

The orchestrator task can crash, leaving a workflow stuck. A periodic sweeper
detects and recovers stalled workflows.

**Stall detection logic:**

```python
# Sweeper runs every N minutes (configurable, default: 5)
# Check all active workflow states, not just "running"
ACTIVE_STATES = ["running", "waiting_signal", "compensating"]

for workflow in list_workflows_by_status(ACTIVE_STATES):
    orchestrator_task = await queue.get(workflow.orchestrator_task_id)
    now = await queue.now()  # Use server time

    if orchestrator_task is None:
        # Task was deleted or never created - stalled
        await recover_workflow(workflow.id)

    elif orchestrator_task.status in ["completed", "failed", "cancelled"]:
        # Orchestrator finished but workflow still active - stalled
        await recover_workflow(workflow.id)

    elif orchestrator_task.status == "running":
        # Check if lease expired (task timed out)
        if orchestrator_task.lease_expires_at < now:
            # Will be recovered by qo's timeout monitor
            pass
        else:
            # Orchestrator is alive, workflow is healthy
            pass
```

**Recovery action:**

```python
async def recover_workflow(wf_id):
    state, etag = await get_workflow_state(wf_id)

    # Re-submit orchestrator task with idempotency
    task = await queue.submit(
        "workflow.orchestrate",
        {"workflow_id": wf_id},
        idempotency_key=f"{wf_id}:orchestrate:{state['updated_at']}",
    )

    # Update state with new orchestrator task ID
    state["orchestrator_task_id"] = task.id
    await update_workflow_state(wf_id, state, etag)
```

### Sweeper Configuration

```python
# Run as part of qo monitor or standalone
qo workflow sweeper --interval 300  # Check every 5 minutes
```

The sweeper is idempotent - running multiple instances is safe.

## Signal Semantics

Signals use a **cursor-based append-only** model. Signals are never deleted;
consumption is tracked by advancing a cursor in workflow state.

> **Canonical spec:** See `signal-semantics.md` for full details.

### Key Format

```
workflow/{id}/signals/{signal_name}/{timestamp}_{uuid}.json
```

- **Timestamp**: ISO 8601 from `queue.now()` (server time, prevents clock skew)
- **Separator**: Underscore (`_`) to avoid confusion with ISO timestamp hyphens
- **UUID**: Random UUID for uniqueness

Example:
```
workflow/wf-123/signals/approval/2026-01-28T12:00:00Z_a1b2c3d4.json
```

### Signal Lifecycle

```
                    send_signal()
                         │
                         ▼
┌─────────────────────────────────────────┐
│  workflow/{id}/signals/{name}/{ts}_{id} │  (object created, immutable)
└─────────────────────────────────────────┘
                         │
                         │  step calls wait_for_signal()
                         ▼
┌─────────────────────────────────────────┐
│  Cursor advanced in workflow state      │  (signal remains)
└─────────────────────────────────────────┘
```

### Cursor Format

The cursor stores the **suffix only** (`{timestamp}_{uuid}`), not the full key:

```json
{
  "signals": {
    "approval": {
      "cursor": "2026-01-28T12:00:00Z_a1b2c3d4"
    }
  }
}
```

S3 ListObjects uses `start_after=prefix+cursor` to list signals after the cursor.

### Consumption (Cursor-Based)

Signals are consumed by **advancing the cursor**, not deleting objects:

```python
async def wait_for_signal(wf_id, name, timeout):
    deadline = await queue.now() + timeout
    prefix = f"workflow/{wf_id}/signals/{name}/"

    while await queue.now() < deadline:
        state, etag = await get_workflow_state(wf_id)
        cursor = state.get("signals", {}).get(name, {}).get("cursor")

        # List signals after cursor (suffix only)
        start_after = f"{prefix}{cursor}" if cursor else None
        signals = await s3.list_objects(prefix=prefix, start_after=start_after)

        if signals:
            signal = signals[0]  # Oldest unconsumed
            data = await get_signal(signal.key)

            # Advance cursor to suffix only
            suffix = signal.key.removeprefix(prefix)
            state.setdefault("signals", {}).setdefault(name, {})["cursor"] = suffix
            await update_workflow_state(wf_id, state, etag)

            return data

        await sleep(poll_interval)

    return None  # Timeout
```

### At-Least-Once Delivery

Signals are **at-least-once**, consistent with qo's task execution model:

- The same signal may be sent multiple times (sender retries, network issues)
- Signal handlers **must be idempotent**
- qo does not deduplicate signals at the infrastructure level

This is intentional: qo doesn't solve idempotency for tasks, and doesn't solve
it for signals either. Handlers own idempotency.

```python
async def send_signal(wf_id, name, data):
    ts = await queue.now()
    signal_id = str(uuid4())
    key = f"workflow/{wf_id}/signals/{name}/{ts.isoformat()}_{signal_id}.json"
    await s3.put_object(key, json.dumps({
        "id": signal_id,
        "name": name,
        "payload": data,
        "created_at": ts.isoformat(),
    }))
```

### Signal Expiration

Old signals can be garbage-collected by the retention policy (see `retention.md`).
Only signals from **terminal workflows** are eligible for deletion.

## Features

### DAG Support

Define step dependencies declaratively:

```python
from qow import Workflow, step

wf = Workflow("order_fulfillment")

@wf.step("validate_order")
async def validate(ctx):
    return {"valid": True}

@wf.step("reserve_inventory", depends_on=["validate_order"])
async def reserve(ctx):
    return {"reserved": True}

@wf.step("charge_card", depends_on=["validate_order"])
async def charge(ctx):
    return {"charged": True}

@wf.step("ship", depends_on=["reserve_inventory", "charge_card"])
async def ship(ctx):
    # Only runs after both reserve and charge complete
    return {"shipped": True}
```

Execution graph:
```
validate_order
    ├── reserve_inventory ──┐
    └── charge_card ────────┴── ship
```

### Timers and Delays

Use qo's `available_at` for durable timers:

```python
@wf.step("send_reminder", delay=timedelta(hours=24))
async def remind(ctx):
    # Runs 24 hours after previous step completes
    await send_reminder_email(ctx.data["user_id"])
```

Under the hood, this submits a task with `schedule_at` set to 24 hours in the future.

### Retries and Timeouts

Steps use qo's existing retry policies:

```python
@wf.step("charge_card", retries=3, timeout=30)
async def charge(ctx):
    # Retries up to 3 times with exponential backoff
    # Times out after 30 seconds
    return await payment_gateway.charge(ctx.data["amount"])
```

### Best-Effort Exactly-Once Effects

Side effects can be made idempotent **if and only if** the downstream system
supports idempotency keys or you can implement check-then-act patterns:

```python
@wf.step("charge_card")
async def charge(ctx):
    # Works IF payment gateway supports idempotency
    result = await payment_gateway.charge(
        amount=ctx.data["amount"],
        idempotency_key=f"order-{ctx.data['order_id']}-charge",
    )
    return {"transaction_id": result.id}

@wf.step("send_email")
async def send(ctx):
    # Check-then-act pattern for systems without idempotency
    if await db.email_already_sent(ctx.data["order_id"]):
        return {"skipped": True}

    await email_service.send(ctx.data["email"])
    await db.mark_email_sent(ctx.data["order_id"])
    return {"sent": True}
```

**qo cannot guarantee exactly-once effects.** It provides:
1. Idempotency keys to prevent duplicate task submission
2. The same idempotency key pattern for your external calls

But if your downstream doesn't support idempotency, effects may happen multiple
times on retry. This is inherent to at-least-once systems.

qo's built-in idempotency keys prevent duplicate step submissions:

```python
await queue.submit(
    f"workflow.step:order_fulfillment:charge_card",
    {...},
    idempotency_key=f"{workflow_id}:charge_card",  # Step task created once
)
# Note: The step TASK is created once, but the step CODE may run multiple
# times if the worker crashes mid-execution.
```

### Cancellation

Workflows and steps support cancellation (using qo's cancellation features):

```python
# Cancel a workflow
await workflow.cancel(wf_id, reason="User requested refund")

# In a step, check for cancellation
@wf.step("long_process")
async def process(ctx):
    for item in items:
        if await ctx.is_cancellation_requested():
            # Clean up and exit
            return {"cancelled": True, "processed": len(processed)}
        await process_item(item)
```

### Signals (External Events)

Wait for external events using signal objects:

```python
@wf.step("wait_for_approval")
async def wait_approval(ctx):
    # Poll for approval signal
    signal = await ctx.wait_for_signal("approval", timeout=timedelta(days=7))

    if signal is None:
        raise PermanentError("Approval timed out")

    return {"approved_by": signal["approver"]}

# External system sends signal
await workflow.signal(wf_id, "approval", {"approver": "manager@example.com"})
```

Signals are stored as S3 objects:
```
workflow/{id}/signals/approval/{timestamp}_{uuid}.json
```

### Fan-Out / Fan-In

Process items in parallel and wait for all to complete:

```python
@wf.step("process_items")
async def process(ctx):
    items = ctx.data["items"]

    # Fan-out: submit child tasks for each item
    child_ids = []
    for item in items:
        task = await queue.submit(
            "process_single_item",
            {"item": item},
            idempotency_key=f"{ctx.workflow_id}:item:{item['id']}",
        )
        child_ids.append(task.id)

    # Fan-in: wait for all children to complete
    await ctx.wait_for_tasks(child_ids, timeout=timedelta(hours=1))

    # Collect results
    results = [await queue.get(tid) for tid in child_ids]
    return {"processed": len(results)}
```

### Saga Pattern (Compensation)

Handle failures with compensating actions:

```python
wf = Workflow("transfer_funds")

@wf.step("debit_source")
async def debit(ctx):
    return await accounts.debit(ctx.data["source"], ctx.data["amount"])

@wf.step("credit_destination", depends_on=["debit_source"])
async def credit(ctx):
    try:
        return await accounts.credit(ctx.data["destination"], ctx.data["amount"])
    except InsufficientFundsError:
        # Compensate: refund the source account
        await accounts.credit(ctx.data["source"], ctx.data["amount"])
        raise PermanentError("Transfer failed, funds refunded")
```

Or use explicit compensation handlers:

```python
@wf.step("reserve_inventory")
async def reserve(ctx):
    return await inventory.reserve(ctx.data["items"])

@reserve.on_compensate
async def unreserve(ctx, step_result):
    await inventory.release(step_result["reservation_id"])
```

## Guarantees

### What qow Guarantees

- **Durable workflow state**: State is persisted in S3 with CAS protection
- **At-least-once step execution**: Every step will run at least once
- **Crash recovery**: Workers can crash at any point; workflows resume correctly
- **Exactly-once step submission**: Idempotency keys prevent duplicate step tasks
- **Ordered dependencies**: Steps respect DAG dependencies
- **Auditable history**: All state changes visible via S3 versioning
- **Stall recovery**: Sweeper detects and recovers stuck workflows

### What qow Does NOT Guarantee

- **Exactly-once step execution**: Steps may run multiple times. Use idempotency.
- **Exactly-once side effects**: Only achievable if downstream systems support
  idempotency AND your handler uses it correctly. qo provides idempotency keys
  to help, but cannot guarantee external systems behave correctly.
- **Sub-second latency**: Expect 1-5 second step dispatch times
- **Strong ordering across workflows**: Different workflows have no ordering
- **Deterministic replay**: No event sourcing. State is snapshots, not logs.
- **Signal exactly-once delivery**: Signals are at-least-once. Design handlers
  to be idempotent.

### Idempotency: Your Responsibility

qow provides tools for idempotency, but **you must use them correctly**:

```python
# GOOD: Using idempotency key for external API
@wf.step("charge_card")
async def charge(ctx):
    return await payment_api.charge(
        amount=ctx.data["amount"],
        idempotency_key=f"order-{ctx.data['order_id']}-charge",  # ✓
    )

# BAD: No idempotency - will double-charge on retry
@wf.step("charge_card")
async def charge(ctx):
    return await payment_api.charge(amount=ctx.data["amount"])  # ✗
```

If the downstream system doesn't support idempotency, you cannot achieve
exactly-once effects. This is a fundamental distributed systems constraint,
not an qo limitation.

## API

### Python

```python
from qow import Workflow, step, WorkflowClient

# Define workflow
wf = Workflow("order_fulfillment")

@wf.step("validate")
async def validate(ctx):
    return {"valid": True}

@wf.step("process", depends_on=["validate"])
async def process(ctx):
    return {"processed": True}

# Start workflow
client = WorkflowClient(queue)
run = await client.start("order_fulfillment", {"order_id": "ORD-123"})
print(f"Started workflow: {run.id}")

# Check status
status = await client.get(run.id)
print(f"Status: {status.status}, current steps: {status.current_steps}")

# Wait for completion
result = await client.wait(run.id, timeout=timedelta(minutes=30))
print(f"Workflow completed: {result.data}")

# Cancel
await client.cancel(run.id, reason="User requested")

# Send signal
await client.signal(run.id, "approval", {"approved": True})
```

### CLI

```bash
# Start a workflow
qo workflow start order_fulfillment '{"order_id": "ORD-123"}'

# Check status
qo workflow status wf-abc123

# List workflows
qo workflow list --type order_fulfillment --status running

# Cancel
qo workflow cancel wf-abc123 --reason "No longer needed"

# Send signal
qo workflow signal wf-abc123 approval '{"approved": true}'

# View step results
qo workflow steps wf-abc123
```

## Implementation

### Package Structure

```
qow/
└── crates/qow/
    ├── src/
    │   ├── lib.rs           # Rust core
    │   ├── workflow.rs      # Workflow state management
    │   ├── orchestrator.rs  # Step scheduling logic
    │   └── signals.rs       # Signal handling
    └── python/
        └── qow/
            ├── __init__.py
            ├── workflow.py
            └── client.py
```

### S3 Object Layout

```
workflow/
├── {workflow_id}/
│   ├── state.json              # Current workflow state
│   ├── steps/
│   │   ├── validate.json       # Step result
│   │   ├── process.json
│   │   └── ship.json
│   └── signals/
│       └── approval/
│           └── {timestamp}_{signal_id}.json
```

### Estimated Effort

| Component | Effort |
|-----------|--------|
| Workflow state management | 2-3 days |
| Orchestrator task handler | 2-3 days |
| DAG computation | 1-2 days |
| Signal handling | 1-2 days |
| Python API | 2-3 days |
| CLI commands | 1 day |
| Tests | 3-4 days |
| Documentation | 1-2 days |
| **Total** | **2-3 weeks** |

## When to Use qow

**Good fit:**
- Multi-step processes that need durable coordination
- Batch processing pipelines
- Order fulfillment, ETL jobs, onboarding flows
- Teams that want workflow orchestration without infrastructure
- When S3 costs are cheaper than running PostgreSQL

**Poor fit:**
- Real-time or interactive workflows (latency too high)
- Workflows requiring sub-100ms step dispatch
- Complex DAGs with hundreds of steps (S3 list operations)
- When you need Temporal's deterministic replay guarantees

## Comparison: When to Use What

| Use Case | qow | Hatchet | Temporal |
|----------|--------------|---------|----------|
| Simple multi-step jobs | ✓ Best fit | Good | Overkill |
| Data pipelines | ✓ Good | Good | Overkill |
| Long-running sagas | ✓ Good | ✓ Best fit | ✓ Best fit |
| Real-time workflows | ✗ Too slow | ✓ Good | ✓ Good |
| Complex DAGs (100+ steps) | ✗ S3 limits | ✓ Good | ✓ Good |
| Exactly-once with replay | ✗ No replay | ✗ No replay | ✓ Yes |
| Zero infrastructure | ✓ Best fit | ✗ Needs Postgres | ✗ Needs Postgres+ |
| Debugging production issues | ✓ Best fit | Good | Requires tooling |

## Summary

qow provides durable workflow orchestration for teams that want:
- **Simplicity**: Just S3, no databases or message brokers
- **Transparency**: All state is inspectable JSON
- **Reliability**: Crash-safe with at-least-once execution
- **Low cost**: Pay for S3 requests, not infrastructure

It trades latency and deterministic replay for operational simplicity. For most
batch processing and async workflow needs, this is the right tradeoff.
