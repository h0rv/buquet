# Feature: Workflow Signal Semantics

## Status: Implemented

## Overview
Define explicit semantics for workflow signals (ordering and consumption) so
retries and polling are safe and predictable. Implemented in the workflow
signals manager and Python bindings.

## Philosophy Alignment

| Principle | How This Aligns |
|-----------|-----------------|
| Simple, boring, reliable | Signals are append-only S3 objects |
| Debuggable | All signals are visible via S3 keys |
| At-least-once | Signals may be sent multiple times; handlers must be idempotent |

## Problem
Without clear rules, signal delivery can be flaky under retries and polling.

## Solution
- Signals are immutable objects stored at
  `workflow/{id}/signals/{name}/{timestamp}_{uuid}.json`.
- The workflow state stores a `signal_cursor` per signal name (suffix only).
- `wait_for_signal(name)` lists keys after the cursor and consumes the first.
- Consumption is recorded by updating the cursor in workflow state (CAS).

### Key Format

```
workflow/{wf_id}/signals/{name}/{timestamp}_{uuid}.json
```

- **Timestamp**: ISO 8601 format from `queue.now()` (server time)
- **Separator**: Underscore (`_`) to avoid confusion with ISO timestamp hyphens
- **UUID**: Random UUID for uniqueness

Example:
```
workflow/wf-123/signals/approval/2026-01-28T12:00:00Z_a1b2c3d4.json
```

### Time Source
The timestamp **must** use server time from `queue.now()`, not client wall-clock
time. This prevents clock skew from causing signals to be written "before"
already-consumed signals.

```python
# CORRECT: Use server time
ts = await queue.now()  # Uses S3/server time
key = f"workflow/{wf_id}/signals/{name}/{ts.isoformat()}_{uuid4()}.json"

# WRONG: Client clock can skew
ts = datetime.now(UTC)  # Client clock may drift
```

### Cursor Format

The cursor stores the **suffix only** (`{timestamp}_{uuid}`), not the full key.
The workflow_id and signal name are already known from context.

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

## Signal Object

```json
{
  "id": "a1b2c3d4",
  "name": "approval",
  "payload": {"approved": true},
  "created_at": "2026-01-28T12:00:00Z"
}
```

## Behavior

### Ordering
- **Best-effort FIFO** by S3 key lexicographic order (timestamp prefix).
- Using server time prevents most clock skew issues.
- Sub-millisecond races result in arbitrary (but stable) UUID-based order.
- Cross-signal-name ordering is not guaranteed.

### At-Least-Once Delivery
Signals are **at-least-once**, consistent with buquet's task execution model:
- The same signal may be sent multiple times (sender retries, duplicates).
- Signal handlers must be idempotent.
- buquet does not deduplicate signals at the infrastructure level.

This is intentional: buquet doesn't solve idempotency for tasks, and doesn't solve
it for signals either. Handlers own idempotency.

### Consumption
- Cursor advancement (not deletion) marks signals as consumed.
- Signals remain in S3 for audit trail.
- Late signals (written after cursor advanced past their timestamp) are skipped.

## Migration / Compatibility
- Older workflows can ignore `signals` cursor fields.

## Related
- `index.md` (main workflow spec)
