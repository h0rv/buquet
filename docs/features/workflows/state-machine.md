# Feature: Workflow State Machine

## Status: Implemented

## Overview
Define explicit workflow and step state transitions to remove ambiguity around
failures, retries, and cancellation. These states are enforced by the
buquet-workflow engine and serialized in `state.json`.

## Philosophy Alignment

| Principle | How This Aligns |
|-----------|-----------------|
| Simple, boring, reliable | A small, explicit state machine |
| Debuggable | State transitions are visible in JSON |
| Explicit > implicit | Clear failure/cancel semantics |

## Problem
Without a defined state machine, different implementations can interpret
failures inconsistently (e.g., retry vs fail vs pause).

## Solution
Define workflow and step states and allowed transitions.

## Workflow States
- `pending` - Created, not yet started
- `running` - Actively executing steps
- `waiting_signal` - Blocked waiting for external signal
- `compensating` - Running compensation chain after failure
- `paused` - Suspended (version mismatch or manual pause)
- `completed` - All steps finished successfully
- `failed` - Step failed after retries exhausted
- `cancelled` - Cancelled by user/system

Allowed transitions:
```
pending -> running
running -> completed
running -> failed
running -> cancelled
running -> paused
running -> waiting_signal
running -> compensating
waiting_signal -> running         (signal received)
waiting_signal -> cancelled
waiting_signal -> failed          (timeout)
compensating -> failed            (compensation done)
compensating -> cancelled         (compensation done, was cancel)
paused -> running                 (resumed)
paused -> cancelled
```

### State Diagram

```
                    ┌──────────┐
                    │ pending  │
                    └────┬─────┘
                         │ start
                         ▼
    ┌───────────────► running ◄───────────────┐
    │                   │ │ │                 │
    │     ┌─────────────┘ │ └─────────────┐   │
    │     │               │               │   │
    │     ▼               ▼               ▼   │
    │  paused      waiting_signal    compensating
    │     │               │               │
    │     │ resume        │ signal        │ done
    └─────┘               │               │
                         │               ▼
              ┌──────────┴──────────► failed
              │                          ▲
              ▼                          │
          completed                  cancelled
```

## Step States
- `pending`
- `running`
- `completed`
- `failed`
- `cancelled`

Allowed transitions:
- `pending -> running`
- `running -> completed`
- `running -> failed`
- `running -> cancelled`
- `failed -> pending` (retry)

## Failure Policy
Each step can declare `on_failure`:
- `fail_workflow` (default)
- `pause_workflow`
- `continue` (skip step)
- `compensate` (run compensation handler)

## Workflow State Fields
```json
{
  "status": "running",
  "current_steps": ["charge_card", "reserve_inventory"],
  "last_error": {
    "step": "charge_card",
    "message": "timeout",
    "attempt": 3,
    "at": "2026-01-28T12:30:00Z"
  }
}
```

**Note:** `current_steps` is an **array** to support parallel step execution
(e.g., fan-out). This is the canonical field name across all workflow specs.

## Migration / Compatibility
- This defines behavior for the orchestrator; existing runs are unchanged.
- Older clients can ignore new fields.

## Related
- `index.md` (main workflow spec)
