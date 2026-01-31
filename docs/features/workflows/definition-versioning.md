# Feature: Workflow Definition Versioning

## Status: Implemented

## Overview
Record a workflow definition hash in each workflow run so orchestrator behavior
is explicit and stable across deploys.

> **Note:** This is the detailed spec for versioning described in `index.md`.

## Philosophy Alignment

| Principle | How This Aligns |
|-----------|-----------------|
| Simple, boring, reliable | Store a hash in state |
| Debuggable | Hash visible in `state.json` |
| No extra infra | Pure S3 metadata |

## Problem
Workflow code can change while runs are in progress. Without a recorded
version, retries may execute under a new definition, causing inconsistent
behavior.

## Solution
- **Primary:** Store `definition_hash` (SHA-256 of step names + dependencies) in
  workflow state. This is computed automatically.
- **Optional (not implemented):** Store human-readable `definition_version`
  (e.g., "2026-01-28") for display purposes.
- On mismatch, orchestrator **pauses** the workflow (never fails).
- Allow explicit migration with manual override.

**Important:** Mismatch always results in `paused` status (not `failed`), because
the workflow may still be valid under the new definition. The operator must
explicitly resume or cancel.

## Definition Hash Computation

```python
# Hash is computed from structural metadata only
definition_hash = sha256(json.dumps({
    "steps": sorted(["validate", "reserve", "charge", "ship"]),
    "dependencies": {
        "reserve": ["validate"],
        "charge": ["validate"],
        "ship": ["reserve", "charge"]
    }
}, sort_keys=True))
```

The hash changes when:
- Steps are added, removed, or renamed
- Step dependencies change

The hash does NOT change when:
- Step handler code changes (logic updates are safe)
- Retry policies change
- Timeouts change

## Workflow State Fields
```json
{
  "id": "wf-123",
  "type": "order_fulfillment",
  "definition_hash": "sha256:abc123...",
  "definition_version": "2026-01-28",
  "status": "running",
  ...
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `definition_hash` | Yes | SHA-256 of step structure (auto-computed) |
| `definition_version` | No | Human-readable version for display |

## Compatibility Rules

### V1: Simple (Current Implementation)

Any hash mismatch results in pause. This is safe and predictable:

| Scenario | Action |
|----------|--------|
| Hash matches | Continue normally |
| Hash differs (any reason) | **Pause** - operator must review |

### V2: Smart Compatibility (Future)

> **TODO:** Implement nuanced compatibility detection based on step completion state.

| Scenario | Action |
|----------|--------|
| Hash matches | Continue normally |
| New step added (not yet reached) | Continue - compatible |
| Step removed (already completed) | Continue - compatible |
| Step removed (not yet run) | **Pause** - requires migration |
| Step dependencies changed | **Pause** - requires migration |
| Step renamed | **Pause** - requires migration |

V2 requires analyzing which steps have completed vs which are pending to determine
if the structural change affects the remaining execution path.

## API Sketch

### Python
```python
# Version is optional, hash is auto-computed
wf = Workflow("order_fulfillment", version="2026-01-28")
run = await client.start("order_fulfillment", data)

# Check for version mismatch
if run.status == "paused" and run.error.type == "VersionMismatch":
    print(f"Expected: {run.error.expected_hash}")
    print(f"Got: {run.error.actual_hash}")
```

### CLI
```bash
# Start with optional version label
qo workflow start order_fulfillment '{"order_id":"123"}' --version 2026-01-28

# Resume with force (acknowledge version change)
qo workflow resume wf-123 --force-version
```

## Behavior
- On mismatch, orchestrator sets status `paused` with error:
  ```json
  {
    "type": "VersionMismatch",
    "message": "Workflow definition changed",
    "expected_hash": "sha256:abc...",
    "actual_hash": "sha256:def...",
    "incompatible_steps": ["renamed_step"]
  }
  ```
- Operator can resume with `--force-version` to acknowledge change.

## Migration / Compatibility
- If `definition_hash` is absent, orchestrator treats it as "unversioned" and
  continues (backward compatible).
- `definition_version` is purely informational and not used for mismatch detection.

## Related
- `index.md` (main workflow spec, Workflow Versioning section)
