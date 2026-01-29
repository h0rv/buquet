# oq-workflows Specification

This directory contains the specifications for oq-workflows, a durable
orchestration layer built on oq.

## Documents

| File | Description |
|------|-------------|
| `index.md` | Main specification - start here |
| `state-machine.md` | Workflow and step state transitions |
| `signal-semantics.md` | Signal ordering, deduplication, consumption |
| `definition-versioning.md` | Workflow versioning and migration |
| `reconciliation.md` | Sweeper for stall recovery |
| `retention.md` | Artifact retention and pruning |
| `rate-limits.md` | Step concurrency limits |
| `step-payload-refs.md` | Payload references for large step data |

## Key Design Decisions

1. **Cursor-based signals**: Signals are append-only with cursor-based
   consumption, not delete-to-ack.

2. **Signal key format**: `{timestamp}_{uuid}` (underscore separator).

3. **Cursor stores suffix only**: `{timestamp}_{uuid}`, not the full S3 key.

4. **Signals are at-least-once**: No infrastructure-level deduplication.
   Signal handlers must be idempotent (same as task handlers).

5. **`current_steps` is an array**: Supports parallel step execution.

6. **Definition hash for versioning**: `definition_hash` is the primary
   version identifier (auto-computed), `definition_version` is optional.

7. **Mismatch → pause**: Version mismatches always pause (never fail).

8. **Server time everywhere**: All timestamps use `queue.now()` to
   prevent clock skew.

9. **Terminal-only pruning**: Retention only affects completed/failed/cancelled
   workflows.

10. **Sweeper checks all active states**: `running`, `waiting_signal`, and
    `compensating` are all monitored for stalls.
