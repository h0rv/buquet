# Feature: Shard Leasing (Dynamic Shard Ownership)

## Status: Implemented

Shard Leasing is a simpler alternative to Direct Dispatch. It keeps the current
single-object task model and ready/lease indexes, but eliminates per-worker
full-shard polling by assigning shards dynamically via S3-backed leases.

## Overview

Instead of every worker listing every shard, workers **lease a subset of shards**
from S3. Each worker only polls the shards it owns. Ownership is maintained with
short-lived leases stored in S3 using conditional writes.

This preserves the core model:
- **Tasks live in `tasks/` for their entire lifecycle** (event-sourced history)
- **Ready/lease indexes stay as projections**
- **Claiming is still CAS on the task object**

```
Current (Shared Pull):
Workers poll all shards
┌───────────────┐
│ ready/0/..    │
│ ready/1/..    │  ← Every worker lists these
│ ...           │
│ ready/f/..    │
└───────────────┘

Shard Leasing:
┌───────────────┐      ┌───────────────┐
│ shard leases  │      │ ready shards  │
│ leases/0.json │ ───▶ │ ready/0/...   │
│ leases/1.json │      │ ready/1/...   │
│ ...           │      │ ...           │
└───────────────┘      └───────────────┘

Worker A owns shards: 0, 3, 9
Worker B owns shards: 1, 2, 4
...
```

## Why This Helps

| Aspect | Shared Pull | Shard Leasing |
|--------|-------------|---------------|
| Task discovery | W x S LISTs | ~S LISTs total |
| Contention | None | None (shards are exclusive) |
| Scaling | Worker cost grows with shard count | Worker cost grows with owned shards only |
| Complexity | Low | Moderate (leases) |
| Auditability | Full | Full |

- Costs drop because **workers no longer list shards they don't own**.
- No registry manager, no per-worker inboxes, no new control plane.

## S3 Structure

```
bucket/
├── tasks/                      # Canonical tasks (unchanged)
├── ready/                      # Ready index (unchanged)
├── leases/                     # Task leases (unchanged)
└── shard-leases/               # NEW: shard ownership leases
    ├── 0.json
    ├── 1.json
    └── ...
```

`shard-leases/{shard}.json` contents:
```json
{
  "shard": "0",
  "worker_id": "worker-abc",
  "lease_expires_at": "2026-01-27T12:00:30Z",
  "updated_at": "2026-01-27T12:00:00Z"
}
```

All timestamps are based on **authoritative S3 time** (via `Queue::now()`), not
local clock time.

## Component Responsibilities

### Producer (Unchanged)
- Writes task to `tasks/{shard}/{id}.json` (CAS)
- Writes ready index `ready/{shard}/{bucket}/{id}`

### Worker
- Acquires leases on some shards
- Polls only owned shards
- Renews shard leases periodically
- Releases leases on shutdown

### Monitor (Optional)
- Can either:
  - run shard-leased sweeps (same mechanism), or
  - run globally (less efficient but simple)

## Lease Algorithm (Worker)

### Acquire shard lease
```python
async def try_lease(shard):
    key = f"shard-leases/{shard}.json"
    now = await queue.now()

    # Fast path: create lease if none exists
    lease = {"shard": shard, "worker_id": id, "lease_expires_at": now + ttl}
    ok = await s3.put_object_if_none_match(key, lease)
    if ok:
        return True

    # Slow path: lease exists, check expiry
    body, etag = await s3.get_object(key)
    if body["lease_expires_at"] > now:
        return False

    # Take over expired lease with If-Match
    lease["lease_expires_at"] = now + ttl
    await s3.put_object_if_match(key, lease, etag)
    return True
```

### Renew lease
```python
async def renew(shard, etag):
    now = await queue.now()
    lease = {"shard": shard, "worker_id": id, "lease_expires_at": now + ttl}
    await s3.put_object_if_match(f"shard-leases/{shard}.json", lease, etag)
```

If renewal fails (etag mismatch), the worker **loses ownership** and stops
polling that shard.

## Worker Poll Loop (Shard-Leased)

```
loop:
  ensure_leases(target_shards)
  for shard in owned_shards:
     list ready/{shard}/{bucket}/
     claim tasks via CAS
  sleep(adaptive_polling)
```

## Configuration

```toml
[worker.shard_leasing]
enabled = true

# How many shards each worker should target.
# Use more shards than workers to keep all workers busy.
# Example: 256 shards, 16 workers -> 16 shards per worker
shards_per_worker = 16

# Lease timing (authoritative S3 time)
lease_ttl_secs = 30
lease_renew_interval_secs = 10
```

Environment variables:
```
QO_SHARD_LEASING=1
QO_SHARD_LEASING_SHARDS_PER_WORKER=16
QO_SHARD_LEASING_TTL_SECS=30
QO_SHARD_LEASING_RENEW_SECS=10
```

## Trade-offs

**Pros**
- Keeps the single-object state machine intact
- Eliminates per-worker full-shard LIST cost
- No worker registry or inbox routing layer
- Failure recovery is simple (lease expiration)

**Cons**
- Adds lease management complexity
- Requires enough shards to distribute across workers
- Leases add extra PUT/GETs (but far less than LISTing all shards)

## Failure Modes & Recovery

### Worker dies
- Shard leases expire (TTL)
- Other workers take over by CAS
- Tasks are still in ready indexes, so nothing is lost

### Multiple workers try to take the same shard
- Only one succeeds due to CAS on lease object
- Others back off and try a different shard

### Lease object stuck or corrupted
- Can be overwritten after expiry with If-Match
- Worst case: delete by operator (safe, no task loss)

## Migration

Safe to enable without data migration:
- Producers unchanged
- Workers with shard leasing only change how they *discover* tasks

## Implementation Estimate

| Component | Lines (Rust) | Lines (Python) |
|-----------|--------------|----------------|
| Lease types + config | ~80 | ~30 |
| Lease manager | ~120 | ~50 |
| Runner changes | ~60 | ~20 |
| Tests | ~150 | ~60 |
| **Total** | **~410** | **~160** |

## Open Questions

1. **Lease allocation policy**: random, round-robin, or load-based?
2. **Shard count guidance**: how many shards per worker is ideal?
3. **Monitor integration**: should the monitor also use shard leases?
4. **Backoff when no shards available**: sleep vs. retry with jitter?

## Verdict

Shard Leasing captures most of the scalability win of Direct Dispatch while
preserving buquet’s core invariants and avoiding a new control plane. It is a strong
candidate for the “simple but scalable” path.
