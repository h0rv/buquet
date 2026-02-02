# Feature: Direct Dispatch

## Status: Not Planned

> **Decision**: Direct Dispatch is not being implemented. See [Why Not Implemented](#why-not-implemented) below. For scaling needs, use [Shard Leasing](./shard-leases.md) instead.

---

## Why Not Implemented

After cost analysis, Direct Dispatch doesn't provide cost savings over simpler approaches:

| Configuration | Monthly Cost (50 workers, 100K tasks/day) |
|--------------|-------------------------------------------|
| Hybrid Index + Adaptive Polling | **$56** |
| Shard Leasing + Adaptive | **$77** |
| Direct Dispatch | **$1,630** |

**Key insight**: Direct Dispatch doesn't reduce polling—it just changes *what* you poll (inbox instead of shards). Workers still LIST at the same frequency, so costs remain high.

The actual cost wins come from:
1. **Hybrid Index** (GET instead of LIST) — 200x reduction
2. **Adaptive Polling** (backoff when idle) — 17x reduction

Direct Dispatch adds significant complexity (inbox management, overflow, cursors, re-dispatch) for a **contention reduction** benefit that Shard Leasing achieves more simply.

**Recommendation**: Use Shard Leasing if you need contention reduction. It preserves all core invariants with far less complexity.

---

## Design Document (Archived)

The following is the complete design for reference, in case future requirements change.

## Status: Design Draft (v3)

This is a significant architectural addition. It adds complexity but enables horizontal scaling with reduced polling overhead.

> **v3 Changes**: Uses authoritative S3 time everywhere. Defines re-dispatch path for retries/timeouts. Adds cursor-based inbox scanning. Fixes data integrity issues.
>
> **v2 Changes**: Inbox is now an index (pointers only), tasks remain canonical in `tasks/`, claims use CAS, worker IDs are lease-protected.

## Overview

Direct Dispatch routes task **index entries** to worker inboxes at submit time. The canonical task object remains in `tasks/{shard}/{id}.json` for its entire lifecycle (preserving auditability and `get_history()`). Workers still claim via CAS on the task object—inbox entries are just "you should look at this task."

```
Key Insight: Inbox is an INDEX, not task storage.

┌──────────┐                      ┌─────────────────────────────────┐
│ Producer │──────────────────────▶ tasks/{shard}/{id}.json        │ (canonical)
└────┬─────┘                      └─────────────────────────────────┘
     │
     │ also writes index pointer
     ▼
┌─────────────────────────────────────────────────────────────────────┐
│ inbox/{worker}/{bucket}/{id}   (tiny: just task ID + shard)        │
└─────────────────────────────────────────────────────────────────────┘
     │
     │ worker reads inbox, then claims task via CAS
     ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Worker: GET tasks/{shard}/{id}.json                                 │
│         PUT tasks/{shard}/{id}.json (If-Match: etag) ← CLAIM        │
│         DELETE inbox/{worker}/{bucket}/{id} (best-effort)           │
└─────────────────────────────────────────────────────────────────────┘
```

## Core Invariants (Preserved)

1. **Single object for life**: Task lives at `tasks/{shard}/{id}.json` from submit to archive
2. **CAS-based claims**: Workers claim via `If-Match` on task object (not inbox)
3. **Auditability**: `get_history()` works—all state transitions on canonical object
4. **Lease semantics**: Existing lease/timeout logic unchanged
5. **Authoritative S3 time**: All time-based decisions use `Queue::now()` (S3 `Date` header), never local clocks

## Authoritative Time (Critical)

**All time-sensitive operations must use S3 time, not local clocks.**

```rust
// WRONG - local clock can drift, be manipulated, or disagree across machines
let now = Utc::now();
let expires_at = now + Duration::seconds(60);

// CORRECT - authoritative time from S3
let now = queue.now().await?;  // Reads S3 Date header
let expires_at = now + Duration::seconds(60);
```

This applies to:
- Lease expiry checks (`expires_at > now`)
- Time bucket calculation (`time_bucket(available_at)`)
- Availability checks (`task.available_at <= now`)
- Worker lease renewal
- Dead worker detection

Without authoritative time, clock skew between machines can cause:
- Tasks processed before `available_at`
- Leases expiring early/late
- Time buckets disagreeing between producer and worker

## What Direct Dispatch Changes

| Aspect | Shared Shards | Direct Dispatch |
|--------|---------------|-----------------|
| Task discovery | LIST `ready/{shard}/` | LIST `inbox/{worker}/{bucket}/` |
| Index written by | Worker (on claim) | Producer (on submit) |
| Who sees task first | Any worker polling shard | Specific worker |
| Contention | Workers race on same index | Each worker has own inbox |

**What stays the same:**
- Task object location (`tasks/{shard}/{id}.json`)
- Claim mechanism (CAS with `If-Match`)
- Lease renewal and timeout
- Status transitions
- `get_history()` and auditability

## S3 Structure

```
bucket/
├── tasks/                          # Canonical task objects (UNCHANGED)
│   ├── {shard}/
│   │   └── {id}.json              # Full task state, all history
│
├── inbox/                          # NEW: Worker inbox indexes
│   ├── {worker_id}/
│   │   ├── {time_bucket}/         # Bucketed by available_at minute
│   │   │   ├── {task_id}          # Tiny: {"shard": "a", "id": "..."}
│   │   │   └── ...
│   │   └── ...
│
├── workers/                        # NEW: Worker leases
│   └── {worker_id}.lease.json     # Worker ID reservation
│
├── overflow/                       # NEW: Orphaned inbox entries
│   ├── {time_bucket}/
│   │   └── {task_id}
│   └── ...
│
├── ready/                          # Existing ready index (still used for overflow)
│   └── {shard}/{bucket}/{id}
│
└── index/                          # Existing (unchanged)
```

## Detailed Flows

### Producer: Submit Task

```python
async def submit(task_type: str, input: dict, available_at: datetime = None) -> Task:
    now = await queue.now()  # Authoritative time

    task = Task(
        type=task_type,
        input=input,
        available_at=available_at or now,
        created_at=now,
    )

    # 1. Write canonical task object (ALWAYS)
    task_key = f"tasks/{task.shard}/{task.id}.json"
    await s3.put_object(task_key, task.to_json())

    # 2. Select target worker
    workers = await get_active_workers()  # Cached

    if workers:
        worker = select_worker(workers, task)
        # 3a. Write inbox index entry
        bucket = time_bucket(task.available_at)
        inbox_key = f"inbox/{worker.id}/{bucket}/{task.id}"
        inbox_entry = {"shard": task.shard, "id": str(task.id)}
        await s3.put_object(inbox_key, json.dumps(inbox_entry))
    else:
        # 3b. No workers available, write to shared ready index
        bucket = time_bucket(task.available_at)
        ready_key = f"ready/{task.shard}/{bucket}/{task.id}"
        await s3.put_object(ready_key, b"")  # Empty, just a pointer

    return task
```

### Worker: Startup with Lease

```python
async def startup(worker_id: str):
    lease_key = f"workers/{worker_id}.lease.json"
    now = await queue.now()  # Authoritative time

    lease_data = {
        "worker_id": worker_id,
        "created_at": now.isoformat(),
        "expires_at": (now + timedelta(seconds=60)).isoformat(),
        "hostname": socket.gethostname(),
    }

    # Try to create lease (If-None-Match = only if doesn't exist)
    try:
        await s3.put_object_if_none_match(lease_key, json.dumps(lease_data))
    except PreconditionFailed:
        # Lease exists - check if expired (using authoritative time)
        now = await queue.now()
        existing = await s3.get_object(lease_key)
        if parse_datetime(existing["expires_at"]) > now:
            raise WorkerIdConflict(f"Worker ID {worker_id} already in use")
        # Expired - take over with CAS
        lease_data["expires_at"] = (now + timedelta(seconds=60)).isoformat()
        await s3.put_object_if_match(lease_key, json.dumps(lease_data), existing.etag)

    # Start lease renewal loop
    asyncio.create_task(lease_renewal_loop(lease_key))
```

### Worker: Lease Renewal

```python
async def lease_renewal_loop(lease_key: str):
    while not shutdown:
        await asyncio.sleep(20)  # Renew every 20s (lease is 60s)

        now = await queue.now()  # Authoritative time

        lease_data = {
            "worker_id": worker_id,
            "created_at": original_created_at,
            "expires_at": (now + timedelta(seconds=60)).isoformat(),
            "hostname": socket.gethostname(),
        }

        try:
            current = await s3.get_object(lease_key)
            await s3.put_object_if_match(lease_key, json.dumps(lease_data), current.etag)
        except (PreconditionFailed, NotFound):
            # Lost lease - another worker took over or lease deleted
            logger.error("Lost worker lease, shutting down")
            trigger_shutdown()
            return
```

### Worker: Process Loop

```python
async def run():
    await startup(worker_id)

    while not shutdown:
        now = await queue.now()  # Authoritative time

        # 1. Poll MY inbox (cursor-based, never skips buckets)
        inbox_entries = await poll_inbox(now)

        # 2. Poll ready/ shards (for retries/timeouts)
        ready_entries = await poll_ready_shards(now)

        all_entries = inbox_entries + ready_entries

        # 3. Process each task
        for entry in all_entries:
            is_inbox = entry.key.startswith(f"inbox/{worker_id}/")

            if is_inbox:
                entry_data = json.loads(await s3.get_object(entry.key))
                task_key = f"tasks/{entry_data['shard']}/{entry_data['id']}.json"
            else:
                # Ready entry - parse shard and id from key
                # Format: ready/{shard}/{bucket}/{task_id}
                parts = entry.key.split("/")
                shard, task_id = parts[1], parts[3]
                task_key = f"tasks/{shard}/{task_id}.json"

            # 4. Claim via CAS (same as shared shard model)
            try:
                task, etag = await s3.get_object(task_key)

                if task.status != "pending":
                    # Already claimed - delete stale index entry
                    await s3.delete_object(entry.key)
                    continue

                if not task.is_available(now):  # Use authoritative time
                    # Not ready yet - skip (will see it next poll)
                    continue

                # Claim with lease
                task.status = "running"
                task.claimed_by = worker_id
                task.lease_expires_at = now + lease_duration

                await s3.put_object_if_match(task_key, task.to_json(), etag)

            except PreconditionFailed:
                # Someone else claimed it - that's fine
                await s3.delete_object(entry.key)  # Best-effort cleanup
                continue

            # 5. Delete index entry (best-effort, idempotent)
            await s3.delete_object(entry.key)

            # 6. Process task (same as before)
            await process_task(task)

        # 7. Adaptive backoff if no tasks
        if not all_entries:
            await adaptive_backoff()

        # 8. Occasionally scavenge overflow (for orphaned inbox entries)
        if should_scavenge():
            await scavenge_overflow()
```

### Time Bucketing (Handles Scheduled Tasks)

```python
def time_bucket(dt: datetime) -> str:
    """Bucket by minute. Format: YYYYMMDD-HHMM"""
    return dt.strftime("%Y%m%d-%H%M")

def get_buckets_up_to(current_bucket: str, lookback_minutes: int = 60) -> list[str]:
    """Generate buckets from (current - lookback) to current, inclusive."""
    buckets = []
    end_dt = parse_bucket(current_bucket)
    start_dt = end_dt - timedelta(minutes=lookback_minutes)

    current = start_dt
    while current <= end_dt:
        buckets.append(time_bucket(current))
        current += timedelta(minutes=1)

    return buckets
```

This prevents repeatedly listing future tasks—if `available_at` is tomorrow, the inbox entry is in tomorrow's bucket.

**Note**: `get_buckets_up_to` is only for overflow/ready scavenging (bounded lookback is acceptable). Inbox scanning uses cursor-based approach (never skips).

### Cursor-Based Inbox Scanning (Prevents Missing Tasks)

**Problem**: Fixed lookback (e.g., 60 minutes) can miss tasks if worker was down longer.

**Solution**: Each worker maintains a cursor—the oldest bucket it has fully processed.

```python
async def poll_inbox(now: datetime) -> list[InboxEntry]:
    """Scan inbox from cursor to current time. Never skip buckets."""

    # Load cursor (persisted in S3)
    cursor = await load_cursor()  # e.g., "20240115-1030"
    current_bucket = time_bucket(now)

    if cursor is None:
        # First run - start from current bucket only
        # (don't scan entire history on fresh worker)
        cursor = current_bucket

    # Generate all buckets from cursor to now (inclusive)
    buckets = generate_bucket_range(cursor, current_bucket)

    entries = []
    for bucket in buckets:
        prefix = f"inbox/{worker_id}/{bucket}/"
        bucket_entries = await s3.list_objects(prefix)
        entries.extend(bucket_entries)

        # Update cursor as we complete each bucket
        if bucket != current_bucket:  # Don't advance past current
            await save_cursor(bucket)

    return entries

def generate_bucket_range(start: str, end: str) -> list[str]:
    """Generate all buckets between start and end (inclusive)."""
    buckets = []
    current = parse_bucket(start)
    end_dt = parse_bucket(end)

    while current <= end_dt:
        buckets.append(time_bucket(current))
        current += timedelta(minutes=1)

    return buckets

async def load_cursor() -> str | None:
    """Load worker's inbox cursor from S3."""
    try:
        data = await s3.get_object(f"workers/{worker_id}.cursor.json")
        return data["last_processed_bucket"]
    except NotFound:
        return None

async def save_cursor(bucket: str):
    """Persist cursor to S3."""
    await s3.put_object(
        f"workers/{worker_id}.cursor.json",
        json.dumps({"last_processed_bucket": bucket})
    )
```

**Guarantees**:
- Worker never skips a bucket (even after extended downtime)
- Fresh worker starts from current bucket (doesn't scan infinite history)
- Cursor persists across restarts

**Edge case**: Worker down for days, cursor is very old. First poll after restart scans many buckets. This is correct but potentially slow. Mitigation: cap bucket range at 24 hours and log warning if cursor is older.

### Dead Worker Recovery

**Key principle**: Never move canonical task objects. Only rebuild/move index entries.

```python
async def recover_dead_worker(dead_worker_id: str):
    """Called when a worker's lease expires without renewal."""

    # 1. List all inbox entries for dead worker
    prefix = f"inbox/{dead_worker_id}/"
    inbox_entries = await s3.list_objects(prefix)

    for entry in inbox_entries:
        # Parse bucket from key path: inbox/{worker}/{bucket}/{task_id}
        # Key format: "inbox/worker-1/20240115-1030/abc123"
        parts = entry.key.split("/")
        bucket = parts[2]  # Third segment is bucket
        task_id = parts[3]  # Fourth segment is task_id

        entry_data = json.loads(await s3.get_object(entry.key))
        task_key = f"tasks/{entry_data['shard']}/{task_id}.json"

        # 2. Check if task was actually claimed by this worker
        try:
            task, etag = await s3.get_object(task_key)
        except NotFound:
            # Task was deleted - just clean up inbox entry
            await s3.delete_object(entry.key)
            continue

        if task.status == "running" and task.claimed_by == dead_worker_id:
            # Task was in progress - let lease expiry handle it
            # (existing monitor/sweeper will reset to pending and write to ready/)
            pass

        elif task.status == "pending":
            # Task was never claimed - move index to overflow
            overflow_key = f"overflow/{bucket}/{task_id}"
            await s3.put_object(overflow_key, json.dumps(entry_data))

        # 3. Delete from dead worker's inbox
        await s3.delete_object(entry.key)

    # 4. Delete worker lease and cursor
    await s3.delete_object(f"workers/{dead_worker_id}.lease.json")
    await s3.delete_object(f"workers/{dead_worker_id}.cursor.json")
```

### Overflow Scavenging

```python
async def scavenge_overflow():
    """Claim overflow entries directly (don't copy to inbox)."""

    now = await queue.now()  # Authoritative time
    current_bucket = time_bucket(now)

    # Scan from oldest bucket to current (cursor-based would be better for large backlogs)
    for bucket in get_buckets_up_to(current_bucket, lookback_minutes=120):
        prefix = f"overflow/{bucket}/"
        entries = await s3.list_objects(prefix, limit=10)  # Limit per bucket

        for entry in entries:
            # Try to delete entry first to reduce duplicate claims
            try:
                # Note: vanilla S3 doesn't support conditional DELETE
                # On S3, this is best-effort; on MinIO with versioning, use If-Match
                await s3.delete_object(entry.key)
            except NotFound:
                # Another worker got it - skip
                continue

            # Parse entry (bucket is in key path, not body)
            entry_data = json.loads(entry.body)  # Body was fetched with list
            task_key = f"tasks/{entry_data['shard']}/{entry_data['id']}.json"

            # Try to claim directly (same as inbox processing)
            try:
                task, etag = await s3.get_object(task_key)
                if task.status != "pending":
                    # Already claimed - we already deleted overflow entry, done
                    continue

                if not task.is_available(now):
                    # Not ready yet - re-add to overflow (we deleted it prematurely)
                    await s3.put_object(entry.key, json.dumps(entry_data))
                    continue

                # Claim with CAS
                task.status = "running"
                task.claimed_by = worker_id
                task.lease_expires_at = now + lease_duration
                await s3.put_object_if_match(task_key, task.to_json(), etag)

                # Process task
                await process_task(task)

            except PreconditionFailed:
                # Another worker claimed the task - that's fine, overflow entry already deleted
                pass
```

**Note**: We delete the overflow entry before claiming to reduce duplicate work. If the task isn't claimable (not ready, already claimed), we re-add the entry or ignore. This is safe because CAS on the task object is the authoritative claim.

## Retry/Timeout Re-Dispatch (Critical)

**Problem**: When monitor/sweeper resets a task to `pending` (timeout, retry), there's no inbox entry. The task becomes invisible to direct dispatch workers.

**Solution**: Monitor writes to `ready/` index. Direct dispatch workers **must also poll `ready/`** for re-dispatched tasks.

```
Initial submit (direct dispatch):
  Producer → tasks/{shard}/{id}.json
           → inbox/{worker}/{bucket}/{id}

Task times out, monitor resets to pending:
  Monitor → tasks/{shard}/{id}.json (status=pending)
          → ready/{shard}/{bucket}/{id}  ← RE-DISPATCH INDEX

Worker claims re-dispatched task:
  Worker polls ready/ → finds entry → claims via CAS → deletes ready/ entry
```

### Monitor: Re-Dispatch to Ready Index

```python
async def reset_timed_out_task(task: Task, etag: str):
    """Called by monitor when task lease expires."""

    now = await queue.now()

    # 1. Reset task to pending (existing logic)
    task.status = "pending"
    task.claimed_by = None
    task.lease_expires_at = None
    task.attempt += 1
    task.available_at = now + task.retry_policy.calculate_delay(task.attempt)

    await s3.put_object_if_match(
        f"tasks/{task.shard}/{task.id}.json",
        task.to_json(),
        etag
    )

    # 2. Write to ready index for re-dispatch
    bucket = time_bucket(task.available_at)
    ready_key = f"ready/{task.shard}/{bucket}/{task.id}"
    await s3.put_object(ready_key, b"")  # Empty pointer
```

### Worker: Poll Both Inbox and Ready

```python
async def run():
    while not shutdown:
        now = await queue.now()

        # 1. Poll my inbox (primary, fast)
        inbox_tasks = await poll_inbox(now)

        # 2. Poll ready/ shards (for retries/timeouts/no-worker submits)
        ready_tasks = await poll_ready_shards(now)

        # 3. Process all
        for task in inbox_tasks + ready_tasks:
            await claim_and_process(task)

        if not inbox_tasks and not ready_tasks:
            await adaptive_backoff()
```

### Why Ready/ Instead of Overflow/?

`overflow/` is for orphaned inbox entries (dead worker recovery).
`ready/` is for tasks that need processing (retries, timeouts, no-worker submits).

Keeping them separate makes the semantics clear:
- `inbox/` = tasks routed directly to me
- `ready/` = tasks available to any worker (same as shared shard model)
- `overflow/` = index entries orphaned by dead workers

**Trade-off**: Direct dispatch workers must poll `ready/` shards, reducing the "no shared polling" benefit. But this only matters for retries/timeouts, which are (hopefully) minority cases.

## Worker Discovery (No Registry Manager)

The v1 design had a "registry manager" service—that's a control plane.

**Simpler approach**: Workers discover each other via lease files.

```python
async def get_active_workers() -> list[Worker]:
    """List workers by scanning lease files. Cache result."""

    # Check cache first (TTL 30s)
    if worker_cache.is_fresh():
        return worker_cache.workers

    now = await queue.now()  # Authoritative time

    # List all objects in workers/ prefix
    all_objects = await s3.list_objects("workers/")

    # Filter client-side for .lease.json files (S3 doesn't support suffix filtering)
    lease_keys = [obj.key for obj in all_objects if obj.key.endswith(".lease.json")]

    active = []
    for lease_key in lease_keys:
        try:
            lease = json.loads(await s3.get_object(lease_key))
            if parse_datetime(lease["expires_at"]) > now:
                active.append(Worker(id=lease["worker_id"]))
        except NotFound:
            # Lease deleted between list and get - skip
            continue

    worker_cache.update(active, now)
    return active
```

**Cost**: One LIST + N GETs per cache refresh. With 100 workers and 30s TTL:
- 2,880 LIST/day + 288,000 GET/day = ~$0.16/day

This is done by producers, not a separate service.

**Note**: S3 LIST doesn't support suffix filtering server-side. We list all objects under `workers/` and filter client-side. With workers also having `.cursor.json` files, the list is 2N objects. Still cheap.

## Costs Revisited

### Heartbeat (Lease Renewal)

Renewal every 20s × N workers:

| Workers | PUTs/day | Cost/day |
|---------|----------|----------|
| 10 | 43,200 | $0.22 |
| 100 | 432,000 | $2.16 |
| 1,000 | 4,320,000 | $21.60 |

At 1,000 workers, lease renewal is ~$650/month. That's significant.

**Mitigation**: Renew on activity, not timer. If worker is idle, extend renewal interval.

### Inbox Operations

Per task:
- Producer: 1 PUT (task) + 1 PUT (inbox entry) = 2 PUTs
- Worker: 1 LIST (inbox) + 1 GET (entry) + 1 GET (task) + 1 PUT (claim) + 1 DELETE (entry) = amortized ~2.5 ops/task

Similar to shared shards, but LIST is smaller (own inbox vs shared shard).

## Comparison (Honest)

| Aspect | Shared Shards | Direct Dispatch |
|--------|---------------|-----------------|
| Complexity | Simple | Moderate |
| Code added | — | ~500 lines |
| New concepts | — | Worker lease, inbox index, overflow, cursor |
| Task discovery | LIST ready/{shard}/ | LIST inbox/ + LIST ready/ |
| Contention | Workers race on claims | Reduced (own inbox for new tasks) |
| Retries/timeouts | Appear in ready/ | Appear in ready/ (same) |
| Failure recovery | Lease expiry | Lease expiry + inbox recovery |
| Producer dependency | None | Must discover workers |
| Horizontal scaling | Limited by shard count | Better (own inbox per worker) |

### "Zero polling" claim (corrected)

v1 said "zero polling overhead"—that was wrong.

Direct Dispatch still has:
- Workers LIST their inbox (primary)
- Workers LIST ready/ shards (for retries/timeouts)
- Workers LIST overflow (scavenging)
- Producers LIST/GET worker leases (cached)

It's **reduced** overhead for the happy path (new tasks go to inbox), not zero. The win is:
- Smaller LISTs for new tasks (own inbox vs shared shard)
- Less contention on new task claims
- But retries/timeouts still go through shared ready/ index

## Behavioral Rules (Clarifications)

### 1. Worker Loses Lease Mid-Task

**Rule**: If a worker loses its lease while processing a task, it **must stop immediately**.

```python
async def lease_renewal_loop(lease_key: str):
    while not shutdown:
        await asyncio.sleep(20)
        try:
            # ... renewal logic ...
        except (PreconditionFailed, NotFound):
            logger.error("Lost worker lease, shutting down immediately")
            # Cancel all in-progress tasks
            for task in active_tasks:
                task.cancel()
            trigger_shutdown()
            return
```

**Rationale**: If worker continues after losing lease, another worker might claim the same ID, and you get two workers with the same inbox. CAS on task objects prevents duplicate execution, but it's wasteful and confusing.

### 2. Overflow Scavenging Duplicates

**Rule**: Multiple workers may attempt to claim the same overflow entry. This is safe but wasteful.

```python
async def scavenge_overflow():
    for entry in overflow_entries:
        # Try to delete entry first (If-Match to avoid race)
        try:
            await s3.delete_object_if_match(entry.key, entry.etag)
        except PreconditionFailed:
            # Another worker got it - skip
            continue

        # We won the race - now claim the task
        await claim_and_process(entry)
```

**Optimization**: Delete overflow entry with `If-Match` before claiming. First worker to delete wins; others skip. Reduces wasted claims.

### 3. Direct Dispatch vs Ready Index

**Rule**: In direct dispatch mode, workers poll BOTH their inbox AND ready/ shards.

| Index | Written By | Contains |
|-------|-----------|----------|
| `inbox/{worker}/` | Producer (on submit) | Tasks routed directly to worker |
| `ready/{shard}/` | Monitor (on retry/timeout), Producer (no workers available) | Tasks available to any worker |
| `overflow/` | Recovery (dead worker inbox) | Orphaned inbox entries |

```python
async def run():
    while not shutdown:
        # Primary: my inbox
        inbox_tasks = await poll_inbox()

        # Secondary: ready shards (retries, timeouts, overflow)
        ready_tasks = await poll_ready_shards()

        # Tertiary: overflow (orphaned entries)
        if should_scavenge():
            overflow_tasks = await scavenge_overflow()
```

### 4. Fresh Worker Inbox Cursor

**Rule**: A fresh worker (no cursor) starts from current bucket, not from the beginning of time.

```python
cursor = await load_cursor()
if cursor is None:
    # Fresh worker - don't scan infinite history
    cursor = time_bucket(now)
```

**Rationale**: A new worker shouldn't inherit another worker's backlog. If tasks were in a dead worker's inbox, recovery moves them to overflow, where any worker can claim them.

### 5. Who Triggers Dead Worker Recovery?

**Rule**: Any worker can trigger recovery for a dead worker when it encounters an expired lease.

```python
async def get_active_workers():
    for lease_key in lease_keys:
        lease = await s3.get_object(lease_key)
        if parse_datetime(lease["expires_at"]) <= now:
            # Expired lease - trigger recovery (idempotent)
            await recover_dead_worker(lease["worker_id"])
```

Recovery is idempotent—multiple workers triggering it simultaneously is safe (CAS on deletes, best-effort moves).

## When to Use Direct Dispatch

**Use shared shards (default) when:**
- < 50 workers
- < 100K tasks/day
- Simplicity is priority

**Consider direct dispatch when:**
- Many workers competing on same shards
- Claim contention is measurable problem
- Willing to accept added complexity

## Configuration

```toml
[queue]
dispatch_mode = "shared"  # Default, or "direct"

[queue.direct_dispatch]
worker_discovery_cache_ttl_secs = 30
worker_selection = "round_robin"  # or "hash", "random"

[worker]
lease_duration_secs = 60
lease_renewal_interval_secs = 20
overflow_scavenge_interval_secs = 120
```

## Open Questions

1. **Inbox entry size**: Should entries include task metadata (type, priority) for smarter worker-side filtering?

2. **Multi-inbox workers**: Should a worker be able to own multiple inboxes (for weighted load balancing)?

3. **Inbox TTL**: Should old inbox buckets be auto-cleaned? Or rely on scavenging?

4. **Shared + Direct hybrid**: Can some task types use shared shards while others use direct dispatch?

## Implementation Estimate

| Component | Lines (Rust) |
|-----------|--------------|
| Worker lease (create/renew/check) | ~80 |
| Inbox write (producer) | ~40 |
| Inbox read + claim (worker) | ~60 |
| Time bucketing | ~30 |
| Cursor management | ~50 |
| Worker discovery | ~60 |
| Overflow scavenging | ~60 |
| Dead worker recovery | ~80 |
| Monitor re-dispatch to ready/ | ~40 |
| **Total** | **~500** |

Plus tests: ~250 lines.

## Summary

Direct Dispatch is **inbox as index**:
- Tasks stay canonical in `tasks/` (auditability preserved)
- Inbox entries are pointers, not data
- Claims still use CAS on task object
- Worker IDs protected by S3-enforced leases
- All time decisions use authoritative S3 time
- Retries/timeouts go to ready/ (workers must poll both inbox and ready)
- Cursor-based inbox scanning (never skips buckets)
- Dead worker recovery moves index entries, not tasks

It reduces contention for **new tasks** and improves horizontal scaling, but adds ~500 lines of code and several new concepts. It's an opt-in feature for users who hit scaling limits with shared shards.

**Honest trade-off**: The benefit is smaller for workloads with high retry/timeout rates, since those tasks still go through shared ready/ index. Direct Dispatch shines when most tasks succeed on first attempt.
