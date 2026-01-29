# Feature: Queue Prefixes (Namespaces)

## Status: Proposed

## Overview
Allow a queue to operate under a configurable key prefix (namespace) so multiple
logical queues can share the same S3 bucket safely. This improves test
isolation, multi-tenant usage, and staged environments without adding
infrastructure.

## Philosophy Alignment

| Principle | How This Aligns |
|-----------|-----------------|
| S3 as only dependency | Prefixing is just a key path change |
| Simple, boring, reliable | No new services or coordination |
| Debuggable | Objects remain directly inspectable |
| Explicit > implicit | Prefix is explicit config |

## Problem
- Tests can collide when sharing a bucket.
- Multi-tenant or multi-env deployments need separation.
- Today, separation requires separate buckets or ad-hoc conventions.

## Solution
Add an optional `prefix` to `QueueConfig` that is prepended to all storage
paths. When unset, behavior is unchanged.

## Storage Layout
With `prefix = "tenant-a"`:
```
{prefix}/tasks/{shard}/{task_id}.json
{prefix}/ready/{shard}/{bucket}/{task_id}
{prefix}/leases/{shard}/{bucket}/{task_id}
{prefix}/workers/{worker_id}.json
{prefix}/schedules/{schedule_id}.json
```

## API Sketch

### Rust
```rust
let queue = Queue::connect_with_prefix("my-bucket", "tenant-a").await?;
```

### Python
```python
queue = await connect(bucket="my-bucket", prefix="tenant-a")
```

### CLI
```bash
oq --prefix tenant-a submit -t send_email -i '{}'
```

## Behavior
- Prefix is purely a namespacing key; semantics are unchanged.
- Prefix is part of all key builders (tasks, indexes, schedules, workers, etc.).
- Multiple prefixes can safely coexist in one bucket.

## Migration / Compatibility
- Default prefix is empty (backward compatible).
- Can be introduced incrementally by creating a new prefix and moving clients.

## Related
- Test isolation (ongoing pain point in integration tests)
- Workflow engine benefits from isolated namespaces for multi-env runs
