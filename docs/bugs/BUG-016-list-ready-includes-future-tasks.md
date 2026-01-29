# BUG-016: Queue::list_ready includes future scheduled tasks

Status: Resolved

## Summary
`Queue::list_ready` lists all keys under `ready/{shard}/` and returns task ids from all buckets, including tasks scheduled in the future. The docstring says "ready for claiming", which is not true in this implementation.

## Impact
Admin tools and monitoring can misreport the active backlog and attempt to claim tasks that are not yet available. This can also cause unnecessary work if callers assume readiness.

## Example
A task scheduled 2 hours in the future appears in `list_ready` immediately because its ready index key exists.

## Location
- `crates/oq/src/queue/ops.rs:325-349`

## Proposed Fix
Filter buckets by authoritative time (S3 time) so only buckets <= now are returned. Alternatively, add a parameter to include future buckets and update the docstring accordingly.

Add a test that verifies future tasks are excluded by default.

## Resolution
- `Queue::list_ready` now filters out future buckets using authoritative S3 time.
