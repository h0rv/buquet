# ISSUE-002: LIST-bound latency and cost are fundamental

Status: Known limitation

## Summary
Task discovery relies on S3 LIST operations. LIST cost and latency scale with
active index size, and can become the dominant cost at medium/large scale.

## Impact
- Higher per-task latency when indexes are large.
- Request costs scale with task volume and shard count.
- Monitor/sweeper can become a bottleneck if scans are naive.

## Why This Is Fundamental
S3 is an object store, not a message broker. LIST is the only discovery
mechanism, and it is O(number of objects under a prefix).

## Mitigations
- Adaptive polling (already implemented).
- Time-bucketed indexes + cursors (already implemented for ready/).
- Shard leasing to reduce per-worker LISTs (`docs/features/shard-leases.md`).
- Conservative monitor scan strategies (incremental cursors, limited buckets).

## Notes
This is a tradeoff aligned with the project's goals: simplicity and auditability
in exchange for non-real-time latency.
