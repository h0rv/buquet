# BUG-003: Shard starvation due to fixed scan order

Status: Resolved

## Summary
Workers iterate shards in a fixed order and return after the first claim. Hot shards can starve later shards indefinitely.

## Impact
Unbounded latency for tasks in later shards; fairness is not guaranteed.

## Example
Shard “0” is hot and always has work. Shard “f” has a few tasks that never get claimed.

## Proposed Fix
Implemented. See "Resolution" below.

## Resolution
- Added a per-worker shard cursor to rotate the starting shard each poll.
- Ensures fairness even when early shards are hot.
