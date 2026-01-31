# BUG-010: Ready index listing should filter by time buckets

Status: Resolved

## Summary
Workers still list the entire `ready/{shard}/` prefix, which includes future buckets. This inflates LIST/GET cost and can trigger unnecessary log-scan fallbacks.

## Impact
High request volume and wasted work, especially when many tasks are scheduled for the future (retries/backoff).

## Example
A task scheduled 2 hours out is repeatedly listed and fetched by every worker on every poll.

## Proposed Fix
Implemented. See "Resolution" below.

## Resolution
- Ready listing now targets a specific bucket prefix: `ready/{shard}/{bucket}/`.
- A per-shard cursor tracks `(bucket, continuation_token)` and advances sequentially.
- Initial bucket is derived from the oldest ready key (lexicographic order) or falls back to current S3 time.
- All bucket comparisons use authoritative S3 time from `Queue::now()`.
