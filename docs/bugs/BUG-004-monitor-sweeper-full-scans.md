# BUG-004: Monitor/sweeper perform O(total objects) scans

Status: Fixed

## Summary
Timeout monitor lists all lease keys each interval and GETs each task; sweeper lists all ready/lease/task keys and scans everything.

## Impact
High LIST/GET cost, throttling risk, and memory pressure at medium/large scale.

## Example
Millions of tasks with frequent monitor intervals generate excessive S3 requests and can starve workers.

## Fix Applied

Implemented time-bucketed scans with incremental cursors for both the Monitor and Sweeper.

### Monitor Changes (`crates/buquet/src/worker/monitor.rs`)

1. Added `ShardCursor` struct to track scan position per shard:
   - `continuation_token`: S3 LIST continuation token
   - `completed_full_scan`: Whether at least one full scan has completed

2. Added `max_tasks_per_scan` config (default: 100) to limit work per cycle

3. Changed `check_shard()` to use incremental scanning:
   - Lists only `max_tasks_per_scan` lease keys per call
   - Uses S3 continuation tokens to resume from previous position
   - Maintains cursor state in `shard_cursors` HashMap

4. Added helper methods:
   - `get_cursor(shard)`: Get current cursor state
   - `reset_cursor(shard)`: Force fresh scan for a shard
   - `reset_all_cursors()`: Reset all shards
   - `all_shards_scanned()`: Check if all shards completed at least one full scan

### Sweeper Changes (`crates/buquet/src/worker/sweeper.rs`)

1. Added `SweepMode` enum:
   - `Continuous` (default): Incremental sweeping with cursors
   - `OnDemand`: Full sweep only when triggered

2. Added `SweeperCursor` struct:
   - `phase`: 0=ready index, 1=lease index, 2=task objects
   - `continuation_token`: S3 LIST continuation token
   - `completed_full_scan`: Whether all phases completed

3. Added config options:
   - `max_objects_per_sweep` (default: 1000)
   - `sweep_mode` (default: Continuous)

4. Separated sweep logic:
   - `sweep_shard_full()`: Original full-scan behavior (for OnDemand mode)
   - `sweep_shard_incremental()`: New incremental behavior

5. Added `truncated` field to `SweepReport` to indicate partial scans

### Config Changes (`crates/buquet/src/config.rs`)

Added new environment variables:
- `BUQUET_MONITOR_MAX_TASKS_PER_SCAN`: Limit tasks per monitor cycle
- `BUQUET_SWEEPER_MAX_OBJECTS_PER_SWEEP`: Limit objects per sweeper cycle
- `BUQUET_SWEEPER_MODE`: "continuous" or "on_demand"

Added new config fields to `MonitorConfig`:
- `max_tasks_per_scan: Option<usize>`
- `max_objects_per_sweep: Option<usize>`
- `sweep_mode: Option<String>`

### Complexity Analysis

Before: O(N) per cycle where N = total objects in queue
After: O(max_tasks_per_scan) per cycle, eventually covering all objects

The incremental approach spreads work across multiple cycles while ensuring
all objects are eventually scanned. Cursor state is kept in memory and
resets on restart, which is acceptable since the sweeper/monitor will
naturally re-scan from the beginning.
