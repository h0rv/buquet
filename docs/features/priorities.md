# Feature: Task Priorities

## Status: Proposed

## Overview

Add priority levels to tasks so urgent work gets processed before background
tasks. Workers claim highest-priority tasks first within each shard.

## Problem

All tasks are currently equal — a critical alert waits behind a batch of
low-priority cleanup jobs. Users need control over execution order when the
queue backs up.

## Solution

Add a `priority` field to tasks. Modify the ready index to include priority,
so workers naturally claim high-priority tasks first via S3 list ordering.

## Priority Levels

```
Priority 255 (highest) — Critical/emergency
Priority 200          — Urgent
Priority 128          — Normal (default)
Priority 50           — Low
Priority 0   (lowest) — Background/bulk
```

Using `u8` (0-255) provides granularity while keeping storage compact.
Default is 128 (middle) to allow both up and down adjustments.

## Storage Layout

### Ready Index Change

Current:
```
ready/{shard}/{timestamp}/{task_id}
```

With priorities:
```
ready/{shard}/{priority_inverted:03}/{timestamp}/{task_id}
```

**Priority is inverted** (255 - priority) so that S3's ascending list order
returns highest priority first:

```
ready/0/000/2026-01-27T12:00:00Z/abc  # priority 255 (255-255=000)
ready/0/055/2026-01-27T12:00:00Z/def  # priority 200 (255-200=055)
ready/0/127/2026-01-27T12:00:00Z/ghi  # priority 128 (255-128=127)
ready/0/205/2026-01-27T12:00:00Z/jkl  # priority 50  (255-50=205)
ready/0/255/2026-01-27T12:00:00Z/mno  # priority 0   (255-0=255)
```

### Task Object

```json
{
  "id": "abc123",
  "task_type": "send_alert",
  "priority": 255,
  "status": "pending",
  ...
}
```

## API

### Python

```python
from qo import connect, Priority

queue = await connect(bucket="my-queue")

# High priority (critical alert)
task = await queue.submit(
    "send_alert",
    {"message": "Server down!"},
    priority=255
)

# Normal priority (default)
task = await queue.submit("send_email", {"to": "user@example.com"})

# Low priority (background job)
task = await queue.submit(
    "cleanup_old_files",
    {"days_old": 30},
    priority=50
)

# Named constants for clarity
task = await queue.submit("urgent_task", data, priority=Priority.URGENT)
task = await queue.submit("background_task", data, priority=Priority.LOW)
```

Priority constants:
```python
class Priority:
    CRITICAL = 255
    URGENT = 200
    HIGH = 175
    NORMAL = 128  # default
    LOW = 50
    BACKGROUND = 10
    BULK = 0
```

### Rust

```rust
use qo::{Queue, Priority};

let queue = Queue::connect("my-queue").await?;

// High priority
let task = queue
    .submit("send_alert")
    .input(json!({"message": "Server down!"}))
    .priority(Priority::CRITICAL)  // or .priority(255)
    .send()
    .await?;

// Normal priority (default)
let task = queue
    .submit("send_email")
    .input(json!({"to": "user@example.com"}))
    .send()
    .await?;

// Low priority
let task = queue
    .submit("cleanup_old_files")
    .input(json!({"days_old": 30}))
    .priority(Priority::LOW)
    .send()
    .await?;
```

### CLI

```bash
# High priority
qo submit -t send_alert -i '{"message":"Server down!"}' --priority 255
qo submit -t send_alert -i '{"message":"Server down!"}' --priority critical

# Normal priority (default)
qo submit -t send_email -i '{"to":"user@example.com"}'

# Low priority
qo submit -t cleanup -i '{}' --priority 50
qo submit -t cleanup -i '{}' --priority low

# Check task priority
qo get abc123 --json | jq .priority
```

Named priority flags:
- `--priority critical` → 255
- `--priority urgent` → 200
- `--priority high` → 175
- `--priority normal` → 128
- `--priority low` → 50
- `--priority background` → 10
- `--priority bulk` → 0

## Worker Behavior

Workers claim tasks in priority order automatically — the ready index ordering
handles this. No worker code changes needed beyond the index key format.

```rust
// Worker claim loop (simplified)
let ready_keys = s3.list_objects("ready/{shard}/", limit=10).await?;
// Returns highest priority first due to inverted prefix

for key in ready_keys {
    if let Ok(task) = try_claim(key).await {
        return Some(task);
    }
}
```

## Priority Aging (Anti-Starvation)

Low-priority tasks could starve if high-priority tasks keep arriving. Optional
aging bumps priority over time:

```json
{
  "id": "abc123",
  "priority": 50,
  "priority_age_rate": 1,
  "created_at": "2026-01-27T12:00:00Z"
}
```

Effective priority = `min(255, base_priority + age_minutes * age_rate)`

A task created at priority 50 with age_rate=1 would reach priority 128 after
78 minutes, ensuring it eventually gets processed.

### Configuration

```toml
# .qo.toml
[worker]
priority_aging = true
priority_age_rate = 1  # priority points per minute
priority_age_cap = 200  # max effective priority from aging
```

## Priority Tiers (Alternative Design)

For simpler implementation, use discrete tiers instead of 256 levels:

```
ready/0/high/{timestamp}/{task_id}
ready/0/normal/{timestamp}/{task_id}
ready/0/low/{timestamp}/{task_id}
```

Workers check `high/` first, then `normal/`, then `low/`. Simpler but less
granular.

**Recommendation:** Start with discrete tiers, add numeric priorities later
if needed.

## Per-Task-Type Priorities

Set default priority per task type:

```python
# In worker registration
@worker.task("cleanup_old_files", default_priority=Priority.LOW)
async def handle_cleanup(input):
    ...

@worker.task("send_alert", default_priority=Priority.CRITICAL)
async def handle_alert(input):
    ...
```

Submitted tasks inherit the default unless explicitly overridden.

## Guarantees

- **Ordering within shard:** Highest priority first, then by timestamp (FIFO)
- **Ordering across shards:** Not guaranteed (shards are independent)
- **Priority is immutable:** Cannot change priority after submission

## Limitations

- **No cross-shard ordering:** A high-priority task in shard 3 won't preempt
  a low-priority task being processed in shard 1
- **Not real-time:** Worker must finish current task before claiming next
- **S3 list latency:** ~100ms overhead per claim cycle

## Migration

Existing tasks without priority field are treated as priority 128 (normal).
Ready index keys without priority prefix are listed after all prioritized keys.

```
# Migration order: new format first, then legacy
ready/0/127/2026-01-27T12:00:00Z/new_task    # new format
ready/0/2026-01-27T12:00:00Z/old_task        # legacy (no priority)
```

## Implementation Phases

### Phase 1: Discrete Tiers
- Three tiers: high, normal, low
- Simple prefix-based ready index
- CLI `--priority high|normal|low`

### Phase 2: Numeric Priorities
- Full 0-255 range
- Inverted numeric prefix in ready index
- API accepts both names and numbers

### Phase 3: Priority Aging
- Anti-starvation mechanism
- Configurable age rate
- Effective priority calculation

## See Also

- [Scheduling](scheduling.md) — Scheduled tasks can have priorities
- [Rate Limiting](rate-limiting.md) — Rate limits apply within priority tiers
