# Feature: Configurable Shards

## Status: Implemented

## Overview

Allow users to configure the number of shards (S3 prefixes) used for task storage. More shards = better parallelism and smaller LIST operations, at the cost of more complexity in worker assignment.

## Current Behavior

Shards are hardcoded to 16 (single hex character: `0-f`):

```
tasks/
├── 0/   ← ~6.25% of tasks
├── 1/
├── 2/
...
├── e/
└── f/
```

With UUID v4, tasks distribute uniformly across shards.

## Problem

At scale, 16 shards becomes a bottleneck:

| Tasks in queue | Tasks per shard | LIST cost per poll |
|----------------|-----------------|-------------------|
| 1,000 | 62 | Negligible |
| 100,000 | 6,250 | Noticeable |
| 1,000,000 | 62,500 | Expensive |

S3 LIST returns max 1000 objects per request. With 62,500 tasks per shard, that's 63 LIST requests just to enumerate one shard.

## Solution

Make shard count configurable via prefix length:

| Prefix Length | Shards | Tasks per shard (1M total) |
|---------------|--------|---------------------------|
| 1 | 16 | 62,500 |
| 2 | 256 | 3,906 |
| 3 | 4,096 | 244 |

## Configuration

### Queue-Level Setting

Shard count is a **queue property**, not a worker property. All producers and workers must agree on the shard count, otherwise tasks will be written to wrong shards or missed entirely.

### Rust

```rust
use oq::queue::{Queue, QueueConfig};

let config = QueueConfig {
    shard_prefix_len: 2,  // 256 shards (00-ff)
    ..Default::default()
};

let queue = Queue::with_config(client, config);
```

### Python

```python
from oq import connect

queue = await connect(
    bucket="my-tasks",
    shard_prefix_len=2,  # 256 shards
)
```

### Config File (oq.toml)

```toml
[queue]
shard_prefix_len = 2  # 1=16 shards, 2=256 shards, 3=4096 shards
```

### Environment Variable

```bash
OQ_SHARD_PREFIX_LEN=2
```

## Shard Generation

### Current

```rust
pub fn shard_from_id(id: &Uuid) -> String {
    id.to_string().chars().next().unwrap().to_string()
}
// UUID: a1b2c3d4-... → Shard: "a"
```

### Proposed

```rust
pub fn shard_from_id(id: &Uuid, prefix_len: usize) -> String {
    id.to_string()
        .chars()
        .filter(|c| *c != '-')  // Skip hyphens
        .take(prefix_len)
        .collect()
}
// prefix_len=1: a1b2c3d4-... → "a"
// prefix_len=2: a1b2c3d4-... → "a1"
// prefix_len=3: a1b2c3d4-... → "a1b"
```

## Worker Shard Assignment

Workers need to know which shards to poll. With more shards, assignment becomes important.

### All Shards (Simple, Default)

```rust
// Worker polls all shards
let worker = Worker::new(queue, "worker-1", None);  // None = all shards
```

### Explicit Assignment

```rust
// Worker polls specific shards
let my_shards = vec!["00", "01", "02", "03"];
let worker = Worker::new(queue, "worker-1", Some(my_shards));
```

### Range Assignment (Helper)

```rust
// Generate shard range for this worker
let all_shards = Queue::all_shards(prefix_len);  // ["00", "01", ..., "ff"]
let my_shards = all_shards
    .chunks(all_shards.len() / num_workers)
    .nth(worker_index)
    .unwrap();
```

### Config-Driven Assignment

```toml
# oq.toml
[worker]
# Explicit list
shards = ["00", "01", "02", "03"]

# Or range (proposed syntax)
shard_range = "00-3f"  # First quarter of 256 shards
```

## S3 Key Format

No change to key format—shard is just longer:

```
# Current (prefix_len=1)
tasks/a/a1b2c3d4-e5f6-7890-abcd-ef1234567890.json

# With prefix_len=2
tasks/a1/a1b2c3d4-e5f6-7890-abcd-ef1234567890.json

# With prefix_len=3
tasks/a1b/a1b2c3d4-e5f6-7890-abcd-ef1234567890.json
```

## Trade-offs

| Shards | Pros | Cons |
|--------|------|------|
| 16 | Simple, easy to reason about | Large LISTs at scale |
| 256 | Good parallelism, manageable | Need to assign shards to workers |
| 4096 | Excellent parallelism | Complex assignment, many empty shards at low volume |

### Recommendation by Scale

| Tasks/day | Recommended shards | Reasoning |
|-----------|-------------------|-----------|
| < 10,000 | 16 (default) | Simple, LIST is cheap |
| 10,000 - 1,000,000 | 256 | Balance of parallelism and simplicity |
| > 1,000,000 | 4096 | Minimize LIST size |

## Migration

**Important**: Changing shard count on an existing queue is a breaking change. Existing tasks will be in old shard paths and won't be found.

### Migration Options

1. **Drain first**: Process all existing tasks, then change shard count
2. **Dual-read**: Temporarily read from both old and new shard paths (complex)
3. **New queue**: Create new queue with new shard count, migrate producers first

### Recommended Approach

```bash
# 1. Stop producers
# 2. Wait for queue to drain (all tasks completed)
# 3. Update config with new shard_prefix_len
# 4. Restart workers with new config
# 5. Restart producers
```

## Implementation

### Files to Modify

```
crates/oq/src/queue/
├── config.rs      # Add QueueConfig with shard_prefix_len
└── ops.rs         # Pass prefix_len to shard_from_id

crates/oq/src/models/
└── task.rs        # Update shard_from_id signature

crates/oq/src/worker/
└── mod.rs         # Generate shard list based on prefix_len

crates/oq/src/python/
├── queue.rs       # Expose shard_prefix_len in connect()
└── worker.rs      # Handle shard assignment

crates/oq/src/config.rs      # Config file parsing
```

### New Types

```rust
/// Queue configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueConfig {
    /// Number of hex characters for shard prefix.
    /// 1 = 16 shards, 2 = 256 shards, 3 = 4096 shards.
    /// Default: 1
    #[serde(default = "default_shard_prefix_len")]
    pub shard_prefix_len: usize,
}

fn default_shard_prefix_len() -> usize {
    1
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            shard_prefix_len: 1,
        }
    }
}
```

### Helper Functions

```rust
impl Queue {
    /// Generate all shard prefixes for a given prefix length.
    pub fn all_shards(prefix_len: usize) -> Vec<String> {
        let count = 16_usize.pow(prefix_len as u32);
        (0..count)
            .map(|i| format!("{:0>width$x}", i, width = prefix_len))
            .collect()
    }
}

// Queue::all_shards(1) → ["0", "1", ..., "f"]
// Queue::all_shards(2) → ["00", "01", ..., "ff"]
// Queue::all_shards(3) → ["000", "001", ..., "fff"]
```

## Validation

```rust
impl QueueConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.shard_prefix_len == 0 {
            return Err(ConfigError::InvalidShardPrefixLen(
                "shard_prefix_len must be at least 1".into()
            ));
        }
        if self.shard_prefix_len > 4 {
            return Err(ConfigError::InvalidShardPrefixLen(
                "shard_prefix_len > 4 creates 65536+ shards, probably not what you want".into()
            ));
        }
        Ok(())
    }
}
```

## Defaults

```rust
QueueConfig {
    shard_prefix_len: 1,  // 16 shards, backward compatible
}
```

## Testing

1. **Unit tests**: Verify `shard_from_id` with different prefix lengths
2. **Unit tests**: Verify `all_shards` generates correct lists
3. **Integration test**: Submit and retrieve tasks with prefix_len=2
4. **Distribution test**: Verify tasks distribute uniformly across shards

```rust
#[test]
fn test_shard_distribution_is_uniform() {
    let mut counts: HashMap<String, usize> = HashMap::new();
    for _ in 0..10000 {
        let id = Uuid::new_v4();
        let shard = Task::shard_from_id(&id, 2);
        *counts.entry(shard).or_default() += 1;
    }
    // With 256 shards and 10000 tasks, expect ~39 per shard
    // Allow 50% variance (20-60 per shard)
    for count in counts.values() {
        assert!(*count > 20 && *count < 60);
    }
}
```

## Documentation

Add to getting-started.md:

```markdown
## Scaling with Shards

By default, oq uses 16 shards. For high-volume queues, increase the shard count:

​```python
queue = await connect(
    bucket="my-tasks",
    shard_prefix_len=2,  # 256 shards
)
​```

| Scale | Recommended `shard_prefix_len` |
|-------|-------------------------------|
| < 10K tasks/day | 1 (default, 16 shards) |
| 10K - 1M tasks/day | 2 (256 shards) |
| > 1M tasks/day | 3 (4096 shards) |

**Note**: All producers and workers must use the same shard configuration.
Changing shard count requires draining the queue first.
```

## Estimation

~50 lines of Rust, ~20 lines of Python bindings, ~10 lines of config parsing.
