# Feature: Task Tagging / Metadata

## Status: Proposed

## Overview

Attach arbitrary key-value tags to tasks for filtering, grouping, and organization.
Tags are indexed for efficient querying without scanning all tasks.

## Philosophy Alignment

| Principle | How This Aligns |
|-----------|-----------------|
| S3 as only dependency | Tags stored in task object, optional tag index |
| Simple, boring, reliable | Just a map field, standard pattern |
| No coordination services | No external tag service |
| Crash-safe | Tags persisted with task |
| Debuggable | `oq list --tag customer=acme` shows filtered tasks |

## Problem

- Can't group related tasks (e.g., all tasks for a customer)
- Can't filter tasks by business attributes
- Hard to find tasks without knowing IDs
- Currently: encode metadata in task_type or input (hacky)

## Solution

Add `tags: Map<String, String>` field to tasks. Optionally index tags for
efficient filtering.

## API

### Python

```python
from oq import connect

queue = await connect(bucket="my-queue")

# Submit with tags
task = await queue.submit(
    "process_order",
    {"order_id": "12345"},
    tags={
        "customer": "acme-corp",
        "region": "us-west",
        "priority": "high",
        "batch_id": "batch-2026-01-28",
    }
)

# Get task (tags included)
task = await queue.get(task_id)
print(task.tags)  # {"customer": "acme-corp", ...}

# List with tag filter (single tag)
tasks = await queue.list(tags={"customer": "acme-corp"})

# List with multiple tags (AND)
tasks = await queue.list(tags={"customer": "acme-corp", "region": "us-west"})

# List with tag existence check
tasks = await queue.list(tags={"batch_id": None})  # has batch_id, any value

# Update tags on existing task (pending only)
await queue.update_tags(task_id, {"priority": "critical"})

# Remove a tag
await queue.remove_tags(task_id, ["priority"])
```

### Rust

```rust
use oq::Queue;
use std::collections::HashMap;

let queue = Queue::connect("my-queue").await?;

// Submit with tags
let mut tags = HashMap::new();
tags.insert("customer".to_string(), "acme-corp".to_string());
tags.insert("region".to_string(), "us-west".to_string());

let task = queue
    .submit("process_order")
    .input(json!({"order_id": "12345"}))
    .tags(tags)
    .send()
    .await?;

// List with tag filter
let tasks = queue
    .list()
    .tags([("customer", "acme-corp")])
    .send()
    .await?;
```

### CLI

```bash
# Submit with tags
oq submit -t process_order -i '{"order_id":"12345"}' \
    --tag customer=acme-corp \
    --tag region=us-west \
    --tag priority=high

# List with tag filter
oq list --tag customer=acme-corp
oq list --tag customer=acme-corp --tag region=us-west

# List tasks that have a tag (any value)
oq list --has-tag batch_id

# Show tags in output
oq list --tag customer=acme-corp --format json | jq '.[].tags'

# Update tags
oq tag abc123 priority=critical

# Remove tags
oq untag abc123 priority
```

## Task Object

```json
{
  "id": "abc123",
  "task_type": "process_order",
  "status": "pending",
  "input": {"order_id": "12345"},
  "tags": {
    "customer": "acme-corp",
    "region": "us-west",
    "priority": "high",
    "batch_id": "batch-2026-01-28"
  },
  ...
}
```

## Tag Constraints

| Constraint | Value |
|------------|-------|
| Max tags per task | 20 |
| Max key length | 64 characters |
| Max value length | 256 characters |
| Allowed key chars | `a-z`, `0-9`, `-`, `_` (lowercase only) |
| Allowed value chars | Any UTF-8, no newlines |

Reserved tag prefixes:
- `oq_` - Reserved for system use
- `_` - Reserved for internal use

## Storage Options

### Option A: No Index (Simple)

Tags stored only in task object. Filtering requires scanning tasks.

**Pros:** No extra storage, no index maintenance
**Cons:** Slow filtering for large queues

```rust
// List with tag filter (scans tasks)
pub async fn list_with_tags(&self, tags: &HashMap<String, String>) -> Result<Vec<Task>> {
    let all_tasks = self.list_all().await?;
    Ok(all_tasks.into_iter().filter(|t| matches_tags(t, tags)).collect())
}
```

### Option B: Tag Index (Recommended)

Maintain index objects for efficient tag queries:

```
tags/{tag_key}/{tag_value}/{task_id}
```

Example:
```
tags/customer/acme-corp/abc123
tags/customer/acme-corp/def456
tags/region/us-west/abc123
tags/priority/high/abc123
```

**Pros:** O(1) tag lookup, efficient filtering
**Cons:** Extra S3 writes on submit, index cleanup needed

```rust
// Submit with tag index
pub async fn submit_with_tags(&self, ..., tags: HashMap<String, String>) -> Result<Task> {
    let task = self.submit_task(...).await?;

    // Create tag index entries (best-effort)
    for (key, value) in &tags {
        let index_key = format!("tags/{}/{}/{}", key, value, task.id);
        self.client.put_object(&index_key, &[]).await.ok();
    }

    Ok(task)
}

// List by tag (uses index)
pub async fn list_by_tag(&self, key: &str, value: &str) -> Result<Vec<Uuid>> {
    let prefix = format!("tags/{}/{}/", key, value);
    let keys = self.client.list_objects(&prefix, 1000).await?;
    Ok(keys.iter().map(|k| parse_task_id(k)).collect())
}
```

### Recommendation

Start with **Option A** (no index). Add tag index later if filtering performance
becomes an issue. Tag data is always in the task object, so index is purely
for query optimization.

## Tag Operations

### Add/Update Tags

```rust
pub async fn update_tags(&self, task_id: Uuid, tags: HashMap<String, String>) -> Result<Task> {
    let (mut task, etag) = self.get(task_id).await?.ok_or(NotFound)?;

    // Only allow tag updates on pending tasks
    if task.status != TaskStatus::Pending {
        return Err(CannotUpdateTags { status: task.status });
    }

    // Merge tags
    for (key, value) in tags {
        task.tags.insert(key, value);
    }
    task.updated_at = self.now().await?;

    self.put_task_if_match(&task, &etag).await?;
    Ok(task)
}
```

### Remove Tags

```rust
pub async fn remove_tags(&self, task_id: Uuid, keys: &[String]) -> Result<Task> {
    let (mut task, etag) = self.get(task_id).await?.ok_or(NotFound)?;

    if task.status != TaskStatus::Pending {
        return Err(CannotUpdateTags { status: task.status });
    }

    for key in keys {
        task.tags.remove(key);
    }
    task.updated_at = self.now().await?;

    self.put_task_if_match(&task, &etag).await?;
    Ok(task)
}
```

## Use Cases

### 1. Customer Isolation

```python
# Submit tasks tagged with customer
await queue.submit("process", data, tags={"customer_id": customer.id})

# Worker only processes tasks for specific customers
tasks = await queue.list(tags={"customer_id": "cust-123"}, status="pending")
```

### 2. Batch Processing

```python
batch_id = f"batch-{datetime.now().isoformat()}"

# Submit all tasks in batch
for item in items:
    await queue.submit("process_item", item, tags={"batch_id": batch_id})

# Check batch progress
all_tasks = await queue.list(tags={"batch_id": batch_id})
completed = [t for t in all_tasks if t.status == "completed"]
print(f"Batch progress: {len(completed)}/{len(all_tasks)}")
```

### 3. Environment Tagging

```python
# Tag tasks with environment
env = os.getenv("ENVIRONMENT", "dev")
await queue.submit("task", data, tags={"env": env})

# Workers filter by environment
tasks = await queue.list(tags={"env": "production"})
```

### 4. A/B Testing

```python
# Tag tasks with experiment variant
variant = "control" if random.random() < 0.5 else "treatment"
await queue.submit("process", data, tags={"experiment": "pricing-v2", "variant": variant})
```

## Metrics

```
oq_tasks_submitted_total{task_type, tags...}  # Optional: include common tags
oq_tag_index_writes_total
oq_tag_queries_total{tag_key}
```

## Version History

Tag changes appear in task history:

```bash
$ oq history abc123

Version 1: pending (created, tags: {customer: acme-corp})
Version 2: pending (tags updated: {customer: acme-corp, priority: critical})
Version 3: running (claimed)
Version 4: completed
```

## Implementation

### Files to Change

- `crates/oq/src/models/task.rs` - Add `tags: HashMap<String, String>` field
- `crates/oq/src/queue/ops.rs` - Add tag filtering to `list()`, add `update_tags()`, `remove_tags()`
- `crates/oq/src/queue/submit.rs` - Accept tags in `SubmitOptions`
- `crates/oq/src/python/queue.rs` - Python bindings
- `crates/oq/src/cli/commands.rs` - Add `--tag` flags to submit/list, add `tag`/`untag` commands

### Estimated Effort

| Component | Lines |
|-----------|-------|
| Task model | ~10 |
| Submit with tags | ~20 |
| Tag update/remove | ~40 |
| List filtering | ~30 |
| Python bindings | ~30 |
| CLI | ~40 |
| Tests | ~80 |
| **Total** | ~250 |

### Optional: Tag Index

| Component | Lines |
|-----------|-------|
| Index writes on submit | ~30 |
| Index cleanup on complete/fail | ~20 |
| Index-based list query | ~40 |
| **Total** | ~90 |

## Non-Goals

- **Structured tag values**: Tags are strings, not JSON
- **Tag schemas/validation**: User's responsibility
- **Tag-based routing**: Workers can filter, but no built-in routing
- **Tag inheritance**: Child tasks don't auto-inherit parent tags
