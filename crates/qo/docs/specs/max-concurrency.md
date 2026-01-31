# Max Concurrency

Limit concurrent execution of tasks across all workers.

## Problem

User needs to say "only run N of this task type at once" globally. Common use cases:
- External API rate limits
- Database connection limits
- Resource-intensive tasks (CPU/memory)

This requires coordination across workers - can't be done in user code without shared state.

## Design: Slot-based Leases

### Concept

Each concurrency group has N slots. To run a task, a worker must acquire a slot. Slots are leases with expiry (handles worker crashes). When task completes, slot is released.

### S3 Layout

```
concurrency/
  {group}/
    slots/
      0 -> { task_id, worker_id, expires_at }
      1 -> { task_id, worker_id, expires_at }
      ...
      N-1
```

Slots are created lazily on first acquisition attempt.

### API

**Python - Worker-side (recommended)**
```python
@worker.task("call_external_api", max_concurrent=10)
async def call_api(data, ctx):
    # Worker automatically acquires slot before running
    # Releases on completion/failure
    ...
```

**Python - Submit-side (alternative)**
```python
await queue.submit(
    "process_heavy",
    data,
    concurrency_group="heavy_processing",  # defaults to task_type
    concurrency_limit=5,                    # sets limit if not already set
)
```

**Rust Core**
```rust
pub struct ConcurrencyManager { ... }

impl ConcurrencyManager {
    /// Try to acquire a slot. Returns slot_id if acquired, None if at capacity.
    pub async fn try_acquire(
        &self,
        group: &str,
        limit: u32,
        task_id: Uuid,
        worker_id: &str,
        lease_duration: Duration,
    ) -> Result<Option<u32>, StorageError>;

    /// Release a slot.
    pub async fn release(
        &self,
        group: &str,
        slot_id: u32,
        task_id: Uuid,
    ) -> Result<(), StorageError>;

    /// Extend lease on a slot (for long-running tasks).
    pub async fn extend_lease(
        &self,
        group: &str,
        slot_id: u32,
        task_id: Uuid,
        additional: Duration,
    ) -> Result<(), StorageError>;

    /// Clean up expired leases. Called by monitor.
    pub async fn cleanup_expired(
        &self,
        group: &str,
    ) -> Result<u32, StorageError>;

    /// Get current slot status for observability.
    pub async fn get_status(
        &self,
        group: &str,
    ) -> Result<ConcurrencyStatus, StorageError>;
}

pub struct ConcurrencyStatus {
    pub group: String,
    pub limit: u32,
    pub active: u32,
    pub slots: Vec<SlotInfo>,
}

pub struct SlotInfo {
    pub slot_id: u32,
    pub task_id: Option<Uuid>,
    pub worker_id: Option<String>,
    pub expires_at: Option<DateTime<Utc>>,
}
```

### Acquisition Algorithm

```
try_acquire(group, limit, task_id, worker_id, lease_duration):
    1. List all slots in group (0 to limit-1)
    2. Shuffle slot order (reduces contention)
    3. For each slot:
        a. Try to read slot
        b. If not exists OR expired:
            - Conditional write: create/update with our lease
            - If succeeds: return slot_id
            - If fails (conflict): continue to next slot
        c. If exists and not expired: continue to next slot
    4. All slots occupied: return None
```

Key properties:
- **No single point of contention**: Workers try different slots
- **Crash-safe**: Leases expire, slots become available
- **Eventually consistent**: Brief over-subscription possible during S3 propagation, acceptable

### Worker Integration

In `worker/handler.rs`, before executing task:

```rust
// If task type has max_concurrent configured
if let Some(limit) = self.concurrency_limits.get(&task.task_type) {
    let slot = self.concurrency_manager
        .try_acquire(&task.task_type, *limit, task.id, &self.worker_id, self.lease_duration)
        .await?;

    match slot {
        Some(slot_id) => {
            // Store slot_id for release after execution
            self.active_slots.insert(task.id, (task.task_type.clone(), slot_id));
        }
        None => {
            // At capacity - put task back in queue
            // Don't fail, don't reschedule with delay - just release lease
            // Task stays pending, will be picked up when slot available
            self.queue.release_task(task.id).await?;
            return Ok(TaskSkipped);
        }
    }
}

// Execute task...

// After completion (success or failure), release slot
if let Some((group, slot_id)) = self.active_slots.remove(&task.id) {
    self.concurrency_manager.release(&group, slot_id, task.id).await?;
}
```

### Lease Extension

For long-running tasks, extend lease alongside task lease:

```rust
// In TaskContext
pub async fn extend_lease(&self, additional_secs: u64) -> Result<()> {
    // Extend task lease
    self.queue.extend_lease(self.task_id, additional_secs).await?;

    // Extend concurrency slot lease if applicable
    if let Some((group, slot_id)) = self.active_slot {
        self.concurrency_manager.extend_lease(
            &group,
            slot_id,
            self.task_id,
            Duration::from_secs(additional_secs)
        ).await?;
    }

    Ok(())
}
```

### Monitor Integration

Add to sweeper:
```rust
// Periodically clean up expired slots across all groups
for group in self.list_concurrency_groups().await? {
    let cleaned = self.concurrency_manager.cleanup_expired(&group).await?;
    if cleaned > 0 {
        info!("Cleaned {} expired slots in group {}", cleaned, group);
    }
}
```

### Configuration

**Worker config**
```python
worker = qo.Worker(queue, "worker-1", shards)

# Register with concurrency limit
worker.task("call_api", max_concurrent=10)(call_api_handler)

# Or via decorator
@worker.task("send_email", max_concurrent=50)
async def send_email(data, ctx):
    ...
```

**Global config (.qo.toml)**
```toml
[concurrency]
default_lease_duration_secs = 300  # 5 minutes

[concurrency.limits]
call_external_api = 10
send_email = 50
heavy_processing = 2
```

### CLI

```bash
# View concurrency status
qo concurrency status [GROUP]

# Manually release stuck slot (admin)
qo concurrency release GROUP SLOT_ID --force

# Set/update limit
qo concurrency set GROUP --limit N
```

### Observability

Dashboard shows:
- Concurrency groups with current/max usage
- Which tasks hold which slots
- Slot acquisition failures (capacity reached)

Metrics:
- `qo_concurrency_slots_active{group}` - gauge
- `qo_concurrency_slots_limit{group}` - gauge
- `qo_concurrency_acquisition_total{group,result=acquired|rejected}` - counter
- `qo_concurrency_wait_seconds{group}` - histogram (time until slot acquired, for queued tasks)

## Implementation Plan

### Phase 1: Rust Core
1. Add `ConcurrencyManager` struct in `src/concurrency/mod.rs`
2. Implement `try_acquire`, `release`, `extend_lease`, `cleanup_expired`
3. Add `ConcurrencyStatus` and `SlotInfo` types
4. Unit tests with mock storage

### Phase 2: Worker Integration
1. Add `concurrency_limits: HashMap<String, u32>` to worker config
2. Modify task execution loop to acquire/release slots
3. Integrate lease extension
4. Add to monitor sweep

### Phase 3: Python Bindings
1. Expose `max_concurrent` parameter on `@worker.task()`
2. Add `concurrency_group` and `concurrency_limit` to submit (optional)
3. Add `ConcurrencyStatus` type

### Phase 4: CLI & Observability
1. Add `qo concurrency` subcommand
2. Add metrics
3. Dashboard integration

## Edge Cases

**Worker crash mid-task**: Lease expires, slot becomes available, task times out and retries normally.

**S3 eventual consistency**: Brief over-subscription possible (two workers both see slot as free). Acceptable - we're limiting concurrency, not guaranteeing exactly N.

**High contention**: Many workers fighting for few slots. Shuffle algorithm + backoff on acquisition helps. For extreme cases, could add exponential backoff on retries.

**Task completes but release fails**: Lease expires eventually. Could add release retry with short timeout.

**Long-running task exceeds lease**: Task context should extend lease. If forgotten, slot becomes "available" but task is still running. Next task on that slot may cause over-subscription until original completes. Solution: document that lease extension is required for tasks longer than default lease duration.

## Non-Goals

- **Queuing with wait**: If at capacity, task goes back to queue, not a wait queue. Simpler, uses existing retry mechanism.
- **Priority within concurrency group**: All tasks equal. Use task priorities (separate feature) if needed.
- **Nested groups**: No hierarchy. Flat groups only.
- **Cross-bucket coordination**: Concurrency is per-bucket.
