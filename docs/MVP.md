# S3-Only Task Queue: MVP Specification

## Executive Summary

A minimal, production-ready distributed task queue built entirely on S3-compatible object storage. No databases, no message brokers, no complex infrastructure. Workers poll S3, claim tasks atomically via conditional writes, and execute work (at-least-once; exactly-once claim).

**Core Philosophy**: Embrace S3's limitations (polling latency) while leveraging its strengths (strong consistency, conditional writes, versioning for audit trails).

---

## What This MVP Does

### In Scope
- Submit tasks to a queue (write JSON to S3)
- Workers poll **ready indexes** for available tasks
- Atomic task claiming via S3 conditional writes (`If-Match`)
- Task execution with configurable timeout
- Automatic retries with exponential backoff
- Dead letter queue for permanently failed tasks
- Task status queries
- Task history via S3 versioning (automatic audit trail)
- Graceful worker shutdown
- Worker registration / heartbeat for observability (not required for correctness)
- Static web UI for observability
- LIST pagination and cursor rotation (prevents starvation)

### Deferred (Post-MVP)
- Idempotency keys (for effectively-once external side effects)
- HTTP API
- DAG dependencies / workflow orchestration
- Task priorities
- Scheduled/delayed tasks

---

## Architecture Overview

### Single-Object State Machine + Index Prefixes

Instead of moving tasks between folders (which requires non-atomic delete+create), each task lives at a **single path** for its entire lifecycle. State changes are atomic PUTs with `If-Match`.

**S3 Versioning** provides the audit trail automatically - every state transition creates a new version.
Small **index objects** keep LIST operations bounded.

```
┌─────────────────────────────────────────────────────────────┐
│     S3-Compatible Storage (Single Bucket, Versioning ON)    │
│                                                             │
│  tasks/                                                     │
│    ├─ {shard}/                    # Hex prefix 0-f          │
│    │   ├─ {task_id}.json          # Task metadata + state   │
│    │   └─ ...                                               │
│                                                             │
│  ready/                         # Claimable task index      │
│    ├─ {shard}/{bucket}/{task_id}                            │
│    └─ ...                                                   │
│                                                             │
│  leases/                        # Running task index        │
│    ├─ {shard}/{bucket}/{task_id}                            │
│    └─ ...                                                   │
│                                                             │
│  workers/                         # Worker registration     │
│    ├─ {worker_id}.json            # Heartbeat + status      │
│    └─ ...                                                   │
│                                                             │
│  data/                            # Optional: large I/O     │
│    ├─ {shard}/                                              │
│    │   ├─ {task_id}/input.json                              │
│    │   └─ {task_id}/output.json                             │
│                                                             │
│  ui/                              # Static web dashboard    │
│    ├─ index.html                                            │
│    └─ app.js                                                │
│                                                             │
└─────────────────────────────────────────────────────────────┘
          ▲                              ▲
          │ LIST ready/ + GET task       │
          │ PUT with If-Match (CAS)      │
          │                              │
    ┌─────┴─────┐                  ┌─────┴─────┐
    │  Worker 1 │                  │  Worker N │
    │ (writes   │                  │ (writes   │
    │  heartbeat)                  │  heartbeat)
    └───────────┘                  └───────────┘
```

### Why Single-Object?

| Old Approach (Multi-Folder) | New Approach (Single-Object) |
|----------------------------|------------------------------|
| Move: DELETE pending/ + PUT running/ | PUT with If-Match (atomic) |
| Race condition on delete+create | No race condition |
| Need orphan cleanup | No orphans possible |
| History requires custom logging | S3 Versioning = free history |
| LIST across 5 folders | LIST one prefix per shard |

---

## S3 Primitives Used

| Feature | AWS Launch | Purpose |
|---------|------------|---------|
| Strong Consistency | Dec 2020 | Read-after-write guaranteed |
| `If-None-Match: *` | Aug 2024 | Create only if key doesn't exist |
| `If-Match: "etag"` | Nov 2024 | Update only if ETag matches (CAS) |
| S3 Versioning | Original | Automatic audit trail |
| ListObjectVersions | Original | Task history retrieval |
| ListObjectsV2 pagination | Original | Prevent LIST starvation |
| Conditional Delete (optional) | Sep 2025 (AWS) | Best-effort index cleanup |

See [S3-REFERENCES.md](./S3-REFERENCES.md) for detailed sources.

---

## Data Model

### Task (Single Object)

```rust
struct Task {
    // Identity
    id: Uuid,
    task_type: String,           // e.g., "send_email", "process_image"
    shard: String,               // First hex char of UUID (0-f)

    // State
    status: TaskStatus,

    // Scheduling
    available_at: DateTime<Utc>,           // When task can be claimed (for retry backoff)
    lease_expires_at: Option<DateTime<Utc>>, // When running task times out

    // Execution
    input: serde_json::Value,
    output: Option<serde_json::Value>,

    // Retry
    timeout_seconds: u64,        // default: 300 (used to calculate lease_expires_at)
    max_retries: u32,            // default: 3
    retry_count: u32,            // current attempt
    retry_policy: RetryPolicy,

    // Timestamps
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    completed_at: Option<DateTime<Utc>>,

    // Worker assignment
    worker_id: Option<String>,
    lease_id: Option<Uuid>,        // Claim token for this attempt
    attempt: u32,                  // Increments on each claim
    last_error: Option<String>,
}

enum TaskStatus {
    Pending,    // Waiting to be claimed
    Running,    // Currently being executed
    Completed,  // Successfully finished
    Failed,     // Max retries exceeded (DLQ state)
    Archived,   // Soft-deleted, will be cleaned up
}
```

### Key Scheduling Fields

**`available_at`**: Tasks cannot be claimed until `now >= available_at`. This enables:
- Retry backoff without worker sleep (worker just skips tasks not yet available)
- Delayed/scheduled tasks (future enhancement)
- Backoff is visible and debuggable in the task itself

**`lease_expires_at`**: Set when task is claimed to `now + timeout_seconds`. Benefits:
- Simple timeout check: `lease_expires_at < now` means timed out
- Can extend lease by updating this field (heartbeat pattern)
- No calculation needed at read time

**Clock skew**: Scheduling and leases rely on worker clocks. Workers should run
with NTP and enforce a maximum skew (e.g., `MAX_CLOCK_SKEW_MS`) to avoid early
claims or premature timeouts.

**`lease_id`**: A random UUID written on claim. Every complete/retry/fail must
present the current `lease_id` (and pass `If-Match`) to prevent stale workers
from mutating a newer attempt.

**`attempt`**: Monotonic counter incremented on each claim. Helps debugging and
guards against ambiguity when replaying history.

### Worker Registration

Workers register themselves via S3 objects for visibility and coordination.

```
workers/{worker_id}.json
```

```rust
struct WorkerInfo {
    worker_id: String,
    started_at: DateTime<Utc>,
    last_heartbeat: DateTime<Utc>,
    shards: Vec<String>,              // Which shards this worker polls
    current_task: Option<Uuid>,       // Task currently being executed
    tasks_completed: u64,             // Lifetime counter
    tasks_failed: u64,                // Lifetime counter
}
```

**Benefits:**
- UI can show active workers and their status
- Timeout monitor can detect dead workers (stale heartbeat)
- Worker-to-shard assignment is visible
- No external service discovery needed
- Debugging: see which worker has which task

### Index Prefixes (Ready + Leases)

To avoid scanning every task object, the queue maintains **tiny index objects**:

```
ready/{shard}/{bucket}/{task_id}   # claimable tasks (projection)
leases/{shard}/{bucket}/{task_id}  # running tasks by expiry (projection)
```

- `bucket` is a **fixed-width epoch-minute** derived from `available_at` or
  `lease_expires_at`, making keys lexicographically sortable.
- **Task objects are the source of truth**; indexes are projections used for
  efficient listing.
- Workers can **page the ready index** and optionally fall back to log scans if
  the index is missing or stale (hybrid mode).
- The timeout monitor can run a **sweeper** that repairs missing indexes and
  deletes stale ones.

### Sharding Strategy

Tasks are sharded by the first hex character of their UUID:
- Shard `0`: `tasks/0/{uuid}.json`
- Shard `a`: `tasks/a/{uuid}.json`
- etc.

Benefits:
- 16 shards distribute LIST operations
- Workers can poll specific shards for load balancing
- UI can browse one shard at a time

### Retry Policy

```rust
struct RetryPolicy {
    initial_interval_ms: u64,  // e.g., 1000 = 1s
    max_interval_ms: u64,      // e.g., 60000 = 60s
    multiplier: f64,           // e.g., 2.0 = exponential
    jitter_percent: f64,       // e.g., 0.25 = ±25% randomness
}
```

---

## Key Operations

### 1. Task Submission

```
Client:
1. Generate task_id (UUID v4)
2. Compute shard = task_id[0] (first hex char)
3. PUT tasks/{shard}/{task_id}.json
   WITH If-None-Match: *
   Body: Task JSON with:
     - status = Pending
     - available_at = now (immediately available)
     - created_at = now
     - updated_at = now
4. PUT ready/{shard}/{bucket}/{task_id} (index object; bucket from available_at)
5. If 200/201: return task_id
   If 412: task already exists (duplicate submission)
```

### 2. Task Polling (Worker Loop)

```
Worker (continuous loop):
1. Update heartbeat: PUT workers/{worker_id}.json
2. For each shard (or assigned shards):
   a. LIST ready/{shard}/{bucket<=now} with pagination
   b. For each task_id in ready:
      - GET task
      - If status == Pending AND available_at <= now:
        - Attempt to claim
        - On success: delete ready index (best-effort) and return task
      - If status != Pending: delete ready index (stale)
3. If no claimable tasks found:
   - Sleep with exponential backoff (100ms → 5s max)
4. Rotate bucket/page cursor to avoid starvation
```

**Note**: The `available_at` check enables retry backoff without worker sleep. Tasks
with `available_at` in the future are simply skipped until they become eligible.
LIST calls must be fully paginated and rotated to avoid starvation.

### 3. Task Claiming (Atomic via If-Match)

**This is the critical section.** Uses `If-Match` for true compare-and-swap.

```
Worker:
1. GET tasks/{shard}/{task_id}.json
   - Capture ETag from response
   - Verify status == Pending AND available_at <= now
2. PUT tasks/{shard}/{task_id}.json
   WITH If-Match: {captured_etag}
   Body: Task JSON with:
     - status = Running
     - worker_id = me
     - lease_id = random UUID
     - attempt += 1
     - lease_expires_at = now + timeout_seconds
     - updated_at = now
3. PUT leases/{shard}/{bucket}/{task_id} (best-effort index; bucket from lease_expires_at)
4. Delete ready/{shard}/{bucket}/{task_id} (best-effort index)
5. Update heartbeat: PUT workers/{worker_id}.json with current_task = task_id
6. If PUT succeeds (200):
   - Return task (claimed successfully)
   - S3 Versioning automatically preserves previous state
7. If PUT fails (412 Precondition Failed):
   - Another worker modified the task
   - Return None (move to next task)
```

**Why this works:**
- `If-Match: {etag}` guarantees the task hasn't changed since we read it
- Only one worker's PUT succeeds; others get 412
- No delete+create race condition
- S3 strong consistency ensures the 412 response is accurate
- Versioning captures the Pending→Running transition automatically
- `lease_expires_at` is pre-computed, no calculation needed for timeout checks

### 4. Task Execution

```
Worker:
1. Execute task logic with timeout monitoring
   - Spawn task in separate tokio task
   - Use tokio::time::timeout(duration, task_future)
2. On success:
   - GET current task (capture ETag)
   - Verify lease_id matches this attempt
   - PUT tasks/{shard}/{task_id}.json
     WITH If-Match: {etag}
     Body:
       - status = Completed
       - output = result
       - completed_at = now
       - updated_at = now
       - lease_id = null
   - Update heartbeat: current_task = null, tasks_completed++
   - Delete leases/{shard}/{bucket}/{task_id} (best-effort)
3. On failure:
   - GET current task (capture ETag)
   - Verify lease_id matches this attempt
   - If retry_count < max_retries:
     - Calculate backoff delay using retry_policy
     - PUT with:
       - status = Pending
       - retry_count++
       - available_at = now + backoff_delay  # KEY: backoff via available_at
       - last_error = error
       - worker_id = null
       - lease_expires_at = null
       - lease_id = null
       - updated_at = now
     - PUT ready/{shard}/{bucket}/{task_id} (new ready index)
     - Delete leases/{shard}/{bucket}/{task_id} (best-effort)
   - Else:
     - PUT with status=Failed, last_error=error (DLQ state)
     - Delete leases/{shard}/{bucket}/{task_id} (best-effort)
   - Update heartbeat: current_task = null, tasks_failed++
4. On timeout:
   - Treat as retriable failure
```

**Key insight**: Retry backoff is encoded in `available_at`. The worker doesn't sleep;
it just PUTs the task back immediately with a future `available_at`. Other workers
will skip this task until it becomes eligible again.

### 5. Timeout Monitoring (Lease Index)

Run as a dedicated monitor (or leader-elected task):

```
Timeout Monitor (every 30s):
1. For each shard:
   a. LIST leases/{shard}/{bucket<=now} with pagination
   b. For each task_id in expired leases:
      - GET task (capture ETag)
      - If status == Running AND lease_expires_at < now:
        - If retry_count < max_retries:
          - Calculate backoff delay
          - PUT with:
            - status = Pending
            - retry_count++
            - available_at = now + backoff_delay
            - worker_id = null
            - lease_expires_at = null
            - lease_id = null
          - PUT ready/{shard}/{bucket}/{task_id}
        - Else:
          - PUT with status=Failed (DLQ)
        - Delete leases/{shard}/{bucket}/{task_id} (best-effort)
        - Log timeout event
2. (Optional) Check worker health for UI:
   a. LIST workers/
   b. For each worker with last_heartbeat older than threshold:
      - Log dead worker warning
```

### 6. Task History (Via S3 Versioning)

```
To view task history:
1. ListObjectVersions for tasks/{shard}/{task_id}.json
2. For each version:
   - GET tasks/{shard}/{task_id}.json?versionId={version_id}
   - Parse and display state at that point in time
```

This gives you a complete audit trail:
- When task was created (first version)
- When it was claimed (status changed to Running)
- Any retry attempts (status oscillated Pending↔Running)
- Final outcome (Completed or Failed)

### 7. Worker Lifecycle

```
Worker Startup (optional for observability):
1. Generate worker_id (or use provided)
2. PUT workers/{worker_id}.json
   Body: WorkerInfo with started_at=now, last_heartbeat=now, current_task=null

Worker Heartbeat (every 10-30s, or on state change; not required for correctness):
1. PUT workers/{worker_id}.json
   Body: Updated WorkerInfo with last_heartbeat=now, current_task, counters

Worker Shutdown (on SIGTERM/SIGINT):
1. Set shutdown flag (AtomicBool)
2. Stop polling for new tasks
3. Wait for current task to complete (with deadline, e.g., 30s)
4. If task still running after deadline:
   - GET current task, PUT with status=Pending, available_at=now (requeue immediately)
   - PUT ready/{shard}/{bucket}/{task_id}
   - Delete leases/{shard}/{bucket}/{task_id} (best-effort)
5. DELETE workers/{worker_id}.json (deregister)
6. Exit cleanly
```

### 8. Task Archival

Completed and failed tasks can be archived instead of deleted:

```
Archive Task:
1. GET task (capture ETag)
2. PUT with status=Archived, updated_at=now
3. Task remains at same path but is excluded from normal listing
```

**Benefits:**
- No delete operations needed
- Task history preserved via versioning
- S3 lifecycle rules can auto-delete Archived tasks after N days
- Can "unarchive" by setting status back to Failed (for replay)

---

## Data Separation (Optional Optimization)

---

## Web UI: Zero-Server Dashboard

A static single-page app hosted in the same S3 bucket.

### Architecture

```
┌─────────────────────────────────────────┐
│  Browser                                │
│  ┌───────────────────────────────────┐  │
│  │  Static HTML/JS from ui/          │  │
│  │                                   │  │
│  │  - Shard selector (0-f)           │  │
│  │  - Task list (LIST ready/leases)  │  │
│  │  - Task detail (GET + versions)   │  │
│  │  - I/O viewer (JSON tree)         │  │
│  │  - Timeline (version history)     │  │
│  └───────────────────────────────────┘  │
│              │                          │
│              │ Direct S3 API calls      │
│              ▼ (CORS configured)        │
└──────────────┼──────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────┐
│  S3 Bucket                              │
│  - tasks/{shard}/*.json                 │
│  - ready/{shard}/{bucket}/*             │
│  - leases/{shard}/{bucket}/*            │
│  - ui/index.html, ui/app.js             │
└─────────────────────────────────────────┘
```

### Features

1. **Shard Browser**: Dropdown to select hex shard (0-f)
2. **Task List**: Shows recent tasks in selected shard
   - Filter by status (Pending/Running/Completed/Failed/Archived/All)
   - Sort by updated_at
   - Show `available_at` for pending tasks (visible backoff)
   - Uses ready/lease indexes for active tasks; completed/failed require scans
3. **Task Detail**: Click to expand
   - Current state with `lease_expires_at` countdown for running tasks
   - Input/Output JSON viewer (collapsible tree)
   - Version timeline (from ListObjectVersions)
   - Diff view between versions
   - Actions: Replay, Archive
4. **Worker Dashboard**:
   - List all registered workers from `workers/`
   - Show status: active (recent heartbeat) vs stale
   - Current task being processed
   - Lifetime counters (completed/failed)
5. **Auto-refresh**: Poll every 5-10 seconds
6. **Auth**: Prefer pre-signed URLs or short-lived credentials; do not embed
   long-lived keys in the browser.

### Cost

- LIST + a few GETs per refresh = negligible
- No server to maintain
- Hosted for free in the same bucket

---

## Technology Stack

### Language
- **Rust**: Memory safety, async support, strong types

### Dependencies

```toml
[dependencies]
# S3
aws-sdk-s3 = "1"
aws-config = "1"

# Async
tokio = { version = "1", features = ["full", "signal"] }

# Serialization
serde = { version = "1", features = ["derive"] }
serde_json = "1"

# CLI
clap = { version = "4", features = ["derive"] }

# Utilities
uuid = { version = "1", features = ["v4", "serde"] }
chrono = { version = "0.4", features = ["serde"] }
thiserror = "2"
anyhow = "1"
rand = "0.8"

# Logging
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
```

---

## Configuration

### Environment Variables

```bash
# S3 Connection
S3_ENDPOINT=http://localhost:3902  # Optional, for local dev
S3_BUCKET=task-queue
S3_REGION=us-east-1

# AWS Credentials (standard AWS SDK env vars)
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...

# Worker (can also be passed via CLI flags)
QO_INDEX_MODE=hybrid             # index-only | hybrid | log-scan
QO_READY_PAGE_SIZE=100           # Page size for ready index listing
QO_LOG_SCAN_PAGE_SIZE=50         # Page size for log scan listing
QO_NO_MONITOR=false              # Disable embedded monitor alongside worker

# Monitor
QO_MONITOR_CHECK_INTERVAL=30     # Seconds between timeout checks
QO_MONITOR_WORKER_HEALTH_THRESHOLD=60
QO_MONITOR_SWEEP_INTERVAL=300    # Seconds between index sweeps (0 disables)
QO_MONITOR_SWEEP_PAGE_SIZE=1000

# Logging
RUST_LOG=info                   # trace/debug/info/warn/error

# Dev override (disables history guarantees)
QO_ALLOW_NO_VERSIONING=1        # Skip bucket versioning check
```

### Bucket Setup

```bash
# Enable versioning (required for audit trail)
aws s3api put-bucket-versioning \
  --bucket task-queue \
  --versioning-configuration Status=Enabled

# CORS for web UI (if hosting UI in bucket)
aws s3api put-bucket-cors \
  --bucket task-queue \
  --cors-configuration '{
    "CORSRules": [{
      "AllowedOrigins": ["*"],
      "AllowedMethods": ["GET", "PUT", "HEAD"],
      "AllowedHeaders": ["*"]
    }]
  }'
```

---

## CLI Interface

```bash
# Submit a task
qo submit --type send_email --input '{"to": "user@example.com"}'

# Get task status (includes current state)
qo status <task_id>

# Get task history (all versions)
qo history <task_id>

# List tasks
qo list [--shard 0-f] [--status pending|running|completed|failed|archived]

# Replay failed task (reset to pending with available_at=now)
qo replay <task_id>

# Archive completed/failed tasks
qo archive <task_id>

# List workers
qo workers

# Run worker (embedded monitor by default)
qo worker [--id worker-1] [--shards 0,1,2,3] [--index-mode hybrid] [--no-monitor]

# Run monitor (timeouts + index sweep)
qo monitor [--check-interval 30] [--worker-health-threshold 60] [--sweep-interval 300]
```

---

## Local Development

### Using Garage (Recommended)

```bash
# Start Garage
docker run -d \
  --name garage \
  -p 3900:3900 -p 3901:3901 -p 3902:3902 \
  -v $(pwd)/data:/data \
  dxflrs/garage:v1.0.1

# Configure (one-time)
docker exec garage /garage layout assign -z dc1 -c 1G $(docker exec garage /garage node id -q | cut -d@ -f1)
docker exec garage /garage layout apply --version 1
docker exec garage /garage key new --name task-queue-key
docker exec garage /garage bucket create task-queue
docker exec garage /garage bucket allow task-queue --read --write --key task-queue-key

# Enable versioning
aws --endpoint-url http://localhost:3902 s3api put-bucket-versioning \
  --bucket task-queue \
  --versioning-configuration Status=Enabled
```

---

## Error Handling

### Retryable Errors
- Network timeouts
- S3 5xx responses (503 SlowDown, 500 InternalError)
- Connection refused/reset
- Task execution timeout
- 412 Precondition Failed on **claim** (another worker won the race)

### Non-Retryable Errors
- Invalid input (deserialization failure)
- Task type not registered
- Explicit permanent failure from task handler
- Max retries exceeded → status=Failed (DLQ state)
- 412 Precondition Failed on **complete/retry/fail** (lost lease; treat as no-op)

```rust
enum TaskError {
    Retryable(String),
    Permanent(String),
    Timeout,
}
```

---

## Testing Strategy

### Unit Tests
- Retry policy calculation (backoff + jitter)
- Task state transitions
- Shard calculation
- Error classification

### Integration Tests (with Garage)
- Submit → claim → complete cycle
- Claiming contention (multiple workers, same task)
- Timeout detection and requeue
- Graceful shutdown requeues task
- Failed task reaches Failed status after max retries
- Version history is correctly recorded

---

## Success Criteria

### Correctness
- No lost tasks (all eventually complete or reach Failed status)
- Exactly-once claim / state transitions via CAS
- At-least-once execution (handlers must be idempotent or deduplicated)
- Tasks complete within timeout or get requeued
- Full audit trail via versioning

### Performance (Local)
- Claim latency: <50ms P99
- Worker can process 10+ simple tasks/second
- Polling overhead: minimal when queue empty

### Observability
- Web UI can list and inspect any task
- Version history shows complete lifecycle
- No server required for monitoring

---

## Future Enhancements (Post-MVP)

1. **Idempotency keys** - Prevent duplicate task execution
2. **Heartbeats** - Faster detection of stuck workers
3. **HTTP API** - REST interface for task submission
4. **Metrics** - Prometheus endpoint
5. **Task priorities** - High-priority tasks processed first
6. **Scheduled tasks** - Execute at specific time
7. **DAG dependencies** - Task A must complete before Task B
8. **S3 Select** - Filter tasks server-side for large shards
