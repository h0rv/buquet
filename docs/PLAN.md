# Implementation Plan

## Overview

This plan breaks down the MVP into implementable phases. Each phase has parallel tracks where components can be built independently, then integrated.

### Architecture Summary

**Single-Object State Machine + Index Prefixes**: Tasks live at one path (`tasks/{shard}/{id}.json`) for their entire lifecycle. State changes are atomic PUTs with `If-Match`. Tiny **ready/lease index objects** keep LIST operations bounded. S3 Versioning provides automatic audit trails.

```
Phase 1: Foundation
    ├── Track A: Data Models
    └── Track B: S3 Client
            │
            ▼
Phase 2: Core Operations
    ├── Track A: Submit + Get
    └── Track B: Claim + Execute
            │
            ▼
Phase 3: Reliability
    ├── Track A: State Transitions (Complete/Retry/Fail)
    └── Track B: Timeout Monitor
            │
            ▼
Phase 4: Operations
    ├── Track A: Graceful Shutdown
    ├── Track B: CLI
    └── Track C: Web UI
            │
            ▼
Phase 5: Integration & Testing
```

---

## Phase 1: Foundation

**Goal**: Establish core types and S3 connectivity.

### Track A: Data Models
No dependencies.

```
crates/qo/src/
├── lib.rs
└── models/
    ├── mod.rs
    ├── task.rs        # Task, TaskStatus
    └── retry.rs       # RetryPolicy, backoff calculation
```

**Deliverables:**
- [ ] `Task` struct with all fields:
  - `id: Uuid`
  - `task_type: String`
  - `shard: String` (first hex char of UUID)
  - `status: TaskStatus`
  - `available_at: DateTime<Utc>` (when task can be claimed - enables backoff without sleep)
  - `lease_expires_at: Option<DateTime<Utc>>` (pre-computed timeout deadline)
  - `input: serde_json::Value`
  - `output: Option<serde_json::Value>`
  - `timeout_seconds: u64` (used to compute lease_expires_at)
  - `max_retries: u32`
  - `retry_count: u32`
  - `retry_policy: RetryPolicy`
  - `created_at, updated_at, completed_at: DateTime<Utc>`
  - `worker_id: Option<String>`
  - `lease_id: Option<Uuid>` (claim token)
  - `attempt: u32` (increments on each claim)
  - `last_error: Option<String>`
- [ ] `TaskStatus` enum: `Pending`, `Running`, `Completed`, `Failed`, `Archived`
- [ ] `RetryPolicy` struct with `calculate_backoff(attempt: u32) -> Duration`
- [ ] `TaskError` enum: `Retryable(String)`, `Permanent(String)`, `Timeout`
- [ ] `Task::new(task_type, input)` - creates with generated UUID, shard, available_at=now
- [ ] `Task::key()` -> `String` - returns `tasks/{shard}/{id}.json`
- [ ] `Task::is_available(&self) -> bool` - returns `now >= available_at`
- [ ] `Task::is_lease_expired(&self) -> bool` - returns `lease_expires_at < now`
- [ ] `WorkerInfo` struct:
  - `worker_id: String`
  - `started_at: DateTime<Utc>`
  - `last_heartbeat: DateTime<Utc>`
  - `shards: Vec<String>`
  - `current_task: Option<Uuid>`
  - `tasks_completed: u64`
  - `tasks_failed: u64`
- [ ] `WorkerInfo::key()` -> `String` - returns `workers/{worker_id}.json`
- [ ] `WorkerInfo::is_healthy(&self, threshold: Duration) -> bool`
- [ ] Serde serialization/deserialization with `#[serde(rename_all = "snake_case")]`
- [ ] Unit tests for:
  - Backoff calculation with jitter bounds
  - Shard derivation from UUID
  - Index bucket calculation (ready/lease)
  - JSON round-trip serialization
  - `is_available()` and `is_lease_expired()` logic

**Acceptance Criteria:**
- `Task` and `WorkerInfo` serialize to/from JSON correctly
- `Task::key()` and `WorkerInfo::key()` return correct paths
- Backoff produces delays within expected bounds
- `available_at` and `lease_expires_at` logic is correct

---

### Track B: S3 Client Wrapper
No dependencies.

```
crates/qo/src/
└── storage/
    ├── mod.rs
    ├── client.rs      # S3Client wrapper
    └── error.rs       # StorageError
```

**Deliverables:**
- [ ] `StorageError` enum (using `thiserror`):
  - `NotFound { key: String }`
  - `PreconditionFailed { key: String }` (412 responses)
  - `AlreadyExists { key: String }`
  - `ConnectionError(String)`
  - `SerializationError(String)`
  - `S3Error { code: String, message: String }`
- [ ] `S3Config` struct:
  - `endpoint: Option<String>` (for local dev)
  - `bucket: String`
  - `region: String`
  - `S3Config::from_env()` - reads from environment
- [ ] `S3Client` struct wrapping `aws_sdk_s3::Client`
- [ ] Methods:
  - `new(config: S3Config) -> Result<Self>`
  - `put_object(key, body, condition) -> Result<String>` (returns ETag)
    - `condition: PutCondition` enum: `None`, `IfNoneMatch`, `IfMatch(String)`
  - `get_object(key) -> Result<(Vec<u8>, String)>` (body + ETag)
  - `get_object_version(key, version_id) -> Result<Vec<u8>>`
  - `delete_object(key) -> Result<()>`
  - `list_objects(prefix, limit, continuation_token) -> Result<(Vec<String>, Option<String>)>`
  - `list_object_versions(key) -> Result<Vec<ObjectVersion>>`
  - `head_object(key) -> Result<Option<String>>` (returns ETag if exists)
- [ ] `ObjectVersion` struct: `version_id`, `last_modified`, `is_latest`
- [ ] Error mapping: S3 errors → `StorageError`

**Acceptance Criteria:**
- Can connect to local Garage/MinIO
- `put_object` with `IfNoneMatch` returns `AlreadyExists` on duplicate
- `put_object` with `IfMatch(wrong_etag)` returns `PreconditionFailed`
- `list_object_versions` returns version history
- `list_objects` supports pagination via continuation token

---

### Phase 1 Integration
Once both tracks complete:
- [ ] `Task` can be serialized, PUT, GET, deserialized round-trip
- [ ] Conditional writes work correctly (test CAS behavior)
- [ ] Index objects can be PUT/LIST/DELETE (ready/leases)

---

## Phase 2: Core Operations

**Goal**: Submit tasks and claim them atomically.

**Depends on**: Phase 1 complete.

### Track A: Task Submission + Retrieval
Depends on: Phase 1.

```
crates/qo/src/
└── queue/
    ├── mod.rs
    └── ops.rs         # Submit, get, list operations
```

**Deliverables:**
- [ ] `Queue` struct holding `S3Client`
- [ ] `Queue::submit(task_type, input, options) -> Result<Task>`
  - Generate UUID, compute shard
  - Create `Task` with `status=Pending`
  - PUT to `tasks/{shard}/{task_id}.json` with `IfNoneMatch`
  - PUT `ready/{shard}/{bucket}/{task_id}` (index object)
  - Return created task
- [ ] `SubmitOptions` struct:
  - `timeout_seconds: Option<u64>`
  - `max_retries: Option<u32>`
  - `retry_policy: Option<RetryPolicy>`
- [ ] `Queue::get(task_id) -> Result<Option<(Task, String)>>`
  - Compute shard from task_id
  - GET `tasks/{shard}/{task_id}.json`
  - Return task and ETag
- [ ] `Queue::get_history(task_id) -> Result<Vec<Task>>`
  - `list_object_versions` for the task
  - GET each version, deserialize
  - Return ordered list (oldest first)
- [ ] `Queue::list(shard, status_filter, limit) -> Result<Vec<Task>>`
  - LIST `tasks/{shard}/` (paginated; for admin/debug only)
  - GET each task, filter by status
  - Return matching tasks
- [ ] `Queue::list_all_shards(status_filter, limit_per_shard) -> Result<Vec<Task>>`
  - Iterate all 16 shards
  - Aggregate results

**Acceptance Criteria:**
- Submit creates task at correct path
- Duplicate submit (same UUID) returns error
- Get retrieves task with ETag
- Get history returns all versions in order
- List filters by status correctly

---

### Track B: Task Claiming + Execution
Depends on: Phase 1.

```
crates/qo/src/
└── worker/
    ├── mod.rs
    ├── claim.rs       # Atomic claiming
    ├── execute.rs     # Task execution with timeout
    └── handler.rs     # TaskHandler trait
```

**Deliverables:**
- [ ] `TaskHandler` trait:
  ```rust
  #[async_trait]
  pub trait TaskHandler: Send + Sync {
      fn task_type(&self) -> &str;
      async fn handle(&self, input: Value) -> Result<Value, TaskError>;
  }
  ```
- [ ] `HandlerRegistry`:
  - `new() -> Self`
  - `register(handler: Box<dyn TaskHandler>)`
  - `get(task_type: &str) -> Option<&dyn TaskHandler>`
- [ ] `Worker` struct:
  - `queue: Queue`
  - `worker_id: String`
  - `handlers: HandlerRegistry`
  - `shards: Vec<String>` (which shards to poll)
- [ ] `Worker::claim(task: &Task, etag: &str) -> Result<Option<Task>>`
  - Verify `task.status == Pending` and `task.is_available()`
  - PUT with `IfMatch(etag)`:
    - `status = Running`
    - `worker_id = self.worker_id`
    - `lease_id = random UUID`
    - `attempt += 1`
    - `lease_expires_at = now + task.timeout_seconds`
    - `updated_at = now`
  - PUT `leases/{shard}/{bucket}/{task_id}` (index)
  - DELETE `ready/{shard}/{bucket}/{task_id}` (best-effort)
  - Update worker heartbeat with `current_task = task.id`
  - On success: return updated task
  - On 412: return None (another worker got it)
- [ ] `Worker::execute(task: &Task) -> Result<Value, TaskError>`
  - Get handler for `task.task_type`
  - Wrap in `tokio::time::timeout(task.timeout_seconds)`
  - Return output or error

**Acceptance Criteria:**
- Only one worker can claim a given task (concurrent test)
- Claimed task has correct `status`, `worker_id`, `lease_id`, `attempt`
- Execution respects timeout
- Unknown task type returns `TaskError::Permanent`

---

### Phase 2 Integration
Once both tracks complete:
- [ ] Submit → Get shows Pending → Claim succeeds → Get shows Running
- [ ] Two workers racing to claim: only one succeeds
- [ ] Execute with mock handler returns output

---

## Phase 3: Reliability

**Goal**: Handle task completion, failure, and timeouts.

**Depends on**: Phase 2 complete.

### Track A: State Transitions
Depends on: Phase 2.

```
crates/qo/src/
└── worker/
    └── transitions.rs  # Complete, retry, fail
```

**Deliverables:**
- [ ] `Worker::complete(task: &Task, output: Value) -> Result<Task>`
  - GET current task (capture ETag)
  - Verify `lease_id` matches this attempt
  - PUT with `IfMatch`:
    - `status = Completed`
    - `output = Some(output)`
    - `completed_at = now`
    - `lease_id = None`
    - `updated_at = now`
  - Update heartbeat: `current_task = None`, `tasks_completed++`
  - DELETE `leases/{shard}/{bucket}/{task_id}` (best-effort)
  - Return updated task
- [ ] `Worker::retry(task: &Task, error: &str) -> Result<Task>`
  - GET current task (capture ETag)
  - Verify `lease_id` matches this attempt
  - Calculate backoff delay from retry_policy
  - PUT with `IfMatch`:
    - `status = Pending`
    - `retry_count += 1`
    - `available_at = now + backoff_delay`  # KEY: backoff via available_at
    - `last_error = error`
    - `worker_id = None`
    - `lease_expires_at = None`
    - `lease_id = None`
    - `updated_at = now`
  - PUT `ready/{shard}/{bucket}/{task_id}` (index)
  - DELETE `leases/{shard}/{bucket}/{task_id}` (best-effort)
  - Update heartbeat: `current_task = None`, `tasks_failed++`
  - Return updated task (immediately, no sleep!)
- [ ] `Worker::fail(task: &Task, error: &str) -> Result<Task>`
  - GET current task (capture ETag)
  - Verify `lease_id` matches this attempt
  - PUT with `IfMatch`:
    - `status = Failed`
    - `last_error = error`
    - `completed_at = now`
    - `lease_id = None`
    - `updated_at = now`
  - Update heartbeat: `current_task = None`, `tasks_failed++`
  - DELETE `leases/{shard}/{bucket}/{task_id}` (best-effort)
  - Return updated task (now in DLQ state)
- [ ] `Worker::archive(task: &Task) -> Result<Task>`
  - GET current task (capture ETag)
  - Verify status is `Completed` or `Failed`
  - PUT with `IfMatch`:
    - `status = Archived`
    - `updated_at = now`
  - Return updated task
- [ ] `Worker::handle_execution_result(task, result) -> Result<Task>`
  - On `Ok(output)`: call `complete`
  - On `Err(TaskError::Retryable)` with retries left: call `retry`
  - On `Err(TaskError::Retryable)` max retries: call `fail`
  - On `Err(TaskError::Permanent)`: call `fail`
  - On `Err(TaskError::Timeout)`: treat as retryable

**Acceptance Criteria:**
- Successful task reaches `Completed` with output
- Failed task with retries left goes back to `Pending` with future `available_at`
- Worker doesn't sleep for backoff (just sets `available_at`)
- Failed task at max retries reaches `Failed`
- State transitions are atomic (CAS)
- Worker heartbeat is updated on completion/failure

---

### Track B: Timeout Monitor
Depends on: Phase 2.

```
crates/qo/src/
└── worker/
    └── monitor.rs     # Background timeout checker
```

**Deliverables:**
- [ ] `TimeoutMonitor` struct:
  - `queue: Queue`
  - `check_interval: Duration`
  - `worker_health_threshold: Duration`
- [ ] `TimeoutMonitor::check_shard(shard: &str) -> Result<Vec<Uuid>>`
  - LIST `leases/{shard}/{bucket<=now}` (paginated)
  - For each task_id:
    - GET task
    - If `status == Running` and `task.is_lease_expired()`:  # Simple check!
      - Calculate backoff delay
      - If `retry_count < max_retries`:
        - PUT with `status = Pending`, `retry_count++`, `available_at = now + backoff`, `lease_id = None`
        - PUT `ready/{shard}/{bucket}/{task_id}`
      - Else:
        - PUT with `status = Failed`, `lease_id = None`
      - DELETE `leases/{shard}/{bucket}/{task_id}` (best-effort)
      - Add to result list
  - Return list of recovered task IDs
- [ ] `TimeoutMonitor::check_all_shards() -> Result<Vec<Uuid>>`
  - Iterate all 16 shards
  - Aggregate recovered tasks
- [ ] `TimeoutMonitor::check_worker_health() -> Result<Vec<String>>`
  - LIST `workers/`
  - For each worker:
    - GET worker info
    - If `!worker.is_healthy(threshold)`:
      - Log dead worker warning
      - Add to result list
  - Return list of unhealthy worker IDs
- [ ] `TimeoutMonitor::run(shutdown: ShutdownSignal) -> Result<()>`
  - Loop:
    - `check_all_shards()`
    - `check_worker_health()`
    - Sleep for `check_interval`
    - Check shutdown signal

**Acceptance Criteria:**
- Timed-out tasks are detected via `lease_expires_at < now`
- Timed-out tasks with retries go back to `Pending` with future `available_at`
- Timed-out tasks at max retries go to `Failed`
- Unhealthy workers (stale heartbeat) are detected and logged
- Monitor respects shutdown signal

---

### Phase 3 Integration
Once both tracks complete:
- [ ] Submit → Claim → Fail → Retry → Fail → Retry → Fail → status=Failed
- [ ] Submit → Claim → Hang → Timeout monitor recovers
- [ ] Version history shows all state transitions

---

## Phase 4: Operations

**Goal**: Production-ready worker, CLI, and observability UI.

**Depends on**: Phase 3 complete.

### Track A: Graceful Shutdown
Depends on: Phase 3.

```
crates/qo/src/
└── worker/
    └── runner.rs      # Main loop with shutdown handling
```

**Deliverables:**
- [ ] `ShutdownSignal`:
  - Wraps `tokio::sync::watch::Receiver<bool>`
  - `is_shutdown(&self) -> bool`
  - `wait(&self) -> impl Future`
- [ ] `shutdown_signal() -> (ShutdownSignal, impl FnOnce())`
  - Returns signal and trigger function
  - Hooks SIGTERM/SIGINT via `tokio::signal`
- [ ] `Worker::register() -> Result<()>`
  - PUT `workers/{worker_id}.json` with initial WorkerInfo
- [ ] `Worker::heartbeat() -> Result<()>`
  - PUT `workers/{worker_id}.json` with updated last_heartbeat, current_task
- [ ] `Worker::deregister() -> Result<()>`
  - DELETE `workers/{worker_id}.json`
- [ ] `Worker::poll_once() -> Result<Option<Task>>`
  - Update heartbeat
  - For each shard:
    - LIST ready/{shard}/{bucket<=now} (paginated)
    - GET task, skip if `!task.is_available()` (future available_at)
    - Try to claim each eligible one
    - If task is not Pending, delete stale ready index (best-effort)
    - On successful claim: return task
  - Return None if no tasks claimed
- [ ] `Worker::run(shutdown: ShutdownSignal) -> Result<()>`
  - Call `register()` on startup
  - Loop:
    - Check shutdown, exit if true
    - `poll_once()`
    - If task claimed:
      - Execute
      - Handle result (complete/retry/fail)
    - Else:
      - Backoff sleep
  - On shutdown with running task:
    - Wait up to N seconds for completion
    - If still running: requeue (PUT with `status = Pending`, `available_at = now`)
    - PUT `ready/{shard}/{bucket}/{task_id}`
    - DELETE `leases/{shard}/{bucket}/{task_id}` (best-effort)
  - Call `deregister()` before exit
- [ ] `WorkerRunner` - combines Worker + TimeoutMonitor:
  - Optional for single-node/dev
  - In production, run TimeoutMonitor as a **separate service** or leader-elected task
  - Coordinates shutdown if co-located

**Acceptance Criteria:**
- Worker registers on startup, deregisters on shutdown
- Heartbeat is updated regularly during operation
- SIGTERM during idle exits immediately after deregistering
- SIGTERM during execution waits for completion
- Long-running task is requeued on shutdown timeout
- No lost tasks on any shutdown scenario

---

### Track B: CLI
Depends on: Phase 3.

```
crates/qo/src/
├── main.rs
└── cli/
    ├── mod.rs
    ├── submit.rs
    ├── status.rs
    ├── history.rs
    ├── list.rs
    ├── replay.rs
    ├── archive.rs
    ├── workers.rs
    └── worker.rs
```

**Deliverables:**
- [ ] CLI structure using `clap` derive:
  ```
  qo <COMMAND>

  Commands:
    submit   Submit a new task
    status   Get task status
    history  Show task version history
    list     List tasks
    replay   Replay a failed task
    archive  Archive a completed/failed task
    workers  List registered workers
    worker   Run a worker
  ```
- [ ] `qo submit --type <TYPE> --input <JSON> [--timeout <SECS>] [--retries <N>]`
  - Prints task ID on success
- [ ] `qo status <TASK_ID>`
  - Shows current task state (formatted)
  - Shows `available_at` if in future (task is in backoff)
  - Shows `lease_expires_at` countdown if running
- [ ] `qo history <TASK_ID>`
  - Shows version timeline with timestamps and status changes
- [ ] `qo list [--shard <HEX>] [--status <STATUS>] [--limit <N>]`
  - Lists tasks matching criteria
  - Default: all shards, all statuses (except Archived), limit 100
  - Show `available_at` for pending tasks in backoff
- [ ] `qo replay <TASK_ID>`
  - GET failed task, PUT with `status = Pending`, `retry_count = 0`, `available_at = now`
- [ ] `qo archive <TASK_ID>`
  - GET completed/failed task, PUT with `status = Archived`
- [ ] `qo workers`
  - LIST `workers/`, GET each, display:
    - worker_id, started_at, last_heartbeat
    - current_task (if any)
    - tasks_completed, tasks_failed
    - health status (active/stale based on heartbeat age)
- [ ] `qo worker [--id <ID>] [--shards <SHARDS>]`
  - Runs worker loop
  - Default ID: hostname + random suffix
  - Default shards: all (0-f)
- [ ] `--json` flag for machine-readable output
- [ ] Proper error messages and exit codes

**Acceptance Criteria:**
- All commands work against local S3
- Human-readable output by default
- `--json` produces valid JSON
- `workers` command shows active and stale workers
- `status` command shows scheduling fields (available_at, lease_expires_at)
- Errors are clear and actionable

---

### Track C: Web UI
Depends on: Phase 2 (can develop in parallel with 3/4A/4B).

```
ui/
├── index.html
├── app.js
└── styles.css
```

**Deliverables:**
- [ ] Static HTML/JS app (no build step, vanilla JS or minimal framework)
- [ ] S3 configuration input (endpoint, bucket, credentials)
- [ ] **Shard Selector**: Dropdown for shards 0-f
- [ ] **Task List**:
  - Fetches `ready/{shard}` + `leases/{shard}` indexes for active tasks
  - (Optional) `LIST tasks/{shard}/` for deep history/admin view
  - Displays task ID, type, status, updated_at
  - Status filter (Pending/Running via indexes; Completed/Failed/Archived via scan)
  - Show `available_at` for pending tasks in backoff (with countdown)
  - Click to select task
- [ ] **Task Detail Panel**:
  - Shows full task JSON
  - Collapsible input/output viewers (JSON tree)
  - Status badge with color coding
  - `lease_expires_at` countdown for running tasks
  - `available_at` display for pending tasks in backoff
- [ ] **Version Timeline**:
  - Calls `ListObjectVersions`
  - Shows timeline of state changes with timestamps
  - Click version to see task state at that point
  - Diff view between versions (optional)
- [ ] **Worker Dashboard**:
  - Fetches `LIST workers/` via S3 API
  - Displays worker_id, started_at, last_heartbeat
  - current_task link (if any)
  - tasks_completed, tasks_failed counters
  - Health indicator: green (active), yellow (stale), red (dead)
  - Auto-refresh with heartbeat threshold detection
- [ ] **Auto-refresh**: Toggle + interval selector (5s/10s/30s/off)
- [ ] **Actions**:
  - Replay failed task (PUT with status=Pending, available_at=now)
  - Archive completed/failed task (PUT with status=Archived)
- [ ] **Auth**: Prefer pre-signed URLs or short-lived credentials (no long-lived keys in browser)
- [ ] CORS-compatible (direct S3 API calls from browser)
- [ ] Deploy script to upload to `ui/` prefix in bucket

**Acceptance Criteria:**
- Works with Garage/MinIO locally
- Can browse all shards and filter by status
- Version history displays correctly
- Worker dashboard shows all registered workers with health status
- Replay and Archive actions work
- Scheduling fields (available_at, lease_expires_at) display correctly
- No backend server required

---

### Phase 4 Integration
Once all tracks complete:
- [ ] Full flow via CLI: submit → worker processes → status shows Completed
- [ ] Graceful shutdown: start worker, submit task, SIGTERM, task completes or requeues
- [ ] Web UI: browse tasks, view history, replay failed task
- [ ] CLI and UI show consistent data

---

## Phase 5: Integration & Testing

**Goal**: Comprehensive testing and documentation.

**Depends on**: Phase 4 complete.

### Deliverables

**Integration Tests:**
- [ ] `crates/qo/tests/integration/submit_claim_complete.rs` - happy path
- [ ] `crates/qo/tests/integration/concurrent_claim.rs` - multiple workers race
- [ ] `crates/qo/tests/integration/retry_exhaustion.rs` - task fails until Failed status
- [ ] `crates/qo/tests/integration/timeout_recovery.rs` - monitor recovers stuck task
- [ ] `crates/qo/tests/integration/pagination_starvation.rs` - ensures paginated LIST makes progress
- [ ] `crates/qo/tests/integration/index_cleanup.rs` - ready/lease orphans are harmless
- [ ] `crates/qo/tests/integration/graceful_shutdown.rs` - no lost tasks
- [ ] `crates/qo/tests/integration/version_history.rs` - history shows all transitions

**Test Infrastructure:**
- [ ] Docker Compose: Garage + test runner
- [ ] Test utilities: `submit_and_wait`, `wait_for_status`, `with_test_bucket`
- [ ] CI workflow (GitHub Actions)

**Documentation:**
- [ ] `README.md` with:
  - Quick start (local dev)
  - Architecture overview
  - CLI reference
  - Web UI setup
- [ ] Example task handlers
- [ ] Configuration reference

---

## Dependency Graph

```
                    ┌─────────────────┐
                    │  Phase 1        │
                    │                 │
              ┌─────┤  A: Models      │
              │     │  B: S3 Client   │
              │     └────────┬────────┘
              │              │
              ▼              ▼
    ┌─────────────────────────────────────┐
    │  Phase 2                            │
    │                                     │
    │  A: Submit + Get ───┐               │
    │  B: Claim + Execute ┴─► Integration │
    └─────────────────┬───────────────────┘
              │       │
              │       └───────────────────┐
              ▼                           ▼
    ┌─────────────────────────┐  ┌────────────────┐
    │  Phase 3                │  │  Phase 4C      │
    │                         │  │                │
    │  A: State Transitions ──┤  │  C: Web UI     │
    │  B: Timeout Monitor ────┤  │  (parallel)    │
    └─────────────────┬───────┘  └────────┬───────┘
                      │                   │
                      ▼                   │
    ┌─────────────────────────────────────┤
    │  Phase 4A/B                         │
    │                                     │
    │  A: Graceful Shutdown ──┐           │
    │  B: CLI ────────────────┴► ◄────────┘
    └─────────────────┬───────────────────┘
                      │
                      ▼
    ┌─────────────────────────────────────┐
    │  Phase 5                            │
    │                                     │
    │  Integration Tests                  │
    │  CI/CD                              │
    │  Documentation                      │
    └─────────────────────────────────────┘
```

---

## File Structure (Final)

```
crates/qo/src/
├── lib.rs
├── main.rs
├── cli/
│   ├── mod.rs
│   ├── submit.rs
│   ├── status.rs
│   ├── history.rs
│   ├── list.rs
│   ├── replay.rs
│   ├── archive.rs
│   ├── workers.rs
│   └── worker.rs
├── models/
│   ├── mod.rs
│   ├── task.rs          # Task, TaskStatus
│   ├── worker.rs        # WorkerInfo
│   └── retry.rs         # RetryPolicy
├── storage/
│   ├── mod.rs
│   ├── client.rs
│   └── error.rs
├── queue/
│   ├── mod.rs
│   └── ops.rs
└── worker/
    ├── mod.rs
    ├── claim.rs
    ├── execute.rs
    ├── handler.rs
    ├── transitions.rs
    ├── monitor.rs
    ├── heartbeat.rs     # Worker registration/heartbeat
    └── runner.rs

ui/
├── index.html
├── app.js
└── styles.css

crates/qo/tests/
└── integration/
    ├── common/mod.rs
    ├── submit_claim_complete.rs
    ├── concurrent_claim.rs
    ├── retry_exhaustion.rs
    ├── retry_backoff.rs         # Test available_at backoff
    ├── timeout_recovery.rs
    ├── graceful_shutdown.rs
    ├── version_history.rs
    └── worker_registration.rs   # Test worker heartbeat/health

docs/
├── MVP.md
├── PLAN.md
└── S3-REFERENCES.md
```

---

## Parallelization Summary

| Phase | Track A | Track B | Track C | Parallel? |
|-------|---------|---------|---------|-----------|
| 1 | Data Models | S3 Client | - | A ∥ B |
| 2 | Submit + Get | Claim + Execute | - | A ∥ B |
| 3 | State Transitions | Timeout Monitor | - | A ∥ B |
| 4 | Graceful Shutdown | CLI | Web UI | A ∥ B ∥ C |
| 5 | Testing & Docs | - | - | Single |

**Note**: Track C (Web UI) can start after Phase 2 completes, running in parallel with Phase 3 and 4A/4B.

---

## Key Architectural Decisions

### Why Single-Object State Machine?

1. **Atomic state transitions**: PUT with `If-Match` is true CAS
2. **No race conditions**: No delete+create sequence
3. **Free audit trail**: S3 Versioning captures every state change
4. **Simpler queries**: Task is always at `tasks/{shard}/{id}.json`

### Why Ready/Lease Indexes?

1. **Bounded LIST**: Workers list only time buckets that can contain eligible work
2. **Scales with active work**: Cost tracks ready/running tasks, not total history
3. **No correctness dependency**: Indexes are best-effort; tasks are source of truth
4. **Cheap**: Index objects are tiny and can be non-versioned

### Why `lease_id`?

1. **Prevents stale updates**: Only the current attempt can complete/retry/fail
2. **Crash-safe**: Old workers cannot overwrite newer attempts
3. **Auditable**: History shows which attempt produced each transition

### Why `available_at` for Backoff?

1. **No worker sleep**: Worker just PUTs task back with future `available_at`
2. **Visible backoff**: Backoff state is in the task, not hidden in worker memory
3. **Survives restarts**: If worker dies, backoff delay is preserved
4. **Debuggable**: Can see exactly when a task will become available
5. **Enables scheduled tasks**: Same mechanism works for delayed execution

### Why `lease_expires_at` Instead of Calculating?

1. **Simple timeout check**: Just `lease_expires_at < now`
2. **No calculation at read time**: Pre-computed when task is claimed
3. **Extendable**: Can update `lease_expires_at` to extend lease (heartbeat pattern)
4. **Visible deadline**: Easy to display countdown in UI

### Why Worker Registration via S3?

1. **No external service discovery**: Workers register themselves in S3
2. **Visible state**: UI can show all workers and their status
3. **Health detection**: Stale heartbeat = dead worker
4. **Debugging**: See which worker has which task
5. **Same storage**: No additional infrastructure
6. **Optional**: Not required for correctness

### Why `Archived` Status Instead of Delete?

1. **No delete operations**: Just status change (cheaper, simpler)
2. **Reversible**: Can "unarchive" if needed
3. **History preserved**: S3 Versioning captures archive transition
4. **Lifecycle cleanup**: S3 lifecycle rules can auto-delete Archived tasks

### Why Sharding?

1. **Distribute LIST load**: 16 shards = 16x less objects per LIST
2. **Worker partitioning**: Workers can own specific shards
3. **UI browsability**: Human-manageable chunks

### Why Static Web UI?

1. **Zero infrastructure**: No backend server
2. **Same-bucket hosting**: UI + data in one place
3. **Direct S3 access**: Browser calls S3 API directly
4. **Cost-effective**: Negligible S3 request costs
