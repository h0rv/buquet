# oq

**oq** is a task queue that chooses simplicity and recoverability over speed.

It is a simple, low-overhead distributed task queue built entirely on S3-compatible object storage.

No databases.
No message brokers.
No coordination services.

Just workers polling object storage and claiming tasks atomically.

## Why oq?

* Uses **object storage as the only dependency**
* Strong consistency + conditional writes for safe task claiming
* Designed to be **boring, understandable, and reliable**
* Easy to run locally or in production
* Scales by adding workers, not infrastructure

If you can run S3, you can run oq.

## Core Ideas

* Tasks are JSON objects stored at a single path for their lifetime
* Workers poll **ready index** objects first, with optional log-scan fallback
* Atomic claiming via `If-Match` (CAS on the task object)
* Retries, timeouts, and DLQ handled with simple state transitions
* Crash-safe by design (leases expire and are recovered)
* At-least-once execution (handlers should be idempotent or deduplicated)
* Bucket versioning is required and enforced at startup for audit history
* **Scheduling**: one-shot delayed tasks and recurring cron schedules

### Versioning (dev override)

If your S3 backend does not support bucket versioning (e.g., Garage), you can
override the startup check for local dev:

```bash
OQ_ALLOW_NO_VERSIONING=1
```

This **disables history/audit guarantees** (`get_history()` will not be accurate).

## Architecture (at a glance)

```
tasks/{shard}/{task_id}.json      # single task object (versioned)
ready/{shard}/{bucket}/{task_id}  # index of claimable tasks
leases/{shard}/{bucket}/{task_id} # index of running tasks by expiry
workers/{worker_id}.json          # optional heartbeat / UI
```

Workers move tasks **between states in-place** (single object). Ready/lease
indexes are **projections** for efficiency and can be repaired from the task log.

## Operations

- `oq worker` runs a worker loop with an embedded monitor by default (use `--no-monitor` to disable).
- `oq monitor` runs the timeout monitor + index sweeper.

## Scheduling

oq supports both one-shot delayed tasks and recurring cron schedules.

### One-Shot Scheduling

Schedule a task to run at a specific time or after a delay:

```bash
# Run at a specific time
oq submit -t send_report -i '{}' --at "2026-01-28T09:00:00Z"

# Run after a delay
oq submit -t send_reminder -i '{}' --delay 1h
```

Python:
```python
from datetime import datetime, timedelta

# Schedule for 1 hour from now
task = await queue.submit(
    "send_reminder",
    {"user_id": "123"},
    schedule_at=datetime.now() + timedelta(hours=1),
)
```

### Recurring Schedules

Create cron-based recurring schedules:

```bash
# Create a schedule (runs every day at 9am UTC)
oq schedule create daily-report -t generate_report -i '{}' -c "0 9 * * *"

# List schedules
oq schedule list

# Run the scheduler daemon
oq schedule run --interval 60
```

The scheduler daemon checks for due schedules and submits tasks. Multiple scheduler instances can run safely (CAS ensures only one triggers each scheduled run).

## Task Cancellation

Cancel pending tasks before they're processed:

```bash
# Cancel a single task
oq cancel abc123 --reason "No longer needed"

# Cancel multiple tasks
oq cancel abc123 def456 ghi789

# Cancel all pending tasks of a type
oq cancel --task-type old_task_type
```

Python:
```python
# Cancel a task
await queue.cancel(task_id, reason="User requested")

# Batch cancel
await queue.cancel_many([task_id1, task_id2])

# Cancel by type
result = await queue.cancel_by_type("old_task_type")
print(f"Cancelled {len(result.cancelled)} tasks")

# Cooperative cancellation for running tasks
@worker.task("long_job")
async def handle(input, context):
    for item in items:
        if await context.is_cancellation_requested():
            return None  # Worker marks as Cancelled
        await process(item)
```

## Task Expiration (TTL)

Tasks can expire if not processed within a deadline:

```bash
# Submit with relative TTL
oq submit -t send_reminder -i '{}' --ttl 1h

# Submit with absolute expiration
oq submit -t time_sensitive -i '{}' --expires-at "2026-01-28T17:00:00Z"
```

Python:
```python
from datetime import datetime, timedelta

# Submit with TTL (expires in 1 hour)
task = await queue.submit(
    "process_event",
    {"event_id": "123"},
    ttl_seconds=3600,
)

# Submit with absolute expiration
task = await queue.submit(
    "meeting_reminder",
    data,
    expires_at=datetime(2026, 1, 28, 17, 0, 0),
)

# Bulk submit with TTL
tasks = await queue.submit_many([
    ("send_email", {"to": "alice@example.com"}),
    ("send_email", {"to": "bob@example.com"}),
], ttl_seconds=1800)  # 30 minute TTL
```

Expired tasks are automatically marked as `Expired` and skipped by workers.

## What oq Is (and Isn't)

**oq is:**

* A simple background task queue
* Suitable for batch jobs, async work, and glue tasks
* Easy to inspect and debug (everything is an object)

**oq is not:**

* A workflow engine
* A real-time queue
* A replacement for Kafka/SQS when you need sub-second latency

## Tradeoffs

Here’s a **concise “Tradeoffs” section** for your README that honestly frames oq versus other approaches:

---

## Tradeoffs

**oq is deliberately minimal. Understanding its tradeoffs is key.**

| Aspect                   | oq (S3-only)                                     | Traditional Queues (SQS, RabbitMQ, Kafka)                             | Notes                                              |
| ------------------------ | ------------------------------------------------ | --------------------------------------------------------------------- | -------------------------------------------------- |
| **Infrastructure**       | Single S3 bucket, no control plane               | Dedicated broker, cluster, or DB                                      | oq is operationally trivial                        |
| **Setup & Ops**          | Minimal, inspectable (`aws s3 ls`)               | More moving parts, monitoring, failover                               | Great for small teams or internal tooling          |
| **Throughput / Latency** | Seconds of latency, LIST via index buckets       | Sub-second latency, high throughput                                   | oq is not for hot, real-time workloads             |
| **Delivery Guarantees**  | At-least-once execution, exactly-once claim      | SQS FIFO: exactly-once optional, Kafka: exactly-once via transactions | True exactly-once requires idempotent side effects |
| **Task Ordering**        | None                                             | FIFO / partitioned ordering                                           | oq trades ordering for simplicity                  |
| **Failure Recovery**     | Workers + timeout monitor handle crashes         | Broker handles in-flight messages                                     | oq recovery is deterministic and transparent       |
| **Scaling**              | Add workers; LIST sharding improves medium-scale | Brokers may need partitions, clusters                                 | oq scales linearly but LIST is O(N/shard)          |
| **Extensibility**        | No control plane → simple to reason about        | Can do DAGs, priorities, scheduling                                   | oq stays small; features deferred intentionally    |
| **Debuggability**        | Excellent — all state in S3                      | Harder to inspect without tooling                                     | Inspect or replay any task with `aws s3`           |
| **Cost**                 | Extremely low — storage + requests               | Broker infrastructure + compute + storage                             | Ideal for internal or batch workloads              |

**Key takeaways**

* **Pros**: simple, durable, transparent, minimal ops, production-usable for async batch workloads
* **Cons**: not real-time, LIST cost grows with active indexes, execution is at-least-once, tasks must be idempotent or deduplicated

> **philosophy**: oq trades features, speed, and orchestration for simplicity, reliability, and low operational overhead. It is intentionally “boring” - which is a feature, not a bug.
