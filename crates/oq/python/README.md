# oq - S3-Only Task Queue for Python

A distributed task queue that uses S3 as the only backend. No Redis, no Postgres, no RabbitMQ - just S3.

## Installation

```bash
uv add oq
```

## Quick Start

### Submit a Task

```python
import asyncio
from oq import connect

async def main():
    queue = await connect()

    task = await queue.submit("send_email", {
        "to": "user@example.com",
        "subject": "Hello!",
        "body": "Welcome to oq!"
    })

    print(f"Submitted task: {task.id}")

asyncio.run(main())
```

### Run a Worker

```python
import asyncio
from oq import connect, Worker, WorkerRunOptions

async def main():
    queue = await connect()
    worker = Worker(queue, "worker-1", ["0", "1", "2", "3"])

    @worker.task("send_email")
    async def handle_email(input):
        print(f"Sending email to {input['to']}")
        # Your email logic here
        return {"sent": True, "to": input["to"]}

    await worker.run()

asyncio.run(main())
```

Custom run options:

```python
opts = WorkerRunOptions(
    poll_interval_ms=500,
    with_monitor=True,
    monitor_check_interval_s=15,
)
await worker.run(opts)
```

By default, `run()` starts an embedded timeout monitor. If you run a separate
`oq monitor` process, disable it with `WorkerRunOptions(with_monitor=False)`.

## Configuration

oq reads configuration from environment variables:

| Variable | Required | Description |
|----------|----------|-------------|
| `S3_ENDPOINT` | No | S3-compatible endpoint URL (for LocalStack, MinIO, etc.) |
| `S3_BUCKET` | Yes | Bucket name for task storage |
| `S3_REGION` | Yes | AWS region |
| `AWS_ACCESS_KEY_ID` | Yes | S3 access key |
| `AWS_SECRET_ACCESS_KEY` | Yes | S3 secret key |

You can also pass configuration directly:

```python
queue = await connect(
    endpoint="http://localhost:4566",  # LocalStack
    bucket="oq-dev",
    region="us-east-1"
)
```

## API Reference

### `connect(endpoint=None, bucket=None, region=None) -> Queue`

Creates a connection to the task queue.

### `Queue`

**Task methods:**
- `submit(task_type, input, timeout_seconds=300, max_retries=3, retry_policy=None, schedule_at=None) -> Task`
- `get(task_id) -> Task | None`
- `list(shard, status=None, limit=100) -> list[Task]`
- `list_ready(shard, limit=100) -> list[str]`
- `get_history(task_id) -> list[Task]`
- `now() -> str`  # RFC 3339 server time

**Schedule methods:**
- `create_schedule(id, task_type, input, cron, timeout_seconds=None, max_retries=None) -> Schedule`
- `get_schedule(id) -> Schedule | None`
- `list_schedules() -> list[Schedule]`
- `delete_schedule(id)`
- `enable_schedule(id)`
- `disable_schedule(id)`
- `trigger_schedule(id) -> Task`
- `get_schedule_last_run(id) -> ScheduleLastRun | None`

### `Worker`

- `Worker(queue, worker_id, shards)`
- `@worker.task(task_type)` - Decorator to register a task handler
- `run(options=None)` - Run the worker loop (monitor enabled by default). Customize via `WorkerRunOptions`.

### `Task`

Properties:
- `id` - Unique task identifier (UUID)
- `task_type` - Type of task (e.g., "send_email")
- `status` - Current status (pending, running, completed, failed)
- `input` - Input data (dict)
- `output` - Output data after completion (dict or None)
- `created_at` - Creation timestamp
- `retry_count` - Number of retries so far
- `last_error` - Last error message (if any)

Note: time-dependent checks require an authoritative time. Use `queue.now()` and the `Task.*_at()` helpers.

Example:

```python
now = await queue.now()
if task.is_available_at(now):
    ...
```

### `TaskStatus`

Enum with values:
- `TaskStatus.Pending`
- `TaskStatus.Running`
- `TaskStatus.Completed`
- `TaskStatus.Failed`
- `TaskStatus.Archived`

### `Schedule`

A recurring schedule definition. Properties:
- `id` - Unique schedule identifier
- `task_type` - Type of task to submit
- `input` - Input data for tasks (dict)
- `cron` - Cron expression (5-field format)
- `enabled` - Whether the schedule is active
- `timeout_seconds` - Task timeout (optional)
- `max_retries` - Task max retries (optional)
- `created_at` - Creation timestamp
- `updated_at` - Last update timestamp

### `ScheduleLastRun`

Last run information for a schedule. Properties:
- `schedule_id` - The schedule ID
- `last_run_at` - When the schedule was last triggered
- `last_task_id` - Task ID of the last submitted task
- `next_run_at` - Next scheduled run time

### `RetryPolicy`

Configure exponential backoff:

```python
from oq import RetryPolicy

policy = RetryPolicy(
    initial_interval_ms=1000,  # Start with 1s delay
    max_interval_ms=60000,     # Cap at 60s
    multiplier=2.0,            # Double each time
    jitter_percent=0.25        # Add 25% randomness
)

task = await queue.submit("task", data, retry_policy=policy)
```

## Scheduling

oq supports scheduling tasks to run at a future time.

### One-Shot Scheduling

```python
from datetime import datetime, timedelta

# Schedule a task to run in 1 hour
task = await queue.submit(
    "send_reminder",
    {"user_id": "123"},
    schedule_at=datetime.now() + timedelta(hours=1),
)
print(f"Task {task.id} scheduled for {task.available_at}")

# Schedule for a specific time
task = await queue.submit(
    "generate_report",
    {"type": "daily"},
    schedule_at=datetime(2026, 1, 28, 9, 0, 0),
)
```

The task won't be picked up by workers until the scheduled time.

### Recurring Schedules

Create and manage cron-based recurring schedules:

```python
# Create a schedule (runs every day at 9am)
schedule = await queue.create_schedule(
    "daily-report",
    "generate_report",
    {"type": "daily"},
    "0 9 * * *"
)

# List all schedules
schedules = await queue.list_schedules()
for s in schedules:
    print(f"{s.id}: {s.cron} (enabled={s.enabled})")

# Get a specific schedule
schedule = await queue.get_schedule("daily-report")

# Enable/disable
await queue.disable_schedule("daily-report")
await queue.enable_schedule("daily-report")

# Trigger immediately (outside normal schedule)
task = await queue.trigger_schedule("daily-report")

# Delete
await queue.delete_schedule("daily-report")
```

Run the scheduler daemon via CLI:
```bash
oq schedule run --interval 60
```

## Error Handling

Raise `RetryableError` to retry a task:

```python
from oq import RetryableError, PermanentError

@worker.task("fetch_data")
async def fetch_data(input):
    try:
        response = await http_client.get(input["url"])
        return {"data": response.json()}
    except ConnectionError as e:
        # Will be retried
        raise RetryableError(f"Connection failed: {e}")
    except ValueError as e:
        # Will NOT be retried
        raise PermanentError(f"Invalid data: {e}")
```

## Local Development

See the [examples](../examples/) directory for complete working examples.

### Quick Start with Docker

```bash
# Start LocalStack (S3-compatible storage)
docker compose up -d

# Run Python commands from the oq crate directory
cd crates/oq

# Run the example worker
source examples/.env.example
uv run python examples/worker.py

# In another terminal, submit tasks
uv run python examples/producer.py
```

## Architecture

```
┌─────────────────┐     ┌─────────────────┐
│    Producer     │     │     Worker      │
│  (your app)     │     │  (your app)     │
└────────┬────────┘     └────────┬────────┘
         │                       │
         │  submit()             │  poll/claim/complete
         │                       │
         ▼                       ▼
┌─────────────────────────────────────────┐
│                   S3                     │
│  (AWS S3 / LocalStack / MinIO / R2)     │
└─────────────────────────────────────────┘
```

Tasks are stored as JSON objects in S3 with conditional writes (ETags) for consistency.
