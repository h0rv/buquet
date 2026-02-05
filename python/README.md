# buquet Python API

Python bindings for [buquet](../README.md).

## Installation

```bash
uv add buquet
```

## Quick Start

```python
import asyncio
from buquet import connect, Worker

async def main():
    queue = await connect()

    # Submit a task
    task = await queue.submit("send_email", {"to": "user@example.com"})
    print(f"Submitted: {task.id}")

    # Run a worker
    worker = Worker(queue, "worker-1", ["0", "1", "2", "3"])

    @worker.task("send_email")
    async def handle_email(input):
        print(f"Sending to {input['to']}")
        return {"sent": True}

    await worker.run()

asyncio.run(main())
```

## API Reference

### Connection

```python
queue = await connect(
    endpoint="http://localhost:4566",  # Optional: for LocalStack/MinIO
    bucket="my-bucket",
    region="us-east-1"
)
```

Or via environment variables: `S3_ENDPOINT`, `S3_BUCKET`, `S3_REGION`.

### Queue Methods

**Tasks:**
- `submit(task_type, input, timeout_seconds=300, max_retries=3, retry_policy=None, schedule_at=None) -> Task`
- `get(task_id) -> Task | None`
- `list(shard, status=None, limit=100) -> list[Task]`
- `list_ready(shard, limit=100) -> list[str]`
- `get_history(task_id) -> list[Task]`
- `now() -> str` — RFC 3339 server time

**Schedules:**
- `create_schedule(id, task_type, input, cron, ...) -> Schedule`
- `get_schedule(id) -> Schedule | None`
- `list_schedules() -> list[Schedule]`
- `delete_schedule(id)` / `enable_schedule(id)` / `disable_schedule(id)`
- `trigger_schedule(id) -> Task`

### Worker

```python
worker = Worker(queue, "worker-id", ["0", "1", "2", "3"])

@worker.task("task_type")
async def handler(input):
    return {"result": "value"}

await worker.run(WorkerRunOptions(
    poll_interval_ms=500,
    with_monitor=True,
))
```

### Error Handling

```python
from buquet import RetryableError, PermanentError

@worker.task("fetch_data")
async def fetch(input):
    try:
        return await do_work(input)
    except ConnectionError as e:
        raise RetryableError(f"Retry: {e}")  # Will retry
    except ValueError as e:
        raise PermanentError(f"Failed: {e}")  # Won't retry
```

### Retry Policy

```python
from buquet import RetryPolicy

policy = RetryPolicy(
    initial_interval_ms=1000,
    max_interval_ms=60000,
    multiplier=2.0,
    jitter_percent=0.25
)
task = await queue.submit("task", data, retry_policy=policy)
```

## Types

- `Task` — id, task_type, status, input, output, created_at, retry_count, last_error
- `TaskStatus` — Pending, Running, Completed, Failed, Archived
- `Schedule` — id, task_type, input, cron, enabled, created_at
- `ScheduleLastRun` — schedule_id, last_run_at, last_task_id, next_run_at

## Examples

See the [examples](../examples/) directory.
