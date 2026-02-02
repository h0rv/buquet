# Python Worker Lifecycle Hooks

> **Status:** COMPLETED (2026-01-26)
> **Effort:** ~120 lines of Rust
> **Tier:** 2 (Production Essential)

Decorators for worker events: startup, shutdown, success, error.

## Why

Real workers need:
- Connection pools initialized before processing
- Graceful cleanup on shutdown
- Custom error handling (alerting, dead letter queues)
- Success callbacks (metrics, logging)

## API

```python
from buquet import connect, Worker

async def main():
    queue = await connect()
    worker = Worker(queue, "email-worker", shards)

    @worker.on_startup
    async def startup():
        """Called once when worker starts, before processing any tasks."""
        global db_pool
        db_pool = await create_pool()
        print("Database pool initialized")

    @worker.on_shutdown
    async def shutdown():
        """Called when worker receives shutdown signal."""
        await db_pool.close()
        print("Cleanup complete")

    @worker.on_success
    async def on_success(task):
        """Called after each successful task completion."""
        metrics.increment("tasks.completed", tags={"type": task.task_type})

    @worker.on_error
    async def on_error(task, error):
        """Called after task failure (before potential retry)."""
        await alert_slack(f"Task {task.id} failed: {error}")
        if task.retry_count >= task.max_retries:
            await dead_letter_queue.push(task)

    @worker.task("send_email")
    async def handle_email(input):
        # ... handler code
        pass

    await worker.run()
```

## Hook Signatures

| Hook | Called When | Arguments | Async |
|------|-------------|-----------|-------|
| `on_startup` | Worker starts, before first poll | None | Yes |
| `on_shutdown` | Shutdown signal received | None | Yes |
| `on_success` | Task completed successfully | `task: Task` | Yes |
| `on_error` | Task failed (before retry decision) | `task: Task, error: str` | Yes |

## Implementation (PyO3)

```rust
#[pyclass]
pub struct PyWorker {
    // ... existing fields
    on_startup: Option<PyObject>,
    on_shutdown: Option<PyObject>,
    on_success: Option<PyObject>,
    on_error: Option<PyObject>,
}

#[pymethods]
impl PyWorker {
    /// Register startup hook
    fn on_startup(&mut self, py: Python<'_>, func: PyObject) -> PyResult<PyObject> {
        self.on_startup = Some(func.clone());
        Ok(func)  // Return for use as decorator
    }

    /// Register shutdown hook
    fn on_shutdown(&mut self, py: Python<'_>, func: PyObject) -> PyResult<PyObject> {
        self.on_shutdown = Some(func.clone());
        Ok(func)
    }

    /// Register success hook
    fn on_success(&mut self, py: Python<'_>, func: PyObject) -> PyResult<PyObject> {
        self.on_success = Some(func.clone());
        Ok(func)
    }

    /// Register error hook
    fn on_error(&mut self, py: Python<'_>, func: PyObject) -> PyResult<PyObject> {
        self.on_error = Some(func.clone());
        Ok(func)
    }
}

// In run loop:
if let Some(ref on_success) = self.on_success {
    let task_obj = PyTask { inner: task.clone() };
    // Await if coroutine
    let result = on_success.call1(py, (task_obj,))?;
    if result.getattr(py, "__await__").is_ok() {
        pyo3_async_runtimes::tokio::into_future(result.bind(py))?.await?;
    }
}
```

## Files to Change

- `crates/buquet/src/python/worker.rs` - Add hook fields and decorators
- `crates/buquet/python/buquet/_buquet.pyi` - Add type stubs for hooks

## Dependencies

None (uses existing PyO3)
