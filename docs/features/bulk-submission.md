# Bulk Task Submission

> **Status:** COMPLETED (2026-01-26)
> **Effort:** ~80 lines of Rust
> **Tier:** 2 (Production Essential)

Submit multiple tasks in a single call.

## Why

- Submitting 1000 tasks with `submit()` = 1000 S3 PUTs = slow
- Bulk submission can batch requests or use parallel uploads
- Common pattern: import jobs, batch notifications, scheduled reports

## Python API

```python
# Submit many tasks at once
tasks = await queue.submit_many([
    ("send_email", {"to": "alice@example.com", "subject": "Hello"}),
    ("send_email", {"to": "bob@example.com", "subject": "Hi"}),
    ("process_order", {"order_id": "ORD-123"}),
])

print(f"Submitted {len(tasks)} tasks")
for task in tasks:
    print(f"  {task.id}: {task.task_type}")
```

## With Options

```python
from oq import RetryPolicy

tasks = await queue.submit_many(
    [
        ("send_email", {"to": "user@example.com"}),
        ("send_email", {"to": "other@example.com"}),
    ],
    max_retries=5,
    retry_policy=RetryPolicy(initial_interval_ms=5000),
    timeout_seconds=300,
)
```

## Implementation

```rust
#[pymethods]
impl PyQueue {
    /// Submit multiple tasks efficiently.
    #[pyo3(signature = (tasks, timeout_seconds=None, max_retries=None))]
    fn submit_many<'py>(
        &self,
        py: Python<'py>,
        tasks: Vec<(String, PyObject)>,
        timeout_seconds: Option<u64>,
        max_retries: Option<u32>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let queue = self.inner.clone();
        let tasks: Vec<_> = tasks.into_iter()
            .map(|(tt, input)| (tt, py_to_json(py, input)))
            .collect::<Result<Vec<_>, _>>()?;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            // Use join_all for parallel submission
            let futures = tasks.into_iter().map(|(task_type, input)| {
                let q = queue.clone();
                async move {
                    q.submit(&task_type, input, SubmitOptions {
                        timeout_seconds,
                        max_retries,
                        ..Default::default()
                    }).await
                }
            });

            let results = futures::future::join_all(futures).await;
            let tasks: Result<Vec<_>, _> = results.into_iter().collect();
            Ok(tasks?.into_iter().map(|t| PyTask { inner: t }).collect::<Vec<_>>())
        })
    }
}
```

## CLI Support

```bash
# Submit from JSON file
oq submit-many tasks.json

# Submit from stdin (one JSON per line)
cat tasks.jsonl | oq submit-many -
```

## Performance

- Uses `futures::future::join_all` for concurrent S3 writes
- Could add configurable concurrency limit (default: 10)
- Consider S3 batch operations if available

## Files to Change

- `crates/oq/src/python/queue.rs` - Add `submit_many` method
- `crates/oq/python/oq/_oq.pyi` - Add type stub
- `crates/oq/src/cli/commands.rs` - Add `SubmitMany` command (optional)

## Dependencies

`futures` (already in deps)
