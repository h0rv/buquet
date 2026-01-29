# Richer Error Messages

> **Status:** Planned
> **Effort:** ~80 lines of Rust (spread across error types)
> **Tier:** 3 (Polish)

Context-aware error messages with recovery suggestions.

## Why

Cryptic errors waste developer time. Good errors teach and guide.

## Before

```
Error: Precondition failed for object: tasks/a/550e8400-e29b-41d4-a716-446655440000.json
```

## After

```
Error: Failed to claim task 550e8400-e29b-41d4-a716-446655440000

Cause: ETag mismatch (HTTP 412 Precondition Failed)
  Another worker claimed this task between your read and write.
  This is normal in high-concurrency environments.

What happened:
  1. You read the task and got ETag "abc123"
  2. Another worker updated the task (new ETag "def456")
  3. Your conditional write failed because ETags didn't match

This is not a bug - it's the system working correctly!
The task will be processed by the worker that won the race.
```

## Error Categories

### Connection Errors
```
Error: Cannot connect to S3

Cause: Connection refused (http://localhost:3900)

Suggestions:
  - Check S3_ENDPOINT is correct
  - Ensure your S3 service is running: docker compose up -d
  - Verify network connectivity: curl http://localhost:3900
```

### Task Not Found
```
Error: Task not found: 550e8400-e29b-41d4-a716-446655440000

Suggestions:
  - The task may have been archived
  - Check the task ID is correct
  - Try `oq history 550e8400...` to see if it was previously processed
```

### Permission Errors
```
Error: Access denied to bucket 'oq-dev'

Cause: S3 returned 403 Forbidden

Suggestions:
  - Check AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are set correctly
  - Verify the credentials have permission to access this bucket
  - Check bucket policy allows your IAM user/role
```

## Implementation

```rust
#[derive(Debug, thiserror::Error)]
pub enum TaskError {
    #[error("Failed to claim task {task_id}")]
    ClaimFailed {
        task_id: Uuid,
        #[source]
        source: StorageError,
    },

    #[error("Task not found: {task_id}")]
    NotFound { task_id: Uuid },

    #[error("Task has already completed")]
    AlreadyCompleted { task_id: Uuid },
}

impl TaskError {
    pub fn suggestion(&self) -> &'static str {
        match self {
            TaskError::ClaimFailed { .. } => {
                "Another worker claimed this task first. This is normal \
                 in high-concurrency environments - the task will be \
                 processed by the worker that won the race."
            }
            TaskError::NotFound { .. } => {
                "The task may have been archived or never existed. \
                 Check the task ID and try `oq history <id>` to see \
                 if it was previously processed."
            }
            TaskError::AlreadyCompleted { .. } => {
                "This task has already finished. Check the task status \
                 with `oq status <id>` to see the result."
            }
        }
    }

    pub fn display_rich(&self) -> String {
        format!(
            "Error: {}\n\nSuggestion:\n  {}",
            self,
            self.suggestion()
        )
    }
}
```

## CLI Integration

```rust
// In main.rs error handling
fn main() -> Result<()> {
    if let Err(e) = run() {
        if let Some(task_err) = e.downcast_ref::<TaskError>() {
            eprintln!("{}", task_err.display_rich());
        } else {
            eprintln!("Error: {}", e);
        }
        std::process::exit(1);
    }
    Ok(())
}
```

## Files to Change

- `crates/oq/src/models/task.rs` - Enhance `TaskError` with suggestions
- `crates/oq/src/storage/error.rs` - Enhance `StorageError` with context
- `crates/oq/src/main.rs` - Use rich error display in CLI

## Dependencies

None
