# Filter `buquet list` by Task Type

> **Status:** COMPLETED (2026-01-26)
> **Effort:** ~15 lines of Rust
> **Tier:** 1 (Quick Win)

Add `--task-type` flag to filter task listings.

## Why

In production, you often have multiple task types. Finding all failed `send_email` tasks requires grep today. Native filtering is cleaner.

## Usage

```bash
# All failed email tasks
buquet list --task-type=send_email --status=failed

# All running order tasks in shard 'a'
buquet list -s a --task-type=process_order --status=running

# Short form
buquet list -t send_email
```

## Implementation

```rust
#[derive(Args)]
pub struct ListArgs {
    #[arg(short, long)]
    shard: Option<String>,

    #[arg(short = 't', long)]
    task_type: Option<String>,  // NEW

    #[arg(long)]
    status: Option<TaskStatus>,

    #[arg(short, long, default_value = "100")]
    limit: usize,
}

// In handler, filter after fetching:
let tasks = queue.list(shard, status, limit).await?;
let tasks = match &args.task_type {
    Some(tt) => tasks.into_iter().filter(|t| t.task_type == *tt).collect(),
    None => tasks,
};
```

## Files to Change

- `crates/buquet/src/cli/commands.rs` - Add `task_type` field to `List` variant
- `crates/buquet/src/main.rs` - Add filter logic in `Commands::List` handler

## Dependencies

None
