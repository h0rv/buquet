# `oq tail` - Live Task Stream

> **Status:** COMPLETED (2026-01-26)
> **Effort:** ~100 lines of Rust
> **Tier:** 3 (Polish)

Real-time streaming of task events to the terminal.

## Why

Debugging and monitoring without opening the dashboard. Essential for:
- Watching deployments
- Debugging specific task types
- Quick health checks

## Usage

```bash
# Stream all task events
oq tail

# Filter by task type
oq tail --task-type=send_email

# Filter by status changes
oq tail --status=failed

# JSON output for piping
oq tail --json | jq '.task_id'

# Limit output
oq tail --limit=100
```

## Example Output

```
$ oq tail --task-type=process_order
[12:34:56.123] a1b2c3d4 pending → running  (worker: order-worker-1)
[12:34:56.891] a1b2c3d4 running → completed (duration: 768ms)
[12:34:57.002] e5f6g7h8 pending → running  (worker: order-worker-2)
[12:34:57.445] e5f6g7h8 running → failed   (error: "Connection refused")
[12:34:57.446] e5f6g7h8 scheduled retry    (attempt 2/3, backoff: 2.4s)
```

## JSON Output

```json
{"timestamp":"2026-01-26T12:34:56.123Z","task_id":"a1b2c3d4","from":"pending","to":"running","worker":"order-worker-1"}
{"timestamp":"2026-01-26T12:34:56.891Z","task_id":"a1b2c3d4","from":"running","to":"completed","duration_ms":768}
```

## Implementation

### Option 1: Reuse SSE endpoint

```rust
// CLI client that consumes dashboard SSE stream
async fn tail(args: TailArgs) -> Result<()> {
    let dashboard_url = args.dashboard_url.unwrap_or("http://localhost:3000".into());
    let mut stream = reqwest::Client::new()
        .get(format!("{}/sse/tasks", dashboard_url))
        .send()
        .await?
        .bytes_stream();

    while let Some(chunk) = stream.next().await {
        let event = parse_sse_event(&chunk?)?;
        if matches_filter(&event, &args) {
            print_event(&event, args.json);
        }
    }
    Ok(())
}
```

### Option 2: Direct S3 polling

```rust
// Poll S3 directly without dashboard
async fn tail(args: TailArgs) -> Result<()> {
    let queue = create_queue().await?;
    let mut seen = HashSet::new();

    loop {
        for shard in &shards {
            let tasks = queue.list(shard, None, 100).await?;
            for task in tasks {
                if !seen.contains(&task.id) {
                    print_task(&task, args.json);
                    seen.insert(task.id);
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
```

## CLI Arguments

```rust
#[derive(Args)]
pub struct TailArgs {
    /// Filter by task type
    #[arg(short = 't', long)]
    task_type: Option<String>,

    /// Filter by status
    #[arg(long)]
    status: Option<String>,

    /// Output as JSON (one event per line)
    #[arg(long)]
    json: bool,

    /// Dashboard URL (default: http://localhost:3000)
    #[arg(long)]
    dashboard_url: Option<String>,

    /// Stop after N events
    #[arg(long)]
    limit: Option<usize>,
}
```

## Files to Change

- `crates/oq/src/cli/commands.rs` - Add `Tail` variant
- `crates/oq/src/main.rs` - Add handler
- `Cargo.toml` - May need `eventsource-client` or similar

## Dependencies

`reqwest` with streaming support, or `eventsource-client`
