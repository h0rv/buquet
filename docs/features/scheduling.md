# Feature: Task Scheduling

## Status: Implemented

## Overview

Enable tasks to be scheduled for future execution — either at a specific time
(one-shot) or on a recurring basis (cron-like). This uses S3 as the only
scheduler, with no external cron service required.

## Problem

Users need to:
1. Submit tasks that run at a specific future time (e.g., "send reminder in 1 hour")
2. Run recurring tasks on a schedule (e.g., "generate report every day at 9am")

Currently, `available_at` exists but isn't exposed in the API. Recurring tasks
require external cron, which defeats the "S3-only" value proposition.

## Solution

### One-Shot Scheduling

Expose `schedule_at` in the submit API, which sets `available_at` on the task.
Tasks with future `available_at` won't appear in the ready index until that time.

### Recurring Schedules

Store schedule definitions in S3. A lightweight scheduler task (runs as a
regular buquet worker) checks schedules and submits tasks when due.

## Storage Layout

```
schedules/
  {schedule_id}.json          # schedule definition
  {schedule_id}/
    last_run.json             # last execution timestamp (CAS)
    history/{timestamp}.json  # optional execution history
```

### Schedule Definition

```json
{
  "id": "daily-report",
  "task_type": "generate_report",
  "input": {"report_type": "daily"},
  "cron": "0 9 * * *",
  "enabled": true,
  "created_at": "2026-01-27T12:00:00Z",
  "updated_at": "2026-01-27T12:00:00Z"
}
```

### Last Run Tracking

```json
{
  "schedule_id": "daily-report",
  "last_run_at": "2026-01-27T09:00:00Z",
  "last_task_id": "abc123",
  "next_run_at": "2026-01-28T09:00:00Z"
}
```

## API

### Python

```python
from datetime import datetime, timedelta
from buquet import connect

queue = await connect(bucket="my-queue")

# One-shot: run in 1 hour
task = await queue.submit(
    "send_reminder",
    {"user_id": "123"},
    schedule_at=datetime.now() + timedelta(hours=1)
)

# One-shot: run at specific time
task = await queue.submit(
    "generate_report",
    {"report_type": "daily"},
    schedule_at=datetime(2026, 1, 28, 9, 0, 0)
)

# Recurring: create a schedule
schedule = await queue.create_schedule(
    schedule_id="daily-report",
    task_type="generate_report",
    input={"report_type": "daily"},
    cron="0 9 * * *",
)

# List schedules
schedules = await queue.list_schedules()

# Pause/resume
await queue.pause_schedule("daily-report")
await queue.resume_schedule("daily-report")

# Delete
await queue.delete_schedule("daily-report")
```

### Rust

```rust
use buquet::{Queue, Schedule};
use chrono::{Utc, Duration};

let queue = Queue::connect("my-queue").await?;

// One-shot: run in 1 hour
let task = queue
    .submit("send_reminder")
    .input(json!({"user_id": "123"}))
    .schedule_at(Utc::now() + Duration::hours(1))
    .send()
    .await?;

// Recurring schedule
let schedule = queue
    .create_schedule("daily-report")
    .task_type("generate_report")
    .input(json!({"report_type": "daily"}))
    .cron("0 9 * * *")
    .send()
    .await?;
```

### CLI

```bash
# One-shot scheduling
buquet submit -t send_reminder -i '{"user_id":"123"}' --at "2026-01-28T09:00:00Z"
buquet submit -t send_reminder -i '{"user_id":"123"}' --in 1h

# Create recurring schedule
buquet schedule create daily-report \
    --task-type generate_report \
    --input '{"report_type":"daily"}' \
    --cron "0 9 * * *"

# List schedules
buquet schedule list

# Show schedule details
buquet schedule show daily-report

# Pause/resume
buquet schedule pause daily-report
buquet schedule resume daily-report

# Trigger immediately (outside schedule)
buquet schedule trigger daily-report

# Delete
buquet schedule delete daily-report
```

## Scheduler Implementation

The scheduler is a special worker that:

1. Polls `schedules/*.json` periodically (e.g., every 60 seconds)
2. For each enabled schedule, checks if it's due to run
3. Uses CAS on `last_run.json` to ensure only one instance triggers
4. Submits the task and updates `last_run.json`

```rust
// Pseudo-code for scheduler loop
loop {
    let schedules = list_schedules().await?;

    for schedule in schedules {
        if !schedule.enabled { continue; }

        let last_run = get_last_run(&schedule.id).await?;
        let next_run = cron_next(&schedule.cron, last_run.last_run_at);

        if Utc::now() >= next_run {
            // CAS update to claim this run
            let updated = update_last_run_cas(
                &schedule.id,
                last_run.etag,
                LastRun {
                    last_run_at: Utc::now(),
                    next_run_at: cron_next(&schedule.cron, Utc::now()),
                    ..
                }
            ).await;

            if updated.is_ok() {
                // We won the race, submit the task
                queue.submit(&schedule.task_type, &schedule.input).await?;
            }
        }
    }

    sleep(Duration::from_secs(60)).await;
}
```

### Running the Scheduler

```bash
# Run scheduler as part of buquet monitor
buquet monitor --enable-scheduler

# Or as standalone
buquet scheduler run
```

The scheduler uses the same CAS primitives as the rest of buquet — multiple
scheduler instances can run safely (only one will trigger each scheduled task).

## Cron Syntax

Standard 5-field cron syntax:

```
┌───────────── minute (0-59)
│ ┌───────────── hour (0-23)
│ │ ┌───────────── day of month (1-31)
│ │ │ ┌───────────── month (1-12)
│ │ │ │ ┌───────────── day of week (0-6, Sun=0)
│ │ │ │ │
* * * * *
```

Examples:
- `0 9 * * *` — Every day at 9:00 AM
- `*/15 * * * *` — Every 15 minutes
- `0 0 * * 0` — Every Sunday at midnight
- `0 9 * * 1-5` — Weekdays at 9:00 AM
- `0 0 1 * *` — First of every month at midnight

## Timezone Handling

Cron schedules are evaluated in **UTC only**. Non-UTC timezones are not supported.

## Missed Runs

If the scheduler is down during a scheduled time, missed runs are skipped and
the next future run is scheduled.

## Guarantees

- **At-least-once triggering** — CAS ensures exactly one trigger per schedule window
- **Distributed-safe** — Multiple schedulers can run without duplicates
- **Durable** — Schedule state survives restarts
- **Inspectable** — All state is JSON in S3

## Limitations

- **Minimum granularity:** ~1 minute (polling interval)
- **Not for sub-second scheduling** — Use a different tool
- **Clock skew:** Assumes workers have reasonably synchronized clocks

## Implementation Phases

### Phase 1: One-Shot Scheduling
- Expose `schedule_at` in submit API
- CLI `--at` and `--in` flags
- No new storage, just uses existing `available_at`

### Phase 2: Recurring Schedules
- Schedule definition storage
- Scheduler worker implementation
- CLI `buquet schedule` commands

### Phase 3: Advanced Features
- Execution history
- Catch-up for missed runs
- Schedule versioning

## Dependencies

- `cron` crate for parsing cron expressions

## See Also

- [Adaptive Polling](adaptive-polling.md) — Scheduler uses similar polling patterns
