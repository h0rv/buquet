# Feature: Rate Limiting via Durable Sleep

## Status: Implemented

## Overview

Handle rate limits and backpressure by allowing task handlers to reschedule
themselves for later execution. When a handler encounters a rate limit (e.g.,
429 response), it raises `RescheduleError` with a delay. The worker updates
the task's `available_at` and immediately moves on to other work.

This is **reactive rate limiting** - responding to actual limits from downstream
systems rather than predicting them with token buckets.

## Philosophy Alignment

This approach aligns with oq's core principles:

| Principle | How This Aligns |
|-----------|-----------------|
| S3 as only dependency | No new infrastructure - uses existing `available_at` |
| Simple, boring, reliable | One new exception type, trivial implementation |
| No coordination services | No shared state, no CAS contention |
| Crash-safe | Task is pending, will be picked up after delay |
| Application-aware | Handler knows actual rate limit from API response |

## Problem

- External APIs return 429 with `Retry-After` headers
- Downstream services signal backpressure
- Workers shouldn't block waiting - they should process other tasks
- Current options: retry (wrong semantics) or fail (loses work)

## Solution

Add `RescheduleError` exception that:
1. Updates task's `available_at` to `now + delay`
2. Returns task to `Pending` status
3. Frees worker to process other tasks
4. Task becomes claimable again after delay

## API

### Python

```python
from oq import connect, Worker, RescheduleError

@worker.task("call_api")
async def call_api(input):
    try:
        response = await external_api.post(input["url"], input["data"])
        return {"status": response.status}
    except RateLimitError as e:
        # API returned 429 - reschedule for later
        raise RescheduleError(delay_seconds=e.retry_after or 60)
    except ServiceUnavailable:
        # Backpressure - try again in 5 minutes
        raise RescheduleError(delay_seconds=300)
```

#### Submit with max_reschedules

```python
# Unlimited reschedules (default)
task = await queue.submit("call_api", {"url": "..."})

# Limit reschedules to prevent infinite loops
task = await queue.submit(
    "call_api",
    {"url": "..."},
    max_reschedules=100,  # Fail after 100 reschedules
)

# Combined with other options
task = await queue.submit(
    "call_api",
    {"url": "..."},
    max_retries=3,        # Retries for actual failures
    max_reschedules=50,   # Reschedules for rate limits
    timeout_seconds=300,
)
```

### Rust

```rust
use oq::{Queue, TaskError, RescheduleError};

// In handler
async fn call_api(input: Value) -> Result<Value, TaskError> {
    match external_api.call(&input).await {
        Ok(response) => Ok(json!({"status": "ok"})),
        Err(e) if e.is_rate_limited() => {
            Err(TaskError::Reschedule {
                delay_seconds: e.retry_after().unwrap_or(60)
            })
        }
        Err(e) => Err(TaskError::Retryable(e.to_string())),
    }
}

// Submit with max_reschedules
queue.submit("call_api", input)
    .max_reschedules(100)
    .send()
    .await?;
```

### CLI

```bash
# Submit with max reschedules
oq submit -t call_api -i '{"url": "..."}' --max-reschedules 100

# Check task reschedule count
oq get <task_id>
# Shows: reschedule_count: 5, max_reschedules: 100
```

## Task Fields

New fields added to Task:

```json
{
  "id": "abc123",
  "task_type": "call_api",
  "status": "pending",
  "input": {"url": "..."},

  "retry_count": 0,
  "max_retries": 3,

  "reschedule_count": 5,
  "max_reschedules": null,

  "available_at": "2026-01-27T12:05:00Z",
  "created_at": "2026-01-27T12:00:00Z"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `reschedule_count` | `u32` | Times task has been rescheduled |
| `max_reschedules` | `Option<u32>` | Limit before failing (null = unlimited) |

## Behavior

### Reschedule Flow

```
Handler                           Worker                          S3
   │                                │                              │
   │  raise RescheduleError(60s)    │                              │
   │───────────────────────────────►│                              │
   │                                │                              │
   │                                │  Update task:                │
   │                                │  - status: pending           │
   │                                │  - available_at: now + 60s   │
   │                                │  - reschedule_count += 1     │
   │                                │─────────────────────────────►│
   │                                │                              │
   │                                │  Remove from lease index     │
   │                                │─────────────────────────────►│
   │                                │                              │
   │                                │  (pick up next task)         │
```

### Reschedule vs Retry

| Aspect | Retry | Reschedule |
|--------|-------|------------|
| Triggered by | Failure/crash | Explicit `RescheduleError` |
| Delay | Automatic (backoff) | Explicit (handler specifies) |
| Counter | `retry_count` | `reschedule_count` |
| Limit | `max_retries` (default: 3) | `max_reschedules` (default: unlimited) |
| Semantics | "Something went wrong" | "Try again later" |
| Use case | Transient errors | Rate limits, backpressure |

### When max_reschedules Exceeded

```python
# Task has been rescheduled 100 times, max is 100
# Next reschedule will fail the task permanently

@worker.task("call_api")
async def call_api(input):
    raise RescheduleError(delay_seconds=60)
    # Task transitions to Failed with:
    # last_error: "Max reschedules (100) exceeded"
```

## Use Cases

### 1. API Rate Limiting

```python
@worker.task("send_email")
async def send_email(input):
    try:
        await email_provider.send(input["to"], input["body"])
        return {"sent": True}
    except TooManyRequests as e:
        # Provider says wait 30 seconds
        raise RescheduleError(delay_seconds=e.retry_after)
```

### 2. Upstream Backpressure

```python
@worker.task("process_order")
async def process_order(input):
    if await inventory_service.is_overloaded():
        # Service is busy, try in 5 minutes
        raise RescheduleError(delay_seconds=300)

    return await inventory_service.reserve(input["items"])
```

### 3. Time-Based Processing

```python
@worker.task("market_order")
async def market_order(input):
    if not is_market_open():
        # Wait until market opens
        seconds_until_open = get_seconds_until_market_open()
        raise RescheduleError(delay_seconds=seconds_until_open)

    return await execute_trade(input)
```

### 4. Distributed Rate Limiting

```python
# Each worker checks a shared rate limit before processing
@worker.task("api_call")
async def api_call(input):
    # Check your own rate limiter (Redis, etc.)
    if not await rate_limiter.acquire("api_calls"):
        raise RescheduleError(delay_seconds=1)

    return await api.call(input)
```

### 5. Circuit Breaker Pattern

```python
@worker.task("external_call")
async def external_call(input):
    if circuit_breaker.is_open("external_service"):
        # Circuit is open, back off
        raise RescheduleError(delay_seconds=30)

    try:
        result = await external_service.call(input)
        circuit_breaker.record_success("external_service")
        return result
    except ServiceError as e:
        circuit_breaker.record_failure("external_service")
        raise RetryableError(str(e))
```

## Edge Cases

### delay_seconds = 0

Reschedule immediately (available_at = now). Task goes to back of queue.

```python
# Yield to other tasks, process this one later
raise RescheduleError(delay_seconds=0)
```

### Very Large Delays

No upper limit enforced. User can schedule far in future.

```python
# Reschedule for tomorrow
raise RescheduleError(delay_seconds=86400)
```

### Reschedule During Retry

Reschedules and retries are independent. A task can be:
- Rescheduled, then later fail and retry
- Retried, then later rescheduled

Both counters are tracked separately.

### Task Timeout During Reschedule Wait

If task times out while waiting (shouldn't happen since it's Pending,
not Running), monitor will handle normally.

## Implementation

### Task State Machine Update

```
                    ┌─────────────────────────────────┐
                    │                                 │
                    ▼                                 │
┌─────────┐     ┌─────────┐     ┌───────────┐     ┌──┴──────┐
│ Pending │────►│ Running │────►│ Completed │     │ Failed  │
└─────────┘     └────┬────┘     └───────────┘     └─────────┘
     ▲               │                                 ▲
     │               │ RescheduleError                 │
     │               │ (update available_at)           │
     │               ▼                                 │
     └───────────────┴─────────────────────────────────┘
                     │
                     │ max_reschedules exceeded
                     └─────────────────────────────────►
```

### Changes Required

1. **Task model**: Add `reschedule_count`, `max_reschedules` fields
2. **SubmitOptions**: Add `max_reschedules` parameter
3. **Worker**: Catch `RescheduleError`, update task, continue
4. **Python**: Add `RescheduleError` exception class
5. **CLI**: Add `--max-reschedules` flag to submit

### Estimated Effort

| Component | Lines |
|-----------|-------|
| Task model changes | ~20 |
| Reschedule transition | ~50 |
| Python exception + binding | ~30 |
| CLI flag | ~10 |
| Tests | ~100 |
| **Total** | ~210 |

## Metrics

Existing metrics work. Optional additions:

- `oq_tasks_rescheduled_total{task_type}` - Total reschedule count
- `oq_reschedule_delay_seconds{task_type}` - Histogram of delays

## Version History

Reschedules appear in task version history:

```bash
$ oq history <task_id>

Version 1: pending (created)
Version 2: running (claimed by worker-1)
Version 3: pending (rescheduled, available_at: +60s, count: 1)
Version 4: running (claimed by worker-2)
Version 5: pending (rescheduled, available_at: +120s, count: 2)
Version 6: running (claimed by worker-1)
Version 7: completed
```

## Comparison to Token Bucket

| Aspect | Token Bucket | Durable Sleep |
|--------|--------------|---------------|
| S3 operations | +2 per claim (read + CAS bucket) | 0 additional |
| Coordination | Workers compete for tokens | None |
| Accuracy | Predictive/approximate | Exact (real 429 response) |
| Complexity | High (buckets, refill, cache) | Trivial (one exception) |
| New infrastructure | Yes | No |
| Application control | Limited | Full |
| Rate limit source | Configured | Actual API response |

## FAQ

**Q: What if I want to limit rate even when API doesn't return 429?**

A: Implement in your handler:
```python
rate_limiter = RateLimiter(max_per_minute=100)

@worker.task("call_api")
async def call_api(input):
    if not rate_limiter.try_acquire():
        raise RescheduleError(delay_seconds=1)
    return await api.call(input)
```

**Q: Won't unlimited reschedules cause problems?**

A: The user controls when to reschedule. If they reschedule forever, that's
intentional (e.g., waiting for market open). Set `max_reschedules` if you
want a limit.

**Q: How is this different from retry with backoff?**

A: Retries are for failures. Reschedules are for "not right now, but later."
The handler explicitly decides when to retry, with full context from the
downstream system.

**Q: What about global rate limits across all workers?**

A: Use an external rate limiter (Redis, etc.) in your handler. oq stays
simple; coordination happens at application layer.
