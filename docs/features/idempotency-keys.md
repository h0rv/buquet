# Feature: Idempotency Keys

## Status: Implemented

## Goal
Prevent duplicate task creation when producers retry submissions, without adding
external systems or breaking S3-only simplicity.

**Important:** This provides *idempotent submit*, not exactly-once effects. It
guarantees the same idempotency key maps to the same task ID.

## Non-Goals
- Exactly-once task **execution** (still at-least-once)
- Deduping retries caused by worker crashes
- Cross-system transactions

## S3 Layout

```
bucket/
├── tasks/{shard}/{task_id}.json
└── idempotency/{scope}/{key_hash}.json
```

`idempotency/{scope}/{key_hash}.json`:

```json
{
  "key": "charge-ORD-123",
  "key_hash": "sha256:…",
  "task_id": "uuid",
  "task_key": "tasks/a/uuid.json",
  "created_at": "2026-01-27T12:00:00Z",
  "expires_at": "2026-02-26T12:00:00Z",
  "task_type": "charge_customer",
  "input_hash": "sha256:…"
}
```

## API

### Rust
```rust
queue.submit("charge_customer", input)
     .idempotency_key("charge-ORD-123")
     .idempotency_ttl(Duration::days(30));
```

### Python
```python
await queue.submit(
    "charge_customer",
    input,
    idempotency_key="charge-ORD-123",
    idempotency_ttl_days=30,
)
```

## Semantics

### On submit with idempotency key:

1. Compute `key_hash = sha256(scope + ":" + key)`.
2. **Attempt to create** `idempotency/{scope}/{key_hash}.json` using `If-None-Match: *`.
3. If create succeeds:
   - Create task object with `If-None-Match`.
   - Return new task.
4. If create fails (already exists):
   - Read existing idempotency record.
   - Return the referenced task (same task ID).

**Guarantee:** All submissions with the same `{scope, key}` return the same task.

### Scope
- Default scope = `task_type`
- Optional scope = `queue` (global)
- Optional custom string for advanced use

## Validation / Safety

To prevent accidental misuse:

- Store `input_hash` in the idempotency record.
- On reuse:
  - If `input_hash` differs, return a **IdempotencyConflict** error.
  - This avoids “same key, different payload” bugs.

## TTL / Retention

Idempotency records are not eternal.
Default: 30 days.

Retention controlled by:
- TTL in record (`expires_at`)
- S3 lifecycle policy on `idempotency/`

If record expired and still present, it is treated as stale and can be overwritten
with CAS.

## Failure Modes

- **Task creation fails after idempotency record created:**
  - Subsequent submits read the record, then fail to load task.
  - Behavior: either return error or recreate task if record marked “pending.”
  - Optional: two-phase status (`status: creating|ready`) in idempotency record.

## Costs

- 1 extra PUT on submit
- 1 extra GET on retry

Still cheaper than duplicate task creation + duplicate downstream effects.

## Why This Helps

- Safe retries for producers
- Prevents duplicate billing, emails, or jobs due to client retries
- Maintains S3-only simplicity
