# Future Considerations

> **Status:** Future (requires more design work)

These features require more design work but are worth tracking for future development.

---

## Idempotency Keys

**Status: Implemented** - See [idempotency-keys.md](idempotency-keys.md)

- Prevents duplicate task creation with `idempotency_key` parameter
- Supports `idempotency_scope` (task_type, queue, custom)
- Configurable TTL via `idempotency_ttl_days`
- Input validation to detect conflicts

---

## Scheduled Tasks

**Status: Implemented** - See [scheduling.md](scheduling.md)

- One-shot scheduling via `schedule_at` parameter
- Recurring schedules via `qo schedule` CLI and cron expressions
- Scheduler daemon via `qo schedule run`

---

## Task Dependencies

Create tasks that wait for other tasks to complete first.

### Design

- `depends_on: Vec<TaskId>` field
- Task stays pending until all dependencies complete
- Enables DAG (directed acyclic graph) workflows

### API

```python
# Create parent tasks
task_a = await queue.submit("extract_data", {"source": "db1"})
task_b = await queue.submit("extract_data", {"source": "db2"})

# Create task that waits for both
task_c = await queue.submit(
    "merge_data",
    {"sources": ["db1", "db2"]},
    depends_on=[task_a.id, task_b.id]
)

# task_c won't run until task_a and task_b complete
```

### Considerations

- How to handle dependency failures?
  - Option 1: Fail dependent task
  - Option 2: Skip dependent task
  - Option 3: Let handler decide
- Cycle detection (prevent A → B → A)
- Performance of dependency checking
- May need separate "blocked" status

---

## Rate Limiting

**Status: Proposed** - See [rate-limiting.md](rate-limiting.md) for full spec.

- Token bucket algorithm with S3 CAS
- Per-task-type, per-key, and global scopes
- Worker backpressure behavior options

---

## Task Priorities

**Status: Proposed** - See [priorities.md](priorities.md) for full spec.

- 0-255 priority range with inverted ready index
- Optional priority aging for anti-starvation
- Phased implementation plan

---

## Implementation Order

If these become priorities:

1. ~~**Scheduled Tasks**~~ - ✅ Implemented
2. ~~**Idempotency Keys**~~ - ✅ Implemented
3. **Task Priorities** - Common request, moderate complexity
4. **Task Dependencies** - Enables workflows, higher complexity
5. **Rate Limiting** - Complex, but important for API integrations
