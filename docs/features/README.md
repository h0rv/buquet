# oq Feature Roadmap

Features that would elevate oq from a solid task queue to an exceptional one.

## Current State: 8.5/10

| Category | Score | Notes |
|----------|-------|-------|
| Core Functionality | 9/10 | Submit, claim, complete, retry, timeout recovery, scheduling |
| API Completeness | 9/10 | Idempotency, lease extension, payload refs, scheduling |
| CLI | 9/10 | Strong coverage; major gaps addressed |
| Python Bindings | 9/10 | Full parity with Rust via PyO3 |
| Documentation | 8/10 | Architecture clear; needs troubleshooting guide |
| Testing | 8/10 | Good integration tests + blind tests; no load/chaos tests |
| Observability | 6/10 | Logging + dashboard + metrics; no tracing |
| Configuration | 8/10 | Config files + env vars + profiles |

## Feature Status

### Tier 0: Adoption Blockers (Correctness + Scale)

| Feature | Status | File |
|---------|--------|------|
| Idempotency Keys / Outbox Helpers | **COMPLETED** | [idempotency-keys.md](./idempotency-keys.md) |
| Task Lease Extension (Long-Running Tasks) | **COMPLETED** | [task-lease-extension.md](./task-lease-extension.md) |
| Payload References (Large Inputs/Outputs) | **COMPLETED** | [payload-references.md](./payload-references.md) |
| Version History Retention Policy | **COMPLETED** | [version-history-retention.md](./version-history-retention.md) |
| Shard Leasing (Discovery Cost Reduction) | **COMPLETED** | [shard-leases.md](./shard-leases.md) |
| Rate Limiting / Backpressure | **COMPLETED** | [rate-limiting.md](./rate-limiting.md) |

### Tier 1: Quick Wins (< 1 hour each)

| Feature | Status | File |
|---------|--------|------|
| Shell Completions | **COMPLETED** | [shell-completions.md](./shell-completions.md) |
| `oq doctor` Command | **COMPLETED** | [doctor-command.md](./doctor-command.md) |
| Filter `oq list` by Task Type | **COMPLETED** | [list-task-type-filter.md](./list-task-type-filter.md) |

### Tier 2: Production Essentials (2-4 hours each)

| Feature | Status | File |
|---------|--------|------|
| Metrics (facade) | **COMPLETED** | [metrics.md](./metrics.md) |
| Python Worker Lifecycle Hooks | **COMPLETED** | [worker-lifecycle-hooks.md](./worker-lifecycle-hooks.md) |
| Bulk Task Submission | **COMPLETED** | [bulk-submission.md](./bulk-submission.md) |
| Configurable Shards | **COMPLETED** | [configurable-shards.md](./configurable-shards.md) |
| Adaptive Polling | **COMPLETED** | [adaptive-polling.md](./adaptive-polling.md) |
| Task Schemas | **COMPLETED** | [task-schemas.md](./task-schemas.md) |
| Load/Chaos Test Harness | Planned | [ISSUE-005-load-and-chaos-testing.md](../issues/ISSUE-005-load-and-chaos-testing.md) |

### Tier 3: Polish Features (4-8 hours each)

| Feature | Status | File |
|---------|--------|------|
| `oq tail` - Live Task Stream | **COMPLETED** | [tail-command.md](./tail-command.md) |
| Richer Error Messages | Planned | [richer-errors.md](./richer-errors.md) |
| Configuration File Support | **COMPLETED** | [config-files.md](./config-files.md) |
| OpenTelemetry Tracing | Planned | [opentelemetry.md](./opentelemetry.md) |

### Tier 4: Simple Workflow Primitives (2-4 hours each)

| Feature | Status | File |
|---------|--------|------|
| Task Cancellation | **COMPLETED** | [task-cancellation.md](./task-cancellation.md) |
| Task Tagging / Metadata | Proposed | [task-tagging.md](./task-tagging.md) |
| Task Expiration / TTL | **COMPLETED** | [task-expiration.md](./task-expiration.md) |
| Progress Reporting | Proposed | [progress-reporting.md](./progress-reporting.md) |
| Global Pause / Resume | Proposed | [global-pause.md](./global-pause.md) |
| Result Chaining / Fan-Out | Proposed | [result-chaining.md](./result-chaining.md) |

### Future Considerations

| Feature | Status | File |
|---------|--------|------|
| Scheduling (One-Shot + Recurring) | **COMPLETED** | [scheduling.md](./scheduling.md) |
| Task Priorities | Planned | [priorities.md](./priorities.md) |
| Task Dependencies | Future | [future.md](./future.md) |

## Design Philosophy for New Features

New features follow the same pattern as Rate Limiting (Durable Sleep):

| Principle | Application |
|-----------|-------------|
| **Reuse existing primitives** | Use `available_at`, task status, task fields - not new infrastructure |
| **S3 as only state store** | Control objects, not external services |
| **User controls complexity** | Opt-in features, patterns over frameworks |
| **Simple > Automatic** | Explicit operations, not magic |

Example: Rate limiting reuses `available_at` via `RescheduleError`. No token buckets, no coordination.

## Implementation Priority

1. ~~Shard leasing (discovery cost reduction)~~ - **DONE**
2. ~~Task lease extension (long-running tasks)~~ - **DONE**
3. ~~Payload references (large inputs/outputs)~~ - **DONE**
4. ~~Idempotency keys / outbox helpers~~ - **DONE**
5. ~~Monitor/sweeper scan efficiency - [BUG-004](../bugs/BUG-004-monitor-sweeper-full-scans.md)~~ - **DONE**
6. ~~Version history retention policy~~ - **DONE**
7. Load/chaos test harness - [ISSUE-005-load-and-chaos-testing.md](../issues/ISSUE-005-load-and-chaos-testing.md)
8. ~~Task schemas - contract enforcement, format-agnostic~~ - **DONE**
9. Richer errors - spread over time
10. ~~Config files~~ - **DONE**
11. OpenTelemetry - for complex deployments
12. ~~Shell completions~~ - **DONE**
13. ~~`oq doctor`~~ - **DONE**
14. ~~Task type filter~~ - **DONE**
15. ~~Metrics~~ - **DONE** (uses `metrics` crate facade)
16. ~~Worker lifecycle hooks~~ - **DONE**
17. ~~Bulk submission~~ - **DONE**
18. ~~`oq tail`~~ - **DONE**
19. ~~Task cancellation~~ - **DONE**
20. ~~Task expiration/TTL~~ - **DONE**
