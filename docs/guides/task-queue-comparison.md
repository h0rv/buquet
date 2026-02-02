# Task Queue and Workflow Engine Comparison

A comprehensive comparison of buquet against other task queue and workflow orchestration systems.

---

## Quick Comparison Matrix

| System | Stars | Language | Backend | License | Managed Cloud | SDKs |
|--------|------:|----------|---------|---------|:-------------:|------|
| [**buquet**](https://github.com/h0rv/buquet) | - | Rust | S3 only | MIT | No | Rust, Python |
| [**Apache Airflow**](https://github.com/apache/airflow) | ~44k | Python | PostgreSQL/MySQL + Redis/RabbitMQ | Apache-2.0 | Yes | Python, Go |
| [**Celery**](https://github.com/celery/celery) | ~28k | Python | RabbitMQ/Redis + Result Backend | BSD-3 | No | Python, Node, Go, Rust, PHP, Ruby |
| [**Kestra**](https://github.com/kestra-io/kestra) | ~26k | Java | PostgreSQL/MySQL/H2 + Kafka/Pulsar | Apache-2.0 | Yes | Any (YAML + plugins) |
| [**Prefect**](https://github.com/PrefectHQ/prefect) | ~21k | Python | PostgreSQL/SQLite | Apache-2.0 | Yes | Python |
| [**Temporal**](https://github.com/temporalio/temporal) | ~18k | Go | PostgreSQL/MySQL/Cassandra | MIT | Yes | Go, Java, Python, TS, .NET, Ruby, PHP |
| [**Argo Workflows**](https://github.com/argoproj/argo-workflows) | ~16k | Go | Kubernetes (etcd) | Apache-2.0 | Partial | Any (containers) |
| [**Windmill**](https://github.com/windmill-labs/windmill) | ~16k | Rust | PostgreSQL | AGPLv3 | Yes | Python, TypeScript, Go, Bash, SQL, PHP |
| [**Dagster**](https://github.com/dagster-io/dagster) | ~15k | Python | PostgreSQL/SQLite | Apache-2.0 | Yes | Python |
| [**Sidekiq**](https://github.com/sidekiq/sidekiq) | ~14k | Ruby | Redis | LGPL-3.0 | No | Ruby |
| [**Trigger.dev**](https://github.com/triggerdotdev/trigger.dev) | ~13k | TypeScript | PostgreSQL + Redis + ClickHouse + S3 | Apache-2.0 | Yes | TypeScript |
| [**Asynq**](https://github.com/hibiken/asynq) | ~13k | Go | Redis | MIT | No | Go |
| [**RQ**](https://github.com/rq/rq) | ~11k | Python | Redis | BSD-2 | No | Python |
| [**BullMQ**](https://github.com/taskforcesh/bullmq) | ~8k | TypeScript | Redis | MIT | No | Node, Python, Elixir, PHP |
| [**Hatchet**](https://github.com/hatchet-dev/hatchet) | ~6.5k | Go | PostgreSQL | MIT | Yes | Python, TypeScript, Go |
| [**Faktory**](https://github.com/contribsys/faktory) | ~6k | Go | Embedded RocksDB | AGPL | No | Ruby, Go, Python, Node, PHP, Elixir, Rust, etc. |
| [**Huey**](https://github.com/coleifer/huey) | ~6k | Python | Redis/SQLite/File/Memory | MIT | No | Python |
| [**Dramatiq**](https://github.com/Bogdanp/dramatiq) | ~5k | Python | RabbitMQ/Redis | LGPL-3.0 | No | Python |
| [**Inngest**](https://github.com/inngest/inngest) | ~5k | Go/TS | PostgreSQL/SQLite + Redis | SSPL | Yes | TypeScript, Python, Go |

---

## Feature Comparison Matrix

| System | Retries | Timeouts | Scheduling | Priorities | Rate Limiting | Workflows/DAGs | UI/Observability |
|--------|:-------:|:--------:|:----------:|:----------:|:-------------:|:--------------:|:----------------:|
| **buquet** | Yes | Yes | Yes | No | Yes (pattern) | Planned | Yes (dashboard) |
| **Apache Airflow** | Yes | Yes | Yes | Yes | Yes (pools) | Yes (native DAGs) | Yes (web UI) |
| **Argo Workflows** | Yes | Yes | Yes | Yes | Yes | Yes (native DAGs) | Yes (web UI) |
| **Celery** | Yes | Yes | Yes (Beat) | Yes | Yes | Yes (canvas) | Yes (Flower) |
| **Dagster** | Yes | Yes | Yes | Yes | Yes | Yes (native) | Yes (Dagit UI) |
| **Prefect** | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| **Temporal** | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| **Kestra** | Yes | Yes | Yes | Yes | Yes | Yes (native) | Yes (web UI) |
| **Windmill** | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| **Sidekiq** | Yes | Yes | Yes (Enterprise) | Yes | Yes | Partial | Yes |
| **Trigger.dev** | Yes | No (unlimited) | Yes | Yes | Partial | Yes | Yes |
| **Asynq** | Yes | Yes | Yes | Yes | Yes | No | Yes (Asynqmon) |
| **RQ** | Yes | Yes | Yes | Partial | No | Partial | Yes (rq-dashboard) |
| **BullMQ** | Yes | Yes | Yes | Yes | Yes | Yes | Yes (Taskforce.sh) |
| **Hatchet** | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| **Faktory** | Yes | Yes | Enterprise | Yes | Enterprise | Enterprise | Yes |
| **Huey** | Yes | No | Yes | Yes | No | Partial | No |
| **Dramatiq** | Yes | Yes | Yes | Yes | Yes | Partial | Partial |
| **Inngest** | Yes | Yes | Yes | Yes | Yes | Yes | Yes |

---

## Detailed Profiles

### buquet

| Field | Value |
|-------|-------|
| **GitHub URL** | https://github.com/h0rv/buquet |
| **Primary Language** | Rust |
| **License** | MIT |
| **Backend Requirements** | S3-compatible storage only (AWS S3, LocalStack, MinIO, etc.) |
| **Hosting Model** | Self-hosted only |
| **SDK Languages** | Rust, Python |
| **One-line Description** | S3-only task queue with no databases or message brokers required |

**Features:**
- Retries: Yes (configurable with exponential backoff)
- Timeouts: Yes (per-task, with automatic recovery)
- Scheduling: Yes (one-shot delays and recurring cron schedules)
- Priorities: No
- Rate Limiting: Yes (via RescheduleError pattern - durable sleep)
- Workflows/DAGs: Planned (buquet-workflow package)
- Observability/UI: Yes (web dashboard, `buquet tail` live stream)

**Additional Features:**
- Task cancellation (single, batch, by-type, cooperative for running tasks)
- Task expiration/TTL (tasks expire if not processed in time)
- Idempotency keys (exactly-once task submission)
- Lease extension (for long-running tasks)
- Payload references (large inputs/outputs stored separately)

**Key Differentiators:**
- Zero infrastructure beyond S3 - no databases, no message brokers
- Uses S3 conditional writes (ETags) for distributed coordination
- Polyglot - Rust and Python workers can process the same tasks
- Costs scale with usage (S3 pricing) rather than fixed infrastructure
- Fully inspectable - all state is JSON objects in S3
- Crash-safe by design - workers can crash at any point

---

### Temporal

| Field | Value |
|-------|-------|
| **GitHub URL** | https://github.com/temporalio/temporal |
| **GitHub Stars** | ~18,000 |
| **Primary Language** | Go |
| **License** | MIT |
| **Backend Requirements** | PostgreSQL, MySQL, Cassandra, or SQLite; Optional Elasticsearch |
| **Hosting Model** | Both (self-hosted + Temporal Cloud) |
| **SDK Languages** | Go, Java, Python, TypeScript, .NET, Ruby, PHP |
| **One-line Description** | Durable execution platform for building reliable, fault-tolerant distributed applications |

**Features:**
- Retries: Yes (automatic with configurable policies)
- Timeouts: Yes (start-to-close, schedule-to-start, schedule-to-close, heartbeat)
- Scheduling: Yes (cron, intervals, calendar-based)
- Priorities: Yes
- Rate Limiting: Yes
- Workflows/DAGs: Yes (native workflow orchestration, Saga patterns)
- Observability/UI: Yes (Web UI, Prometheus/Grafana)

---

### Hatchet

| Field | Value |
|-------|-------|
| **GitHub URL** | https://github.com/hatchet-dev/hatchet |
| **GitHub Stars** | ~6,500 |
| **Primary Language** | Go |
| **License** | MIT |
| **Backend Requirements** | PostgreSQL only |
| **Hosting Model** | Both (self-hosted + managed cloud) |
| **SDK Languages** | Python, TypeScript, Go |
| **One-line Description** | Distributed task queue and workflow orchestration platform built on PostgreSQL |

**Features:**
- Retries: Yes
- Timeouts: Yes
- Scheduling: Yes
- Priorities: Yes
- Rate Limiting: Yes (with dynamic keys)
- Workflows/DAGs: Yes
- Observability/UI: Yes (real-time web dashboard)

**Key Differentiators:**
- PostgreSQL-only backend (no Redis/RabbitMQ required)
- Task start times under 20ms
- Multi-tenant support with per-user rate limiting

---

### Prefect

| Field | Value |
|-------|-------|
| **GitHub URL** | https://github.com/PrefectHQ/prefect |
| **GitHub Stars** | ~21,400 |
| **Primary Language** | Python |
| **License** | Apache-2.0 |
| **Backend Requirements** | SQLite (dev) or PostgreSQL 14.9+ (production) |
| **Hosting Model** | Both (self-hosted + Prefect Cloud) |
| **SDK Languages** | Python only |
| **One-line Description** | Workflow orchestration framework for building resilient data pipelines in Python |

**Features:**
- Retries: Yes (`retries` and `retry_delay_seconds` parameters)
- Timeouts: Yes (`timeout_seconds` parameter)
- Scheduling: Yes (Cron, Interval, RRule)
- Priorities: Yes (work queue priorities)
- Rate Limiting: Yes (global concurrency limits)
- Workflows/DAGs: Yes (native Python control flow)
- Observability/UI: Yes (built-in dashboard)

---

### Celery

| Field | Value |
|-------|-------|
| **GitHub URL** | https://github.com/celery/celery |
| **GitHub Stars** | ~27,900 |
| **Primary Language** | Python |
| **License** | BSD-3-Clause |
| **Backend Requirements** | Message Broker: RabbitMQ, Redis, SQS, Kafka; Result Backend: Redis, PostgreSQL, MySQL, MongoDB |
| **Hosting Model** | Self-hosted only |
| **SDK Languages** | Python (native), Node.js, Go, Rust, PHP, Ruby (community) |
| **One-line Description** | Distributed task queue for real-time processing with support for scheduling |

**Features:**
- Retries: Yes (autoretry_for, max_retries, exponential backoff)
- Timeouts: Yes (soft and hard time limits)
- Scheduling: Yes (Celery Beat - intervals, crontab, solar)
- Priorities: Yes (RabbitMQ native, Redis emulated)
- Rate Limiting: Yes (per-worker, "/s", "/m", "/h" syntax)
- Workflows/DAGs: Yes (Chains, Groups, Chords, Map/Starmap)
- Observability/UI: Yes (Flower, Prometheus)

---

### BullMQ

| Field | Value |
|-------|-------|
| **GitHub URL** | https://github.com/taskforcesh/bullmq |
| **GitHub Stars** | ~8,300 |
| **Primary Language** | TypeScript |
| **License** | MIT |
| **Backend Requirements** | Redis |
| **Hosting Model** | Self-hosted only |
| **SDK Languages** | Node.js/TypeScript (primary), Python, Elixir, PHP |
| **One-line Description** | Fast, Redis-based message queue for Node.js with retries, scheduling, and workflows |

**Features:**
- Retries: Yes (automatic with exponential backoff)
- Timeouts: Yes (stalled job detection)
- Scheduling: Yes (delayed jobs, cron-based repeatable)
- Priorities: Yes
- Rate Limiting: Yes (global and per-group with Pro)
- Workflows/DAGs: Yes (parent-child dependencies, Flows)
- Observability/UI: Via Taskforce.sh (commercial) or Bull Board (community)

---

### RQ (Redis Queue)

| Field | Value |
|-------|-------|
| **GitHub URL** | https://github.com/rq/rq |
| **GitHub Stars** | ~10,600 |
| **Primary Language** | Python |
| **License** | BSD-2-Clause |
| **Backend Requirements** | Redis >= 5.0 or Valkey >= 7.2 |
| **Hosting Model** | Self-hosted only |
| **SDK Languages** | Python only |
| **One-line Description** | Simple job queues for Python |

**Features:**
- Retries: Yes (configurable count and intervals)
- Timeouts: Yes (per-job and per-queue)
- Scheduling: Yes (built-in since v2.5, rq-scheduler extension)
- Priorities: Partial (via multiple queues or `at_front=True`)
- Rate Limiting: No
- Workflows/DAGs: Partial (basic `depends_on`, no native DAG)
- Observability/UI: Yes (rq-dashboard, rqmonitor - external)

---

### Apache Airflow

| Field | Value |
|-------|-------|
| **GitHub URL** | https://github.com/apache/airflow |
| **GitHub Stars** | ~44,000 |
| **Primary Language** | Python |
| **License** | Apache-2.0 |
| **Backend Requirements** | PostgreSQL/MySQL + Redis/RabbitMQ (Celery) or Kubernetes |
| **Hosting Model** | Both (self-hosted + AWS MWAA, Google Cloud Composer, Astronomer) |
| **SDK Languages** | Python, Go (Airflow 3.0+) |
| **One-line Description** | A platform to programmatically author, schedule, and monitor workflows |

**Features:**
- Retries: Yes (configurable per task)
- Timeouts: Yes (execution_timeout, heartbeat timeout)
- Scheduling: Yes (cron, interval, data-aware, backfilling)
- Priorities: Yes (priority_weight with strategies)
- Rate Limiting: Yes (Pools for concurrent task limits)
- Workflows/DAGs: Yes (native DAG support)
- Observability/UI: Yes (Graph View, Grid View, SLA monitoring)

---

### Dramatiq

| Field | Value |
|-------|-------|
| **GitHub URL** | https://github.com/Bogdanp/dramatiq |
| **GitHub Stars** | ~5,100 |
| **Primary Language** | Python |
| **License** | LGPL-3.0 |
| **Backend Requirements** | RabbitMQ or Redis |
| **Hosting Model** | Self-hosted only |
| **SDK Languages** | Python 3.10+ |
| **One-line Description** | Fast and reliable background task processing library for Python |

**Features:**
- Retries: Yes (configurable backoff)
- Timeouts: Yes (message time limits)
- Scheduling: Yes (requires APScheduler for cron)
- Priorities: Yes
- Rate Limiting: Yes (built-in rate limiters)
- Workflows/DAGs: Partial (pipelines and groups, not full DAGs)
- Observability/UI: Partial (Prometheus middleware, no native UI)

---

### Inngest

| Field | Value |
|-------|-------|
| **GitHub URL** | https://github.com/inngest/inngest |
| **GitHub Stars** | ~4,700 |
| **Primary Language** | Go, TypeScript |
| **License** | SSPL + Apache 2.0 (SDKs) |
| **Backend Requirements** | PostgreSQL/SQLite + Redis (self-hosted); managed handles infrastructure |
| **Hosting Model** | Both (managed cloud primary + self-hosted) |
| **SDK Languages** | TypeScript, Python, Go |
| **One-line Description** | Workflow orchestration platform for stateful step functions and AI workflows |

**Features:**
- Retries: Yes (0-20 per step, default 3)
- Timeouts: Yes (up to 2 hours per step)
- Scheduling: Yes (cron, delayed functions)
- Priorities: Yes (expression-based, -600 to 600)
- Rate Limiting: Yes (skip events beyond limit)
- Workflows/DAGs: Yes (step functions with sequential/conditional steps)
- Observability/UI: Yes (built-in dashboard)

---

### Trigger.dev

| Field | Value |
|-------|-------|
| **GitHub URL** | https://github.com/triggerdotdev/trigger.dev |
| **GitHub Stars** | ~13,400 |
| **Primary Language** | TypeScript |
| **License** | Apache-2.0 |
| **Backend Requirements** | PostgreSQL, Redis, ClickHouse, S3-compatible storage |
| **Hosting Model** | Both (managed cloud + self-hosted) |
| **SDK Languages** | TypeScript only |
| **One-line Description** | Open-source platform for AI workflows with long-running tasks and elastic scaling |

**Features:**
- Retries: Yes (automatic with exponential backoff)
- Timeouts: No (unlimited execution duration)
- Scheduling: Yes (durable cron schedules)
- Priorities: Yes (relative priority with anti-starvation)
- Rate Limiting: Partial (concurrency limits, not requests/minute)
- Workflows/DAGs: Yes (code-first task chaining)
- Observability/UI: Yes (real-time dashboard, tracing, alerts)

---

### Asynq

| Field | Value |
|-------|-------|
| **GitHub URL** | https://github.com/hibiken/asynq |
| **GitHub Stars** | ~12,800 |
| **Primary Language** | Go |
| **License** | MIT |
| **Backend Requirements** | Redis 4.0+ |
| **Hosting Model** | Self-hosted only |
| **SDK Languages** | Go only |
| **One-line Description** | Simple, reliable, and efficient distributed task queue in Go backed by Redis |

**Features:**
- Retries: Yes (automatic with exponential backoff)
- Timeouts: Yes (per-task timeout and deadline)
- Scheduling: Yes (cron, delayed execution)
- Priorities: Yes (weighted and strict priority queues)
- Rate Limiting: Yes (distributed semaphore-based)
- Workflows/DAGs: No
- Observability/UI: Yes (Asynqmon web UI, Prometheus, OpenTelemetry)

---

### Faktory

| Field | Value |
|-------|-------|
| **GitHub URL** | https://github.com/contribsys/faktory |
| **GitHub Stars** | ~6,000 |
| **Primary Language** | Go |
| **License** | AGPL (OSS) / Commercial |
| **Backend Requirements** | None (embedded RocksDB) |
| **Hosting Model** | Self-hosted only |
| **SDK Languages** | Ruby, Go, Python, Node, PHP, Elixir, Rust, Swift, Java, .NET, etc. |
| **One-line Description** | Language-agnostic persistent background job server |

**Features:**
- Retries: Yes (exponential backoff)
- Timeouts: Yes (job reservation timeout)
- Scheduling: Enterprise only (cron jobs)
- Priorities: Yes (queue priorities)
- Rate Limiting: Enterprise only (queue throttling)
- Workflows/DAGs: Enterprise only (batches/workflows)
- Observability/UI: Yes (built-in web UI)

**Note:** Created by Mike Perham (creator of Sidekiq). Many features require Enterprise license ($199+/mo).

---

### Huey

| Field | Value |
|-------|-------|
| **GitHub URL** | https://github.com/coleifer/huey |
| **GitHub Stars** | ~5,900 |
| **Primary Language** | Python |
| **License** | MIT |
| **Backend Requirements** | Redis, SQLite, file-system, or in-memory |
| **Hosting Model** | Self-hosted only |
| **SDK Languages** | Python only |
| **One-line Description** | A little task queue for Python |

**Features:**
- Retries: Yes (configurable with delay)
- Timeouts: No
- Scheduling: Yes (delayed execution, crontab)
- Priorities: Yes
- Rate Limiting: No
- Workflows/DAGs: Partial (linear pipelines via `.then()`)
- Observability/UI: No (signals and logging only)

---

### Sidekiq

| Field | Value |
|-------|-------|
| **GitHub URL** | https://github.com/sidekiq/sidekiq |
| **GitHub Stars** | ~13,500 |
| **Primary Language** | Ruby |
| **License** | LGPL-3.0 (OSS) / Commercial |
| **Backend Requirements** | Redis 6.2+ |
| **Hosting Model** | Self-hosted only |
| **SDK Languages** | Ruby only |
| **One-line Description** | Simple, efficient background processing for Ruby |

**Features:**
- Retries: Yes (automatic exponential backoff with jitter, 25 attempts over 21 days)
- Timeouts: Yes (job timeout configuration)
- Scheduling: Enterprise/Pro (cron-like recurring jobs)
- Priorities: Yes (weighted queues)
- Rate Limiting: Yes (Enterprise: window, bucket, concurrent limiters)
- Workflows/DAGs: Partial (Pro: Batches with callbacks, not full DAGs)
- Observability/UI: Yes (built-in web UI with real-time metrics)

**Note:** Created by Mike Perham. Enterprise features ($199+/mo) include periodic jobs, rate limiting, unique jobs, and batches.

---

### Argo Workflows

| Field | Value |
|-------|-------|
| **GitHub URL** | https://github.com/argoproj/argo-workflows |
| **GitHub Stars** | ~16,400 |
| **Primary Language** | Go |
| **License** | Apache-2.0 |
| **Backend Requirements** | Kubernetes cluster (uses etcd) |
| **Hosting Model** | Self-hosted (any K8s), managed options via cloud providers |
| **SDK Languages** | Any (container-based execution) |
| **One-line Description** | Container-native workflow engine for Kubernetes |

**Features:**
- Retries: Yes (per-step retry strategies with limits and backoff)
- Timeouts: Yes (activeDeadlineSeconds for workflows and templates)
- Scheduling: Yes (CronWorkflow for recurring execution)
- Priorities: Yes (pod priority classes)
- Rate Limiting: Yes (parallelism limits at workflow and template level)
- Workflows/DAGs: Yes (native DAG support, steps, and complex dependencies)
- Observability/UI: Yes (web UI with workflow visualization, Prometheus metrics)

**Key Differentiators:**
- Workflows defined as Kubernetes CRDs (YAML)
- Each step runs as a container/pod
- Artifact passing between steps via S3/GCS/MinIO
- Part of CNCF (graduated project)

---

### Windmill

| Field | Value |
|-------|-------|
| **GitHub URL** | https://github.com/windmill-labs/windmill |
| **GitHub Stars** | ~15,600 |
| **Primary Language** | Rust |
| **License** | AGPLv3 (Enterprise features commercial) |
| **Backend Requirements** | PostgreSQL |
| **Hosting Model** | Both (self-hosted + Windmill Cloud) |
| **SDK Languages** | Python, TypeScript, Go, Bash, SQL, PHP, Rust |
| **One-line Description** | Open-source developer platform for scripts, workflows, and UIs |

**Features:**
- Retries: Yes (configurable per step)
- Timeouts: Yes (per-job timeouts)
- Scheduling: Yes (cron, interval-based)
- Priorities: Yes (job priority queues)
- Rate Limiting: Yes (concurrency limits)
- Workflows/DAGs: Yes (visual DAG builder + code-based)
- Observability/UI: Yes (full IDE-like web UI with logs, runs, and debugging)

**Key Differentiators:**
- Built-in web IDE for script development
- Auto-generated UIs from script parameters
- Approval steps for human-in-the-loop workflows
- Native support for scripts as HTTP endpoints

---

### Kestra

| Field | Value |
|-------|-------|
| **GitHub URL** | https://github.com/kestra-io/kestra |
| **GitHub Stars** | ~26,300 |
| **Primary Language** | Java |
| **License** | Apache-2.0 |
| **Backend Requirements** | PostgreSQL/MySQL/H2 + optional Kafka/Pulsar for HA |
| **Hosting Model** | Both (self-hosted + Kestra Cloud) |
| **SDK Languages** | Any (YAML + 600+ plugins) |
| **One-line Description** | Infinitely scalable orchestration platform for all kinds of workflows |

**Features:**
- Retries: Yes (configurable retry policies)
- Timeouts: Yes (per-task timeouts)
- Scheduling: Yes (cron, interval, event-driven triggers)
- Priorities: Yes (task priorities)
- Rate Limiting: Yes (concurrency limits)
- Workflows/DAGs: Yes (native YAML-based DAGs)
- Observability/UI: Yes (rich web UI with live topology view, Gantt charts)

**Key Differentiators:**
- Declarative YAML workflow definitions
- Event-driven architecture with 600+ integrations
- Language-agnostic (Python, Node, Shell, SQL, R via Script tasks)
- Built-in secret management and namespace isolation

---

### Dagster

| Field | Value |
|-------|-------|
| **GitHub URL** | https://github.com/dagster-io/dagster |
| **GitHub Stars** | ~14,800 |
| **Primary Language** | Python |
| **License** | Apache-2.0 |
| **Backend Requirements** | PostgreSQL (production) or SQLite (dev) |
| **Hosting Model** | Both (self-hosted + Dagster Cloud) |
| **SDK Languages** | Python only |
| **One-line Description** | Cloud-native orchestration platform for data pipelines with software-defined assets |

**Features:**
- Retries: Yes (configurable per-op retry policies)
- Timeouts: Yes (op timeout configuration)
- Scheduling: Yes (cron, sensors, partitioned schedules)
- Priorities: Yes (run priority configuration)
- Rate Limiting: Yes (concurrency limits on ops and runs)
- Workflows/DAGs: Yes (native graph-based pipelines)
- Observability/UI: Yes (Dagit web UI with asset lineage, run history)

**Key Differentiators:**
- Software-defined assets (data-centric, not task-centric)
- Built-in data quality checks (asset checks)
- Automatic data lineage tracking
- Partitioned data pipelines with backfill support
- Strong typing with IO managers

---

## Choosing the Right System

### Use buquet when:
- You want minimal infrastructure (just S3, no databases)
- You need polyglot support (Rust + Python workers)
- Your workload tolerates 1-5 second dispatch latency
- You want costs to scale with actual usage (S3 pricing)
- Debuggability matters (all state is inspectable JSON)
- You need scheduling, cancellation, or TTL without infrastructure
- You're building batch jobs, async processing, or background tasks

### Use Temporal when:
- You need complex, long-running workflows
- You require strong durability guarantees
- You have a multi-language team
- Enterprise support is important

### Use Celery when:
- You're building in Python
- You need mature, battle-tested software
- You want extensive community support
- You need flexible broker options

### Use Apache Airflow when:
- You're building data pipelines
- You need complex DAG scheduling
- You want managed cloud options
- You need strong observability

### Use BullMQ/Asynq when:
- You're building in Node.js/Go respectively
- You already have Redis infrastructure
- You need high-performance task processing
- Simple setup is important

### Use Hatchet when:
- You want PostgreSQL-only infrastructure
- You need low-latency task starts
- Multi-tenant rate limiting is important

### Use Sidekiq when:
- You're building in Ruby/Rails
- You need battle-tested, high-performance job processing
- You already have Redis infrastructure
- You want excellent documentation and community

### Use Argo Workflows when:
- You're running on Kubernetes
- You need container-based task execution
- You want GitOps workflow definitions
- You need complex DAG orchestration at scale

### Use Windmill when:
- You need a low-code/no-code workflow builder
- You want auto-generated UIs from scripts
- You need approval steps for human-in-the-loop
- Multi-language support is important

### Use Kestra when:
- You want declarative YAML workflows
- You need 600+ pre-built integrations
- You want event-driven orchestration
- Language-agnostic execution is important

### Use Dagster when:
- You're building data pipelines
- You want asset-centric orchestration
- Data lineage tracking is important
- You need partitioned backfills

---

## Infrastructure Requirements Summary

| System | Database | Message Broker | Other |
|--------|----------|----------------|-------|
| **buquet** | None | None | S3 |
| **Temporal** | PostgreSQL/MySQL/Cassandra | None | Optional: Elasticsearch |
| **Hatchet** | PostgreSQL | None | None |
| **Prefect** | PostgreSQL/SQLite | None | None |
| **Dagster** | PostgreSQL/SQLite | None | None |
| **Celery** | Optional (for results) | RabbitMQ/Redis/SQS | None |
| **Sidekiq** | None | Redis | None |
| **BullMQ** | None | Redis | None |
| **RQ** | None | Redis | None |
| **Asynq** | None | Redis | None |
| **Airflow** | PostgreSQL/MySQL | Redis/RabbitMQ (for Celery executor) | None |
| **Argo Workflows** | None (Kubernetes etcd) | None | Kubernetes cluster |
| **Windmill** | PostgreSQL | None | None |
| **Kestra** | PostgreSQL/MySQL/H2 | Optional: Kafka/Pulsar (HA) | None |
| **Faktory** | Embedded RocksDB | None | None |
| **Huey** | Optional (SQLite) | Redis (optional) | None |
| **Dramatiq** | None | RabbitMQ/Redis | None |
| **Inngest** | PostgreSQL/SQLite | Redis | None |
| **Trigger.dev** | PostgreSQL | Redis | ClickHouse, S3 |

---

*Last updated: January 2025*
