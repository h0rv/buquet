#!/usr/bin/env python3
"""
Demonstrates the S3 file structure and task lifecycle in qo.

This script shows exactly what objects are created/modified/deleted
at each stage of task processing.

Run with:
    cd crates/qo
    uv run python examples/s3_lifecycle_demo.py

Prerequisites:
    docker compose up -d garage garage-init
"""
# ruff: noqa: T201

from __future__ import annotations

import asyncio

from qo import Worker, connect


def print_header(text: str) -> None:
    width = 78
    print(f"╔{'═' * width}╗")
    print(f"║{text.center(width)}║")
    print(f"╚{'═' * width}╝")


def print_section(title: str) -> None:
    print(f"┌─ {title} {'─' * (60 - len(title))}┐")


def print_end() -> None:
    print("└" + "─" * 62 + "┘")


async def main() -> None:  # noqa: PLR0915
    queue = await connect()

    # =========================================================================
    print_header("QO S3 FILE STRUCTURE")
    print()
    print("""
qo-dev/                              # S3 Bucket
├── tasks/                           # Task data (JSON files)
│   ├── 0/                           # Shard 0 (UUIDs starting with 0)
│   │   └── 0abc1234-....json
│   ├── 1/                           # Shard 1
│   ├── ...                          # Shards 2-e
│   └── f/                           # Shard f
│       └── fb95-...-ccc.json        # Task JSON with full state
│
├── ready/                           # Ready index (empty marker files)
│   └── {shard}/{timestamp}/{id}     # Sorted by available_at for polling
│
├── leases/                          # Active leases (worker claims)
│   └── {shard}/{timeout}/{id}       # Sorted by expiry for timeout detection
│
└── history/                         # Version history (optional)
    └── {shard}/{id}/{version}.json  # Previous task states
""")

    print("=" * 78)
    print("TASK LIFECYCLE".center(78))
    print("=" * 78)
    print()

    # =========================================================================
    print_section("STEP 1: Submit Task")
    print("│")

    task = await queue.submit(
        "process_payment",
        {
            "order_id": "ORD-12345",
            "amount": 99.99,
            "currency": "USD",
        },
    )

    print(f"│  Task ID: {task.id}")
    print(f"│  Shard:   {task.shard} (first hex char of UUID)")
    print("│")
    print("│  S3 Objects Created:")
    print("│")
    print(f"│    tasks/{task.shard}/{task.id}.json")
    print("│      └── The actual task data (JSON)")
    print("│")
    print(f"│    ready/{task.shard}/{{timestamp}}/{task.id}")
    print("│      └── Empty marker file for polling (0 bytes)")
    print("│          Timestamp = available_at as zero-padded epoch")
    print("│")
    print("│  Task JSON:")
    print("│    {")
    print(f'│      "id": "{task.id}",')
    print('│      "status": "pending",')
    print(f'│      "input": {task.input},')
    print('│      "output": null,')
    print('│      "worker_id": null,')
    print('│      "lease_id": null,')
    print('│      "attempt": 0')
    print("│    }")
    print("│")
    print_end()
    print()

    # =========================================================================
    print_section("STEP 2: Worker Claims Task (Atomic via ETag)")
    print("│")
    print("│  Claim Process:")
    print("│    1. Worker lists ready/{shard}/ → finds task ID")
    print("│    2. Worker reads tasks/{shard}/{id}.json → gets ETag")
    print("│    3. Worker writes with If-Match={ETag}:")
    print("│       - Sets status='running'")
    print("│       - Sets worker_id, lease_id, lease_expires_at")
    print("│       - Increments attempt counter")
    print("│    4. If another worker already claimed (ETag mismatch):")
    print("│       → S3 returns 412 Precondition Failed")
    print("│       → Worker tries next task")
    print("│")
    print("│  S3 Changes:")
    print(f"│    tasks/{task.shard}/{task.id}.json  ← UPDATED")
    print("│    ready/.../{id}                        ← DELETED")
    print(f"│    leases/{task.shard}/{{exp}}/{{id}}      ← CREATED")
    print("│")

    # Actually process the task
    worker = Worker(queue, "demo-worker", [task.shard])

    @worker.task("process_payment")
    async def handle_payment(data: dict[str, object]) -> dict[str, object]:
        _ = data  # Unused in this demo
        return {"success": True, "transaction_id": "TXN-98765"}

    await worker.run(poll_interval_ms=50, max_tasks=1)

    # Fetch final state
    task = await queue.get(str(task.id))

    print("│  Task JSON (while running):")
    print("│    {")
    print('│      "status": "running",')
    print('│      "worker_id": "demo-worker",')
    print('│      "lease_id": "abc123...",  // Unique claim token')
    print('│      "lease_expires_at": "...",  // Auto-release time')
    print('│      "attempt": 1')
    print("│    }")
    print("│")
    print_end()
    print()

    # =========================================================================
    print_section("STEP 3: Task Completed")
    print("│")
    print("│  Completion Process:")
    print("│    1. Handler returns result")
    print("│    2. Worker writes task with If-Match={ETag}:")
    print("│       - Sets status='completed'")
    print("│       - Stores output")
    print("│       - Clears lease_id")
    print("│       - Sets completed_at")
    print("│    3. Worker deletes lease marker")
    print("│")
    print("│  S3 Changes:")
    print(f"│    tasks/{task.shard}/{task.id}.json  ← UPDATED (final)")
    print("│    leases/.../{id}                       ← DELETED")
    print("│")
    print("│  Final Task JSON:")
    print("│    {")
    print(f'│      "status": "{task.status}",')
    print(f'│      "output": {task.output},')
    print(f'│      "completed_at": "{task.completed_at[:19]}...",')
    print('│      "lease_id": null  // Cleared')
    print("│    }")
    print("│")
    print_end()
    print()

    # =========================================================================
    print_header("THE KEY INSIGHT: S3 AS A DATABASE")
    print()
    print("""
┌─ Why This Works ─────────────────────────────────────────────────────────┐
│                                                                          │
│  1. S3 ListObjects returns keys in LEXICOGRAPHIC ORDER                   │
│     → Encode timestamps in paths → get sorted results for free!          │
│                                                                          │
│  2. S3 supports CONDITIONAL WRITES via ETags                             │
│     → If-Match header enables Compare-And-Swap (CAS)                     │
│     → Only one writer succeeds when multiple compete                     │
│                                                                          │
│  3. Empty marker files as INDEXES                                        │
│     → 0 bytes, just the key name matters                                 │
│     → Sorted by timestamp prefix                                         │
│     → Workers: list ready/{shard}/ with limit=N                          │
│     → Monitor: list leases/{shard}/ to find expired                      │
│                                                                          │
│  4. SHARDING via UUID prefix (16 shards: 0-f)                            │
│     → Workers can partition work                                         │
│     → Parallel listing without hot spots                                 │
│     → Natural load balancing                                             │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘

┌─ No Other Dependencies ──────────────────────────────────────────────────┐
│                                                                          │
│  ✗ No Redis           (S3 handles queuing via ready/ index)              │
│  ✗ No Postgres        (S3 stores task state as JSON)                     │
│  ✗ No RabbitMQ        (S3 ListObjects = polling)                         │
│  ✗ No Zookeeper       (S3 ETags = distributed locks)                     │
│                                                                          │
│  Just S3. Works with AWS S3, MinIO, Garage, R2, or any S3-compatible.    │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
""")


if __name__ == "__main__":
    asyncio.run(main())
