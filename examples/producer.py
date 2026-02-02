#!/usr/bin/env python3
"""
Example producer that submits orders and monitors their status.

Run with:
    cd crates/buquet
    uv run python examples/producer.py
"""
# ruff: noqa: T201

from __future__ import annotations

import asyncio
from typing import Any

from buquet import TaskStatus, connect

# Sample orders to submit
SAMPLE_ORDERS: list[dict[str, Any]] = [
    {
        "order_id": "order-1",
        "items": [
            {"product_id": "widget", "quantity": 2},
            {"product_id": "gadget", "quantity": 1},
        ],
    },
    {
        "order_id": "order-2",
        "items": [
            {"product_id": "gizmo", "quantity": 3},
        ],
    },
    {
        "order_id": "order-3",
        "items": [
            {"product_id": "doohickey", "quantity": 1},
            {"product_id": "thingamajig", "quantity": 2},
        ],
    },
    {
        "order_id": "order-4",
        "items": [
            {"product_id": "widget", "quantity": 5},
            {"product_id": "gizmo", "quantity": 2},
            {"product_id": "gadget", "quantity": 4},
        ],
    },
    {
        "order_id": "order-5",
        "items": [
            {"product_id": "thingamajig", "quantity": 10},
        ],
    },
]

# Prices for display (matches worker.py)
PRICES: dict[str, float] = {
    "widget": 19.99,
    "gadget": 5.99,
    "gizmo": 14.99,
    "doohickey": 29.99,
    "thingamajig": 9.99,
}


def calculate_expected_total(items: list[dict[str, Any]]) -> float:
    """Calculate expected total for an order."""
    total = sum(PRICES[item["product_id"]] * item["quantity"] for item in items)
    return round(total, 2)


def format_order_summary(order: dict[str, Any]) -> str:
    """Format a brief order summary."""
    parts = [f"{item['quantity']}x {item['product_id'].title()}" for item in order["items"]]
    expected = calculate_expected_total(order["items"])
    return f"{', '.join(parts)} = ${expected:.2f}"


async def main() -> None:
    print("[producer] Submitting orders...")

    # Connect to the queue
    queue = await connect()

    # Submit all orders
    task_ids: dict[str, object] = {}
    print(f"[producer] Submitting {len(SAMPLE_ORDERS)} orders...")

    for order in SAMPLE_ORDERS:
        task = await queue.submit("process_order", order)
        task_ids[order["order_id"]] = task.id
        print(f"[producer] Submitted {order['order_id']}: {format_order_summary(order)}")

    print()
    print("[producer] Waiting for tasks to complete...")
    print()

    # Poll for completion
    pending = set(task_ids.keys())
    while pending:
        await asyncio.sleep(0.5)

        for order_id in list(pending):
            task_id = task_ids[order_id]
            task = await queue.get(task_id)

            if task is None:
                print(f"[producer] {order_id}: task not found!")
                pending.remove(order_id)
                continue

            if task.status == TaskStatus.Completed:
                output = task.output
                total = output.get("subtotal", "?")
                print(f"[producer] {order_id}: completed - Total: ${total}")
                pending.remove(order_id)

            elif task.status == TaskStatus.Failed:
                print(f"[producer] {order_id}: failed - {task.last_error}")
                pending.remove(order_id)

            elif task.status == TaskStatus.Running:
                # Still processing
                pass

    print()
    print("[producer] All orders processed!")


if __name__ == "__main__":
    asyncio.run(main())
