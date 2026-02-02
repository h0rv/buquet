#!/usr/bin/env python3
"""
Example worker that processes orders.

Run with:
    cd crates/buquet
    uv run python examples/worker.py
"""
# ruff: noqa: T201 S311

from __future__ import annotations

import asyncio
import random

from buquet import RetryableError, Worker, WorkerRunOptions, connect

# Simulated product catalog
PRODUCTS: dict[str, dict[str, object]] = {
    "widget": {"name": "Widget", "price": 19.99},
    "gadget": {"name": "Gadget", "price": 5.99},
    "gizmo": {"name": "Gizmo", "price": 14.99},
    "doohickey": {"name": "Doohickey", "price": 29.99},
    "thingamajig": {"name": "Thingamajig", "price": 9.99},
}

# Constants for simulation
TRANSIENT_FAILURE_RATE = 0.1


async def main() -> None:
    print("[worker] Starting order processing worker...")

    # Connect to the queue
    queue = await connect()

    # Create a worker that handles all 16 shards
    shards = [f"{i:x}" for i in range(16)]  # ["0", "1", ..., "f"]
    worker = Worker(queue, "order-worker-1", shards)

    @worker.task("process_order")
    async def process_order(data: dict[str, object]) -> dict[str, object]:
        """Process an order by calculating totals and simulating work."""
        order_id = str(data["order_id"])
        items = list(data["items"])  # type: ignore[arg-type]

        print(f"[worker] Processing order {order_id}...")

        # Calculate line items
        line_items = []
        subtotal = 0.0

        for item in items:
            product_id = item["product_id"]
            quantity = item["quantity"]

            if product_id not in PRODUCTS:
                msg = f"Unknown product: {product_id}"
                raise ValueError(msg)

            product = PRODUCTS[product_id]
            price = float(product["price"])  # type: ignore[arg-type]
            name = str(product["name"])
            line_total = price * quantity
            subtotal += line_total

            line_items.append(
                {
                    "product_id": product_id,
                    "name": name,
                    "quantity": quantity,
                    "unit_price": price,
                    "total": round(line_total, 2),
                }
            )

            print(f"[worker]   - {quantity}x {name} @ ${price:.2f} = ${line_total:.2f}")

        # Simulate some processing time (0.2-0.8 seconds)
        process_time = random.uniform(0.2, 0.8)
        await asyncio.sleep(process_time)

        # Simulate occasional transient failures (10% chance)
        if random.random() < TRANSIENT_FAILURE_RATE:
            print(f"[worker] Transient error on {order_id}, will retry...")
            msg = "Payment gateway temporarily unavailable"
            raise RetryableError(msg)

        subtotal = round(subtotal, 2)
        print(f"[worker]   - Subtotal: ${subtotal:.2f}")
        print(f"[worker] Completed {order_id} in {process_time:.1f}s")

        return {
            "order_id": order_id,
            "line_items": line_items,
            "subtotal": subtotal,
            "status": "processed",
        }

    print(f"[worker] Registered handlers: {worker.registered_task_types()}")
    print("[worker] Polling for tasks...")

    # Run the worker (polls every 500ms)
    await worker.run(WorkerRunOptions(poll_interval_ms=500))


if __name__ == "__main__":
    asyncio.run(main())
