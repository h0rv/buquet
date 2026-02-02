#!/usr/bin/env python3
"""
Order Fulfillment Workflow - showcasing buquet-workflow features.

Demonstrates:
  - DAG with parallel steps
  - Signals (human approval)
  - Retries with backoff
  - Cancellation checking
  - Rich progress output

Workflow structure:
  ┌─────────────────────────────────────────────────────────┐
  │                    validate_order                        │
  │                          │                               │
  │              ┌───────────┴───────────┐                   │
  │              ▼                       ▼                   │
  │       check_inventory          charge_payment            │
  │              │                       │                   │
  │              └───────────┬───────────┘                   │
  │                          ▼                               │
  │                  wait_for_approval  ◄── signal: approve  │
  │                          │                               │
  │                          ▼                               │
  │                     ship_order                           │
  │                          │                               │
  │                          ▼                               │
  │                  send_notification                       │
  └─────────────────────────────────────────────────────────┘

Run:
    # Terminal 1: Worker
    uv run python examples/order_workflow.py worker

    # Terminal 2: Submit order
    uv run python examples/order_workflow.py submit

    # Terminal 3: Approve (after seeing "waiting for approval")
    uv run python examples/order_workflow.py approve <workflow-id>
"""

from __future__ import annotations

import asyncio
import random
import sys
from datetime import datetime
from typing import TYPE_CHECKING

import buquet
from buquet import Workflow, WorkflowClient, register_workflow

if TYPE_CHECKING:
    from buquet_workflow.workflow import StepContext

# ============================================================================
# Pretty printing
# ============================================================================

BLUE = "\033[94m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
CYAN = "\033[96m"
DIM = "\033[2m"
BOLD = "\033[1m"
RESET = "\033[0m"


def log(icon: str, step: str, msg: str, color: str = "") -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"{DIM}{ts}{RESET} {icon} {color}{BOLD}{step}{RESET} {msg}")


def log_tree() -> None:
    """Print the workflow DAG."""
    tree = f"""
{CYAN}┌─────────────────────────────────────────────────────────┐
│{RESET}                   {BOLD}Order Fulfillment{RESET}                      {CYAN}│
│{RESET}                                                           {CYAN}│
│{RESET}                    validate_order                         {CYAN}│
│{RESET}                          │                                {CYAN}│
│{RESET}              ┌───────────┴───────────┐                    {CYAN}│
│{RESET}              ▼                       ▼                    {CYAN}│
│{RESET}       check_inventory          charge_payment             {CYAN}│
│{RESET}              │                       │                    {CYAN}│
│{RESET}              └───────────┬───────────┘                    {CYAN}│
│{RESET}                          ▼                                {CYAN}│
│{RESET}                  {YELLOW}wait_for_approval{RESET}  ◄── {DIM}signal: approve{RESET}  {CYAN}│
│{RESET}                          │                                {CYAN}│
│{RESET}                          ▼                                {CYAN}│
│{RESET}                     ship_order                            {CYAN}│
│{RESET}                          │                                {CYAN}│
│{RESET}                          ▼                                {CYAN}│
│{RESET}                  send_notification                        {CYAN}│
└─────────────────────────────────────────────────────────┘{RESET}
"""
    print(tree)


# ============================================================================
# Workflow Definition
# ============================================================================

wf = Workflow("order_fulfillment")


@wf.step("validate_order")
async def validate_order(ctx: StepContext) -> dict:
    """Validate the order data."""
    order = ctx.data
    log("🔍", "validate", f"Checking order {order['order_id']}...", BLUE)
    await asyncio.sleep(0.5)

    if order.get("total", 0) <= 0:
        raise ValueError("Invalid order total")

    log("✓", "validate", f"Order valid: {len(order['items'])} items, ${order['total']:.2f}", GREEN)
    return {"validated": True, "order_id": order["order_id"]}


@wf.step("check_inventory", depends_on=["validate_order"])
async def check_inventory(ctx: StepContext) -> dict:
    """Check inventory for all items (runs parallel with payment)."""
    log("📦", "inventory", "Checking stock levels...", BLUE)

    for item in ctx.data["items"]:
        await asyncio.sleep(random.uniform(0.2, 0.5))
        log("  ", "inventory", f"  ✓ {item['name']}: {item['qty']} in stock", DIM)

    log("✓", "inventory", "All items available", GREEN)
    return {"in_stock": True}


@wf.step("charge_payment", depends_on=["validate_order"], retries=3)
async def charge_payment(ctx: StepContext) -> dict:
    """Process payment (runs parallel with inventory)."""
    log("💳", "payment", f"Charging ${ctx.data['total']:.2f}...", BLUE)
    await asyncio.sleep(random.uniform(0.5, 1.0))

    # Simulate occasional retry
    if random.random() < 0.2:
        log("⚠", "payment", "Payment gateway timeout, retrying...", YELLOW)
        raise Exception("Gateway timeout")  # Will be retried

    tx_id = f"tx_{random.randint(10000, 99999)}"
    log("✓", "payment", f"Payment successful: {tx_id}", GREEN)
    return {"transaction_id": tx_id, "charged": ctx.data["total"]}


@wf.step("wait_for_approval", depends_on=["check_inventory", "charge_payment"])
async def wait_for_approval(ctx: StepContext) -> dict:
    """Wait for human approval signal (for high-value orders)."""
    if ctx.data["total"] < 100:
        log("⏭", "approval", "Low-value order, auto-approved", DIM)
        return {"approved": True, "approver": "auto"}

    log("⏳", "approval", f"Waiting for approval signal...", YELLOW)
    log("  ", "approval", f"  Run: {BOLD}uv run python examples/order_workflow.py approve {ctx.workflow_id}{RESET}", DIM)

    # Wait for approval signal (up to 5 minutes for demo)
    signal = await ctx.wait_for_signal("approve", timeout_seconds=300)

    if signal is None:
        log("✗", "approval", "Approval timeout!", RED)
        raise TimeoutError("Approval not received in time")

    log("✓", "approval", f"Approved by {signal.get('approver', 'unknown')}", GREEN)
    return {"approved": True, "approver": signal.get("approver")}


@wf.step("ship_order", depends_on=["wait_for_approval"])
async def ship_order(ctx: StepContext) -> dict:
    """Ship the order."""
    log("🚚", "shipping", "Preparing shipment...", BLUE)
    await asyncio.sleep(random.uniform(0.5, 1.0))

    tracking = f"TRK{random.randint(100000, 999999)}"
    log("✓", "shipping", f"Shipped! Tracking: {tracking}", GREEN)
    return {"tracking_number": tracking, "shipped_at": datetime.now().isoformat()}


@wf.step("send_notification", depends_on=["ship_order"])
async def send_notification(ctx: StepContext) -> dict:
    """Send confirmation to customer."""
    log("📧", "notify", f"Sending confirmation to {ctx.data['customer_email']}...", BLUE)
    await asyncio.sleep(0.3)
    log("✓", "notify", "Customer notified", GREEN)
    return {"notified": True}


# ============================================================================
# Commands
# ============================================================================


async def run_worker() -> None:
    """Start the workflow worker."""
    print(f"\n{BOLD}buquet-workflow worker{RESET} - Order Fulfillment\n")
    log_tree()

    queue = await buquet.connect()
    worker = buquet.Worker(queue, "order-worker", queue.all_shards())
    register_workflow(worker, wf, queue)

    print(f"{DIM}Waiting for workflows...{RESET}\n")
    await worker.run()


async def submit_order() -> None:
    """Submit a new order workflow."""
    queue = await buquet.connect()
    client = WorkflowClient(queue)

    order = {
        "order_id": f"ORD-{random.randint(1000, 9999)}",
        "customer_email": "customer@example.com",
        "items": [
            {"name": "Widget Pro", "qty": 2, "price": 49.99},
            {"name": "Gadget Plus", "qty": 1, "price": 79.99},
        ],
        "total": 179.97,  # High value = needs approval
    }

    print(f"\n{BOLD}Submitting Order{RESET}")
    print(f"  Order ID: {order['order_id']}")
    print(f"  Items: {len(order['items'])}")
    print(f"  Total: ${order['total']:.2f}\n")

    run = await client.start(wf, order)
    print(f"{GREEN}✓{RESET} Workflow started: {BOLD}{run.id}{RESET}")
    print(f"\n{DIM}Watch the worker terminal for progress.{RESET}")
    print(f"{DIM}When prompted, approve with:{RESET}")
    print(f"  {BOLD}uv run python examples/order_workflow.py approve {run.id}{RESET}\n")


async def approve_order(workflow_id: str) -> None:
    """Send approval signal to a workflow."""
    queue = await buquet.connect()
    client = WorkflowClient(queue)

    print(f"\n{BOLD}Sending Approval Signal{RESET}")
    print(f"  Workflow: {workflow_id}\n")

    await client.signal(workflow_id, "approve", {"approver": "admin@example.com"})
    print(f"{GREEN}✓{RESET} Approval signal sent!\n")


async def watch_order(workflow_id: str) -> None:
    """Watch a workflow until completion."""
    queue = await buquet.connect()
    client = WorkflowClient(queue)

    print(f"\n{BOLD}Watching Workflow{RESET}: {workflow_id}\n")

    while True:
        run = await client.get(workflow_id)
        if run is None:
            print(f"{RED}Workflow not found{RESET}")
            return

        status_color = {
            "completed": GREEN,
            "failed": RED,
            "cancelled": YELLOW,
            "running": BLUE,
        }.get(run.status.value, "")

        print(f"  Status: {status_color}{run.status.value}{RESET}")
        print(f"  Current steps: {run.current_steps or 'none'}")

        if run.status.value in ("completed", "failed", "cancelled"):
            if run.error:
                print(f"  Error: {RED}{run.error}{RESET}")
            print()
            break

        await asyncio.sleep(1)


def main() -> None:
    if len(sys.argv) < 2:
        print(f"""
{BOLD}buquet-workflow Order Workflow Example{RESET}

Usage:
  {sys.argv[0]} worker              Start the worker
  {sys.argv[0]} submit              Submit a new order
  {sys.argv[0]} approve <wf-id>     Approve a pending order
  {sys.argv[0]} watch <wf-id>       Watch workflow progress
""")
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "worker":
        asyncio.run(run_worker())
    elif cmd == "submit":
        asyncio.run(submit_order())
    elif cmd == "approve" and len(sys.argv) > 2:
        asyncio.run(approve_order(sys.argv[2]))
    elif cmd == "watch" and len(sys.argv) > 2:
        asyncio.run(watch_order(sys.argv[2]))
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)


if __name__ == "__main__":
    main()
