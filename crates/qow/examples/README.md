# qow Examples

## Setup

```bash
docker compose up -d
```

Config is read from `.qo.toml`.

## Order Fulfillment (full-featured)

Shows DAG execution, parallel steps, retries, and **signals** for human approval.

```
                    validate_order
                          │
              ┌───────────┴───────────┐
              ▼                       ▼
       check_inventory          charge_payment
              │                       │
              └───────────┬───────────┘
                          ▼
                  wait_for_approval  ◄── signal
                          │
                          ▼
                     ship_order
                          │
                          ▼
                  send_notification
```

```bash
# Terminal 1: Worker
uv run python examples/order_workflow.py worker

# Terminal 2: Submit
uv run python examples/order_workflow.py submit

# Terminal 3: Approve (when prompted)
uv run python examples/order_workflow.py approve <workflow-id>
```

## Research Workflow (simple)

Parallel search steps with fan-in.

```
validate → search_web  ─┐
         → search_docs ─┴→ synthesize
```

```bash
uv run python examples/research_workflow.py worker
uv run python examples/research_workflow.py run "What is qo?"
```
