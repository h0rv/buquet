"""
Research workflow example.

A simple AI-agent-style workflow that:
1. Validates the query
2. Searches for information (parallel: web + docs)
3. Synthesizes results

Run with:
    # Terminal 1: Start worker
    cd crates/buquet-workflow
    source ../buquet/examples/.env.example
    uv run python examples/research_workflow.py worker

    # Terminal 2: Submit a query
    uv run python examples/research_workflow.py run "What is buquet?"
"""

import asyncio
import random
import sys

import buquet
from buquet import Workflow, WorkflowClient, register_workflow


# Define the workflow
wf = Workflow("research")


@wf.step("validate")
async def validate(ctx):
    """Check the query is valid."""
    query = ctx.data.get("query", "")
    if len(query) < 3:
        raise ValueError("Query too short")
    print(f"[validate] Query OK: {query!r}")
    return {"query": query, "valid": True}


@wf.step("search_web", depends_on=["validate"])
async def search_web(ctx):
    """Search the web (simulated)."""
    query = ctx.data["query"]
    print(f"[search_web] Searching web for: {query!r}")
    await asyncio.sleep(random.uniform(0.5, 1.5))  # Simulate API call
    return {
        "source": "web",
        "results": [
            f"Web result 1 for '{query}'",
            f"Web result 2 for '{query}'",
        ],
    }


@wf.step("search_docs", depends_on=["validate"])
async def search_docs(ctx):
    """Search internal docs (simulated)."""
    query = ctx.data["query"]
    print(f"[search_docs] Searching docs for: {query!r}")
    await asyncio.sleep(random.uniform(0.5, 1.0))  # Simulate search
    return {
        "source": "docs",
        "results": [
            f"Doc result 1 for '{query}'",
        ],
    }


@wf.step("synthesize", depends_on=["search_web", "search_docs"])
async def synthesize(ctx):
    """Combine results into a summary."""
    print("[synthesize] Combining results...")
    await asyncio.sleep(0.5)  # Simulate LLM call
    return {
        "summary": f"Based on research about '{ctx.data['query']}': found relevant information from web and docs.",
        "sources_used": 2,
    }


async def run_worker():
    """Start the workflow worker."""
    queue = await buquet.connect()
    worker = buquet.Worker(queue, "research-worker", queue.all_shards())
    register_workflow(worker, wf, queue)

    print("[worker] Starting research workflow worker...")
    print(f"[worker] Registered: {list(wf.steps.keys())}")
    await worker.run()


async def run_workflow(query: str):
    """Submit and wait for a research workflow."""
    queue = await buquet.connect()
    client = WorkflowClient(queue)

    print(f"[client] Starting research workflow for: {query!r}")
    run = await client.start(wf, {"query": query})
    print(f"[client] Workflow ID: {run.id}")

    result = await client.wait(run.id)
    print(f"[client] Status: {result.status.value}")
    print(f"[client] Data: {result.data}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python research_workflow.py worker")
        print('  python research_workflow.py run "your query"')
        sys.exit(1)

    cmd = sys.argv[1]
    if cmd == "worker":
        asyncio.run(run_worker())
    elif cmd == "run":
        query = sys.argv[2] if len(sys.argv) > 2 else "What is buquet?"
        asyncio.run(run_workflow(query))
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
