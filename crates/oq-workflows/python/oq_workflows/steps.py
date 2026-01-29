"""Step handler wrapper for workflow steps."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ._rust import get_engine
from .workflow import StepContext, Workflow

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    from oq import Queue, TaskContext


async def handle_step(
    input_data: dict[str, Any],
    ctx: TaskContext,
    workflow: Workflow,
    queue: Queue,
) -> dict[str, Any]:
    """
    Handle a workflow step execution.

    This function:
    1. Executes the user's step handler
    2. Uses Rust engine to update state and persist results
    3. Triggers the orchestrator to continue

    Args:
        input_data: Input from step task (contains workflow_id, step name, data)
        ctx: Task context for lease extension
        workflow: The workflow definition
        queue: The oq queue

    Returns:
        Result dict with step outcome
    """
    wf_id = input_data["workflow_id"]
    step_name = input_data["step"]
    step_data = input_data.get("data", {})

    step_def = workflow.get_step(step_name)
    if step_def is None:
        error_msg = f"Unknown step: {step_name}"
        return {"error": error_msg}

    # Create step context
    step_ctx = StepContext(
        workflow_id=wf_id,
        step=step_name,
        data=step_data,
        queue=queue,
        task_context=ctx,
    )

    # Execute user's step handler
    result = await step_def.handler(step_ctx)

    # Use engine to complete step (updates state + saves result to S3)
    engine = await get_engine()
    now = await queue.now()
    await engine.complete_step(wf_id, step_name, result, now)

    # Trigger orchestrator to continue
    await queue.submit(
        "workflow.orchestrate",
        {"workflow_id": wf_id},
        idempotency_key=f"{wf_id}:orchestrate:{now}",
    )

    return {"success": True, "result": result, "step": step_name}


async def handle_step_failure(
    wf_id: str,
    step_name: str,
    error: str,
    queue: Queue,
) -> None:
    """
    Handle a step failure after retries are exhausted.

    This should be called by the orchestrator when it detects that a step's
    task has failed (after retries).

    Args:
        wf_id: Workflow ID
        step_name: Name of the failed step
        error: Error message
        queue: The oq queue
    """
    engine = await get_engine()
    now = await queue.now()
    await engine.fail_step(wf_id, step_name, error, now)


async def handle_step_cancellation(
    wf_id: str,
    step_name: str,
    reason: str,
    queue: Queue,
) -> None:
    """
    Handle a step cancellation.

    This should be called by the orchestrator when it detects that a step's
    task has been cancelled.

    Args:
        wf_id: Workflow ID
        step_name: Name of the cancelled step
        reason: Cancellation reason
        queue: The oq queue
    """
    engine = await get_engine()
    now = await queue.now()
    await engine.cancel_step(wf_id, step_name, reason, now)


def create_step_handler(
    workflow: Workflow,
    _step_name: str,
    queue: Queue,
) -> Callable[[dict[str, Any], TaskContext], Coroutine[Any, Any, dict[str, Any]]]:
    """
    Create a step task handler for a specific step.

    This returns a function that can be registered with @worker.task().

    Args:
        workflow: The workflow definition
        _step_name: Name of the step this handler is for (unused, for signature compatibility)
        queue: The oq queue

    Returns:
        Async function suitable for worker.task() decorator
    """

    async def handler(input_data: dict[str, Any], ctx: TaskContext) -> dict[str, Any]:
        return await handle_step(input_data, ctx, workflow, queue)

    return handler
