"""Orchestrator task handler for workflow execution."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from buquet import TaskStatus
from buquet.workflow._rust import get_engine
from buquet.workflow.dag import (
    compute_ready_steps,
    has_unrecoverable_failure,
    is_workflow_complete,
    should_pause_workflow,
)
from buquet.workflow.state import get_workflow_state, update_workflow_state
from buquet.workflow.steps import handle_step_cancellation, handle_step_failure
from buquet.workflow.types import (
    StepState,
    StepStatus,
    WorkflowError,
    WorkflowState,
    WorkflowStatus,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    from buquet import Queue, TaskContext
    from buquet.workflow.workflow import Workflow

logger = logging.getLogger("buquet.workflow.orchestrator")


async def _check_terminal_status(
    state: WorkflowState,
    wf_id: str,
) -> dict[str, Any] | None:
    """Check if workflow is already in a terminal or stopped state."""
    terminal_statuses = (
        WorkflowStatus.PAUSED,
        WorkflowStatus.CANCELLED,
        WorkflowStatus.COMPLETED,
        WorkflowStatus.FAILED,
    )
    if state.status in terminal_statuses:
        return {
            "noop": True,
            "reason": f"workflow already {state.status.value}",
            "workflow_id": wf_id,
        }
    return None


async def _check_version_mismatch(
    state: WorkflowState,
    workflow: Workflow,
    queue: Queue,
    wf_id: str,
    etag: str,
) -> dict[str, Any] | None:
    """Check for version mismatch and pause if detected."""
    if state.definition_hash == workflow.definition_hash:
        return None

    state.status = WorkflowStatus.PAUSED
    expected_hash = state.definition_hash
    actual_hash = workflow.definition_hash
    mismatch_msg = f"Workflow definition changed. Expected {expected_hash}, got {actual_hash}"
    state.error = WorkflowError(
        type="VersionMismatch",
        message=mismatch_msg,
    )
    logger.warning(
        "Workflow %s paused: version mismatch (expected %s, got %s). "
        "The workflow was created with a different definition than the worker is running.",
        wf_id,
        expected_hash,
        actual_hash,
    )
    await update_workflow_state(queue, wf_id, state, etag)
    return {"paused": True, "reason": "version_mismatch", "workflow_id": wf_id}


async def _check_workflow_terminal_conditions(
    state: WorkflowState,
    workflow: Workflow,
    queue: Queue,
    wf_id: str,
    etag: str,
) -> dict[str, Any] | None:
    """Check for completion, failure, or pause conditions."""
    if is_workflow_complete(workflow.steps, state):
        state.status = WorkflowStatus.COMPLETED
        state.current_steps = []
        await update_workflow_state(queue, wf_id, state, etag)
        logger.info("Workflow %s completed successfully", wf_id)
        return {"completed": True, "workflow_id": wf_id}

    if has_unrecoverable_failure(workflow.steps, state):
        state.status = WorkflowStatus.FAILED
        await update_workflow_state(queue, wf_id, state, etag)
        logger.error("Workflow %s failed: unrecoverable step failure", wf_id)
        return {"failed": True, "workflow_id": wf_id}

    if should_pause_workflow(workflow.steps, state):
        state.status = WorkflowStatus.PAUSED
        await update_workflow_state(queue, wf_id, state, etag)
        logger.warning("Workflow %s paused due to step failure policy", wf_id)
        return {"paused": True, "reason": "step_failure_policy", "workflow_id": wf_id}

    return None


async def _reconcile_running_steps(
    state: WorkflowState,
    queue: Queue,
    wf_id: str,
    now: str,
) -> tuple[WorkflowState, str, str | None]:
    """
    Reconcile running steps with their task statuses.

    Returns:
        Tuple of (state, etag, cancellation_reason) where cancellation_reason
        is the reason if a step was cancelled (workflow should be cancelled),
        or None if no cancellation occurred.
    """
    etag: str
    cancellation_reason: str | None = None

    for step_name, step_state in list(state.steps.items()):
        if step_state.status != StepStatus.RUNNING or not step_state.task_id:
            continue

        task = await queue.get(step_state.task_id)
        if task is None:
            continue

        if task.status == TaskStatus.Cancelled:
            # Cancelled task -> cancelled step -> cancelled workflow
            reason = task.last_error or "Task was cancelled"
            await handle_step_cancellation(wf_id, step_name, reason, queue)
            state, etag = await get_workflow_state(queue, wf_id)
            cancellation_reason = reason
        elif task.status in (TaskStatus.Failed, TaskStatus.Expired):
            # Failed/expired task -> failed step -> follow on_failure policy
            error_msg = task.last_error or "Task failed"
            await handle_step_failure(wf_id, step_name, error_msg, queue)
            state, etag = await get_workflow_state(queue, wf_id)
        elif task.status == TaskStatus.Completed and step_state.status == StepStatus.RUNNING:
            engine = await get_engine()
            # Load output (resolves output_ref if offloaded, preserves real None)
            output = await queue.load_output(task)
            await engine.complete_step(wf_id, step_name, output, now)
            state, etag = await get_workflow_state(queue, wf_id)

    # Re-fetch to ensure we have latest state and etag
    state, etag = await get_workflow_state(queue, wf_id)
    return state, etag, cancellation_reason


async def _submit_ready_steps(
    state: WorkflowState,
    workflow: Workflow,
    queue: Queue,
    wf_id: str,
    now: str,
    ready_steps: list[str],
) -> list[str]:
    """Submit tasks for ready steps and update state."""
    submitted_steps: list[str] = []

    for step_name in ready_steps:
        step_def = workflow.steps[step_name]

        task = await queue.submit(
            f"workflow.step:{workflow.name}:{step_name}",
            {
                "workflow_id": wf_id,
                "step": step_name,
                "data": state.data,
            },
            idempotency_key=f"{wf_id}:{step_name}",
            timeout_seconds=step_def.timeout,
            max_retries=step_def.retries,
        )

        state.steps[step_name] = StepState(
            status=StepStatus.RUNNING,
            started_at=now,
            attempt=state.steps.get(step_name, StepState()).attempt + 1,
            task_id=task.id,
        )

        submitted_steps.append(step_name)

    return submitted_steps


async def orchestrate(
    input_data: dict[str, Any],
    _ctx: TaskContext,
    workflow: Workflow,
    queue: Queue,
) -> dict[str, Any]:
    """
    Main orchestrator logic that drives workflow execution.

    This function:
    1. Gets the current workflow state
    2. Checks for version compatibility
    3. Computes ready steps based on DAG
    4. Submits step tasks
    5. Updates workflow state

    Args:
        input_data: Input from orchestrator task (contains workflow_id)
        _ctx: Task context for lease extension (unused, reserved for future use)
        workflow: The workflow definition
        queue: The buquet queue

    Returns:
        Result dict with status and actions taken
    """
    wf_id = input_data["workflow_id"]

    # Get current state
    try:
        state, etag = await get_workflow_state(queue, wf_id)
    except RuntimeError as e:
        return {"error": str(e), "workflow_id": wf_id}

    # Guard: no-op if workflow is already stopped or terminal
    result = await _check_terminal_status(state, wf_id)
    if result is None:
        result = await _check_version_mismatch(state, workflow, queue, wf_id, etag)
    if result is None:
        result = await _check_workflow_terminal_conditions(state, workflow, queue, wf_id, etag)
    if result is not None:
        return result

    # Get current timestamp for any state updates
    now = await queue.now()

    # Reconcile running steps with task statuses
    state, etag, cancellation_reason = await _reconcile_running_steps(state, queue, wf_id, now)

    # If a step was cancelled, cancel the entire workflow
    if cancellation_reason is not None:
        state.status = WorkflowStatus.CANCELLED
        state.error = WorkflowError(type="Cancelled", message=cancellation_reason)
        await update_workflow_state(queue, wf_id, state, etag)
        logger.warning("Workflow %s cancelled: %s", wf_id, cancellation_reason)
        return {"cancelled": True, "workflow_id": wf_id}

    # Re-check terminal conditions after reconciliation (step failures may have changed state)
    if result := await _check_workflow_terminal_conditions(state, workflow, queue, wf_id, etag):
        return result

    # Compute and submit next steps
    ready_steps = compute_ready_steps(workflow.steps, state)

    if not ready_steps:
        logger.debug(
            "Workflow %s waiting on steps: %s",
            wf_id,
            state.current_steps or "none",
        )
        return {
            "waiting": True,
            "workflow_id": wf_id,
            "current_steps": state.current_steps,
        }

    submitted_steps = await _submit_ready_steps(state, workflow, queue, wf_id, now, ready_steps)
    if submitted_steps:
        logger.info("Workflow %s submitted steps: %s", wf_id, submitted_steps)

    # Update current_steps
    currently_running = {name for name, s in state.steps.items() if s.status == StepStatus.RUNNING}
    state.current_steps = list(currently_running)
    state.status = WorkflowStatus.RUNNING

    # Save state
    await update_workflow_state(queue, wf_id, state, etag)

    return {
        "submitted_steps": submitted_steps,
        "workflow_id": wf_id,
        "current_steps": state.current_steps,
    }


def create_orchestrator_handler(
    workflow: Workflow,
    queue: Queue,
) -> Callable[[dict[str, Any], TaskContext], Coroutine[Any, Any, dict[str, Any]]]:
    """
    Create an orchestrator task handler for a workflow.

    This returns a function that can be registered with @worker.task().

    Args:
        workflow: The workflow definition
        queue: The buquet queue

    Returns:
        Async function suitable for worker.task() decorator
    """

    async def handler(input_data: dict[str, Any], _ctx: TaskContext) -> dict[str, Any]:
        return await orchestrate(input_data, _ctx, workflow, queue)

    return handler
