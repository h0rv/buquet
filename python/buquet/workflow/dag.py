"""DAG computation for workflow step scheduling.

Thin wrapper around Rust DAG functions.
"""

from __future__ import annotations

from buquet.workflow._native import OnFailure as RustOnFailure
from buquet.workflow._native import StepDef as RustStepDef
from buquet.workflow._native import compute_ready_steps as _compute_ready_steps
from buquet.workflow._native import get_blocked_steps as _get_blocked_steps
from buquet.workflow._native import get_compensation_chain as _get_compensation_chain
from buquet.workflow._native import has_unrecoverable_failure as _has_unrecoverable_failure
from buquet.workflow._native import is_workflow_complete as _is_workflow_complete
from buquet.workflow._native import should_pause_workflow as _should_pause_workflow
from buquet.workflow._native import topological_sort as _topological_sort
from buquet.workflow._native import validate_dag as _validate_dag
from buquet.workflow.types import OnFailure, StepDef, WorkflowState

# Map Python OnFailure enum to Rust OnFailure enum
_ON_FAILURE_MAP = {
    OnFailure.FAIL_WORKFLOW: RustOnFailure.FailWorkflow,
    OnFailure.PAUSE_WORKFLOW: RustOnFailure.PauseWorkflow,
    OnFailure.CONTINUE: RustOnFailure.Continue,
    OnFailure.COMPENSATE: RustOnFailure.Compensate,
}


def _to_rust_steps(steps: dict[str, StepDef]) -> list[RustStepDef]:
    """Convert Python steps dict to Rust PyStepDef list."""
    return [
        RustStepDef(
            name=step.name,
            depends_on=step.depends_on,
            retries=step.retries,
            timeout=step.timeout,
            on_failure=_ON_FAILURE_MAP[step.on_failure],
            has_compensation=step.compensation is not None,
        )
        for step in steps.values()
    ]


def compute_ready_steps(
    steps: dict[str, StepDef],
    state: WorkflowState,
) -> list[str]:
    """
    Compute which steps are ready to run.

    A step is ready if:
    1. It's not already completed, running, or failed
    2. All its dependencies are completed
    """
    return _compute_ready_steps(_to_rust_steps(steps), state.to_rust())


def is_workflow_complete(
    steps: dict[str, StepDef],
    state: WorkflowState,
) -> bool:
    """Check if all steps in the workflow are completed."""
    return _is_workflow_complete(_to_rust_steps(steps), state.to_rust())


def has_unrecoverable_failure(
    steps: dict[str, StepDef],
    state: WorkflowState,
) -> bool:
    """
    Check if there's a failed step that blocks further progress.

    A failure is unrecoverable if:
    1. A step has failed
    2. That step's on_failure is FAIL_WORKFLOW
    3. Other steps depend on it (directly or transitively)
    """
    return _has_unrecoverable_failure(_to_rust_steps(steps), state.to_rust())


def should_pause_workflow(
    steps: dict[str, StepDef],
    state: WorkflowState,
) -> bool:
    """Check if workflow should pause due to step failure with PAUSE_WORKFLOW policy."""
    return _should_pause_workflow(_to_rust_steps(steps), state.to_rust())


def get_compensation_chain(
    steps: dict[str, StepDef],
    state: WorkflowState,
) -> list[str]:
    """
    Get the compensation chain for completed steps in reverse order.

    When a step fails with COMPENSATE policy, we need to run compensation
    handlers for all previously completed steps in reverse order.
    """
    return _get_compensation_chain(_to_rust_steps(steps), state.to_rust())


def get_blocked_steps(
    steps: dict[str, StepDef],
    state: WorkflowState,
) -> list[str]:
    """Get steps that are blocked by failed dependencies."""
    return _get_blocked_steps(_to_rust_steps(steps), state.to_rust())


def topological_sort(steps: dict[str, StepDef]) -> list[str]:
    """
    Sort steps in topological order (dependencies before dependents).

    Raises:
        ValueError: If there's a cycle in the dependency graph
    """
    return _topological_sort(_to_rust_steps(steps))


def validate_dag(steps: dict[str, StepDef]) -> list[str]:
    """
    Validate the workflow DAG structure.

    Returns:
        List of validation errors (empty if valid)
    """
    return _validate_dag(_to_rust_steps(steps))
