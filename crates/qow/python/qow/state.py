"""Workflow state management operations.

Thin wrapper around Rust StateManager.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from ._rust import get_engine
from .types import WorkflowState

if TYPE_CHECKING:
    from qo import Queue


async def get_workflow_state(
    _queue: Queue,
    wf_id: str,
) -> tuple[WorkflowState, str]:
    """
    Get workflow state from storage.

    Args:
        _queue: Unused (kept for API compatibility)
        wf_id: Workflow ID

    Returns:
        Tuple of (WorkflowState, etag) for CAS updates

    Raises:
        RuntimeError: If workflow doesn't exist
    """
    engine = await get_engine()
    rust_state, etag = await engine.state.get(wf_id)
    return WorkflowState.from_rust(rust_state), etag


async def update_workflow_state(
    queue: Queue,
    wf_id: str,
    state: WorkflowState,
    etag: str,
    *,
    update_timestamp: bool = True,
) -> str:
    """
    Update workflow state with CAS (compare-and-swap).

    Args:
        queue: The qo queue (used for timestamp)
        wf_id: Workflow ID
        state: New state to write
        etag: Expected etag from previous read
        update_timestamp: Whether to update the updated_at field

    Returns:
        New etag after write

    Raises:
        RuntimeError: If CAS fails (state was modified since read)
    """
    if update_timestamp:
        state.updated_at = await queue.now()

    engine = await get_engine()
    rust_state = state.to_rust()
    return await engine.state.update(wf_id, rust_state, etag, state.updated_at or "")


async def create_workflow_state(
    _queue: Queue,
    wf_id: str,
    state: WorkflowState,
) -> str:
    """
    Create initial workflow state.

    Args:
        _queue: Unused (kept for API compatibility)
        wf_id: Workflow ID
        state: Initial state to write

    Returns:
        Etag of created state

    Note:
        This does not use CAS - it will overwrite existing state.
        Use with caution and idempotency keys.
    """
    engine = await get_engine()
    rust_state = state.to_rust()
    return await engine.state.create(wf_id, rust_state)


async def workflow_exists(
    _queue: Queue,
    wf_id: str,
) -> bool:
    """Check if a workflow exists."""
    engine = await get_engine()
    return await engine.state.exists(wf_id)


async def delete_workflow_state(
    _queue: Queue,
    wf_id: str,
) -> None:
    """
    Delete workflow state (for cleanup/testing).

    Args:
        _queue: Unused (kept for API compatibility)
        wf_id: Workflow ID
    """
    engine = await get_engine()
    await engine.state.delete(wf_id)


async def save_step_result(
    _queue: Queue,
    wf_id: str,
    step_name: str,
    result: bytes,
) -> str | None:
    """
    Save step result to storage (atomic, idempotent - won't overwrite existing).

    Uses S3 if-none-match for atomic writes.

    Args:
        _queue: Unused (kept for API compatibility)
        wf_id: Workflow ID
        step_name: Name of the completed step
        result: Serialized step result (JSON bytes)

    Returns:
        Etag of saved result, or None if result already exists
    """
    engine = await get_engine()
    return await engine.state.save_step_result(wf_id, step_name, result)


async def get_step_result(
    _queue: Queue,
    wf_id: str,
    step_name: str,
) -> tuple[bytes, str] | None:
    """
    Get step result from storage.

    Args:
        _queue: Unused (kept for API compatibility)
        wf_id: Workflow ID
        step_name: Name of the step

    Returns:
        Tuple of (result bytes, etag) or None if not found
    """
    engine = await get_engine()
    return await engine.state.get_step_result(wf_id, step_name)


async def list_workflows(
    _queue: Queue,
    prefix: str = "",
    limit: int = 1000,
) -> list[str]:
    """
    List workflow IDs.

    Args:
        _queue: Unused (kept for API compatibility)
        prefix: Optional prefix to filter workflows
        limit: Maximum number of workflows to return

    Returns:
        List of workflow IDs
    """
    engine = await get_engine()
    return await engine.state.list(prefix, limit)
