"""Signal handling for workflows.

Thin wrapper around Rust SignalManager.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from buquet.workflow._native import parse_signal_key as _parse_signal_key
from buquet.workflow._rust import get_engine
from buquet.workflow.state import update_workflow_state
from buquet.workflow.types import Signal, SignalCursor, WorkflowState

if TYPE_CHECKING:
    from buquet import Queue


async def send_signal(
    queue: Queue,
    wf_id: str,
    name: str,
    payload: object,
) -> str:
    """
    Send a signal to a workflow.

    Signals are stored as immutable S3 objects with timestamp-based keys
    for ordering.

    Args:
        queue: The buquet queue (used for timestamp)
        wf_id: Workflow ID
        name: Signal name (e.g., "approval", "payment_received")
        payload: Signal data (must be JSON-serializable)

    Returns:
        The signal ID (UUID)
    """
    ts = await queue.now()
    engine = await get_engine()
    return await engine.signals.send(wf_id, name, payload, ts)


async def list_signals(
    _queue: Queue,
    wf_id: str,
    name: str,
    cursor: str | None = None,
    limit: int = 100,
) -> list[tuple[str, Signal]]:
    """
    List signals after the cursor.

    Args:
        _queue: Unused (kept for API compatibility)
        wf_id: Workflow ID
        name: Signal name
        cursor: Optional cursor (timestamp_uuid suffix) to start after
        limit: Maximum number of signals to return

    Returns:
        List of (suffix, Signal) tuples, where suffix is the cursor value
    """
    engine = await get_engine()
    rust_signals = await engine.signals.list(wf_id, name, cursor, limit)

    # Convert Rust signals to Python Signal dataclass
    return [
        (
            cursor_val,
            Signal(
                id=rust_signal.id,
                name=rust_signal.name,
                payload=rust_signal.payload,
                created_at=rust_signal.created_at,
            ),
        )
        for cursor_val, rust_signal in rust_signals
    ]


async def get_next_signal(
    _queue: Queue,
    wf_id: str,
    name: str,
    cursor: str | None = None,
) -> tuple[str, Signal] | None:
    """
    Get the next unconsumed signal.

    Args:
        _queue: Unused (kept for API compatibility)
        wf_id: Workflow ID
        name: Signal name
        cursor: Current cursor position

    Returns:
        Tuple of (new_cursor, Signal) or None if no signals available
    """
    engine = await get_engine()
    result = await engine.signals.get_next(wf_id, name, cursor)

    if result is None:
        return None

    cursor_val, rust_signal = result
    return (
        cursor_val,
        Signal(
            id=rust_signal.id,
            name=rust_signal.name,
            payload=rust_signal.payload,
            created_at=rust_signal.created_at,
        ),
    )


async def consume_signal(
    queue: Queue,
    wf_id: str,
    state: WorkflowState,
    etag: str,
    signal_name: str,
) -> tuple[Signal | None, WorkflowState, str]:
    """
    Consume the next signal and update workflow state cursor.

    This is an atomic operation that:
    1. Gets the next signal after the current cursor
    2. Updates the workflow state with the new cursor
    3. Returns the signal

    If no signal is available, returns None without modifying state.

    Args:
        queue: The buquet queue
        wf_id: Workflow ID
        state: Current workflow state
        etag: Current state etag
        signal_name: Name of signal to consume

    Returns:
        Tuple of (Signal or None, updated state, new etag)
    """
    # Get current cursor
    cursor_info = state.signals.get(signal_name)
    cursor = cursor_info.cursor if cursor_info else None

    # Get next signal
    result = await get_next_signal(queue, wf_id, signal_name, cursor=cursor)
    if result is None:
        return None, state, etag

    new_cursor, signal = result

    # Update cursor in state
    if signal_name not in state.signals:
        state.signals[signal_name] = SignalCursor()
    state.signals[signal_name].cursor = new_cursor

    # Save updated state
    new_etag = await update_workflow_state(queue, wf_id, state, etag)

    return signal, state, new_etag


async def count_signals(
    _queue: Queue,
    wf_id: str,
    name: str,
    cursor: str | None = None,
) -> int:
    """
    Count signals after the cursor.

    Args:
        _queue: Unused (kept for API compatibility)
        wf_id: Workflow ID
        name: Signal name
        cursor: Optional cursor to count signals after

    Returns:
        Number of signals available
    """
    engine = await get_engine()
    return await engine.signals.count(wf_id, name, cursor)


async def delete_signals(
    _queue: Queue,
    wf_id: str,
    name: str,
) -> int:
    """
    Delete all signals of a given name (for cleanup/testing).

    Note: In production, signals should be retained for audit purposes.

    Args:
        _queue: Unused (kept for API compatibility)
        wf_id: Workflow ID
        name: Signal name

    Returns:
        Number of signals deleted
    """
    engine = await get_engine()
    return await engine.signals.delete_all(wf_id, name)


def parse_signal_key(key: str) -> tuple[str, str, str, str] | None:
    """
    Parse a signal key into components.

    Args:
        key: Full S3 key (e.g., "workflow/wf-123/signals/approval/2026-01-28T12:00:00Z_abc123.json")

    Returns:
        Tuple of (workflow_id, signal_name, timestamp, signal_id) or None if parsing fails
    """
    return _parse_signal_key(key)
