"""Integration tests for signal handling.

These tests require S3/Garage to be running.
"""

from __future__ import annotations

import contextlib
import os
from typing import TYPE_CHECKING

import pytest

import oq
from oq import Queue
from oq_workflows import (
    WorkflowState,
    WorkflowStatus,
    count_signals,
    create_workflow_state,
    delete_signals,
    delete_workflow_state,
    get_next_signal,
    list_signals,
    parse_signal_key,
    send_signal,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

# Skip all tests if S3 not configured
pytestmark = pytest.mark.skipif(
    not os.environ.get("S3_BUCKET"),
    reason="S3_BUCKET not set - skipping integration tests",
)


@pytest.fixture
async def queue() -> Queue:
    """Connect to oq queue."""
    return await oq.connect()


@pytest.fixture
async def test_workflow(queue: Queue) -> AsyncIterator[str]:
    """Create and cleanup a test workflow."""
    wf_id = f"test-signals-{id(queue)}"

    state = WorkflowState(
        id=wf_id,
        type="test",
        definition_hash="sha256:test",
        status=WorkflowStatus.RUNNING,
        current_steps=[],
        data={},
        steps={},
        signals={},
        error=None,
        created_at="2026-01-28T12:00:00Z",
        updated_at="2026-01-28T12:00:00Z",
    )
    await create_workflow_state(queue, wf_id, state)

    yield wf_id

    # Cleanup
    await delete_signals(queue, wf_id, "test_signal")
    await delete_signals(queue, wf_id, "approval")
    with contextlib.suppress(Exception):
        await delete_workflow_state(queue, wf_id)


class TestSendSignal:
    """Tests for sending signals."""

    @pytest.mark.asyncio
    async def test_send_signal(self, queue: Queue, test_workflow: str) -> None:
        """Should send a signal and return signal ID."""
        signal_id = await send_signal(
            queue, test_workflow, "test_signal", {"data": "value"}
        )

        assert signal_id is not None
        assert len(signal_id) == 36  # UUID format

    @pytest.mark.asyncio
    async def test_send_multiple_signals(self, queue: Queue, test_workflow: str) -> None:
        """Should send multiple signals with unique IDs."""
        id1 = await send_signal(queue, test_workflow, "test_signal", {"n": 1})
        id2 = await send_signal(queue, test_workflow, "test_signal", {"n": 2})
        id3 = await send_signal(queue, test_workflow, "test_signal", {"n": 3})

        assert len({id1, id2, id3}) == 3  # All unique


class TestListSignals:
    """Tests for listing signals."""

    @pytest.mark.asyncio
    async def test_list_signals_empty(self, queue: Queue, test_workflow: str) -> None:
        """Should return empty list when no signals."""
        signals = await list_signals(queue, test_workflow, "nonexistent")
        assert signals == []

    @pytest.mark.asyncio
    async def test_list_signals_returns_all(self, queue: Queue, test_workflow: str) -> None:
        """Should list all signals in order."""
        await send_signal(queue, test_workflow, "test_signal", {"n": 1})
        await send_signal(queue, test_workflow, "test_signal", {"n": 2})
        await send_signal(queue, test_workflow, "test_signal", {"n": 3})

        signals = await list_signals(queue, test_workflow, "test_signal")

        assert len(signals) == 3
        # Should be in timestamp order
        payloads = [s[1].payload["n"] for s in signals]
        assert payloads == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_list_signals_with_cursor(self, queue: Queue, test_workflow: str) -> None:
        """Should list signals after cursor."""
        await send_signal(queue, test_workflow, "test_signal", {"n": 1})
        await send_signal(queue, test_workflow, "test_signal", {"n": 2})
        await send_signal(queue, test_workflow, "test_signal", {"n": 3})

        # Get all signals
        all_signals = await list_signals(queue, test_workflow, "test_signal")
        first_cursor = all_signals[0][0]

        # List after first cursor
        remaining = await list_signals(
            queue, test_workflow, "test_signal", cursor=first_cursor
        )

        assert len(remaining) == 2
        payloads = [s[1].payload["n"] for s in remaining]
        assert payloads == [2, 3]


class TestGetNextSignal:
    """Tests for getting next signal."""

    @pytest.mark.asyncio
    async def test_get_next_signal_empty(self, queue: Queue, test_workflow: str) -> None:
        """Should return None when no signals."""
        result = await get_next_signal(queue, test_workflow, "nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_next_signal(self, queue: Queue, test_workflow: str) -> None:
        """Should get the first signal."""
        await send_signal(queue, test_workflow, "test_signal", {"n": 1})
        await send_signal(queue, test_workflow, "test_signal", {"n": 2})

        result = await get_next_signal(queue, test_workflow, "test_signal")

        assert result is not None
        cursor, signal = result
        assert signal.payload == {"n": 1}
        assert cursor is not None

    @pytest.mark.asyncio
    async def test_get_next_signal_with_cursor(self, queue: Queue, test_workflow: str) -> None:
        """Should get signal after cursor."""
        await send_signal(queue, test_workflow, "test_signal", {"n": 1})
        await send_signal(queue, test_workflow, "test_signal", {"n": 2})

        # Get first
        result1 = await get_next_signal(queue, test_workflow, "test_signal")
        cursor1, signal1 = result1

        # Get second using cursor
        result2 = await get_next_signal(
            queue, test_workflow, "test_signal", cursor=cursor1
        )
        cursor2, signal2 = result2

        assert signal1.payload == {"n": 1}
        assert signal2.payload == {"n": 2}

        # No more signals
        result3 = await get_next_signal(
            queue, test_workflow, "test_signal", cursor=cursor2
        )
        assert result3 is None


class TestCountSignals:
    """Tests for counting signals."""

    @pytest.mark.asyncio
    async def test_count_signals_empty(self, queue: Queue, test_workflow: str) -> None:
        """Should return 0 when no signals."""
        count = await count_signals(queue, test_workflow, "nonexistent")
        assert count == 0

    @pytest.mark.asyncio
    async def test_count_signals(self, queue: Queue, test_workflow: str) -> None:
        """Should count all signals."""
        await send_signal(queue, test_workflow, "test_signal", {"n": 1})
        await send_signal(queue, test_workflow, "test_signal", {"n": 2})
        await send_signal(queue, test_workflow, "test_signal", {"n": 3})

        count = await count_signals(queue, test_workflow, "test_signal")
        assert count == 3

    @pytest.mark.asyncio
    async def test_count_signals_with_cursor(self, queue: Queue, test_workflow: str) -> None:
        """Should count signals after cursor."""
        await send_signal(queue, test_workflow, "test_signal", {"n": 1})
        await send_signal(queue, test_workflow, "test_signal", {"n": 2})
        await send_signal(queue, test_workflow, "test_signal", {"n": 3})

        # Get first signal cursor
        result = await get_next_signal(queue, test_workflow, "test_signal")
        cursor = result[0]

        # Count remaining
        count = await count_signals(
            queue, test_workflow, "test_signal", cursor=cursor
        )
        assert count == 2


class TestDeleteSignals:
    """Tests for deleting signals."""

    @pytest.mark.asyncio
    async def test_delete_signals(self, queue: Queue, test_workflow: str) -> None:
        """Should delete all signals of a name."""
        await send_signal(queue, test_workflow, "test_signal", {"n": 1})
        await send_signal(queue, test_workflow, "test_signal", {"n": 2})

        deleted = await delete_signals(queue, test_workflow, "test_signal")
        assert deleted == 2

        # Verify deletion
        count = await count_signals(queue, test_workflow, "test_signal")
        assert count == 0

    @pytest.mark.asyncio
    async def test_delete_nonexistent_signals(
        self, queue: Queue, test_workflow: str
    ) -> None:
        """Should return 0 when deleting nonexistent signals."""
        deleted = await delete_signals(queue, test_workflow, "nonexistent")
        assert deleted == 0


class TestParseSignalKey:
    """Tests for parsing signal keys."""

    def test_parse_signal_key(self) -> None:
        """Should parse signal key into components."""
        key = "workflow/wf-123/signals/approval/2026-01-28T12:00:00Z_abc123def456.json"

        wf_id, name, timestamp, signal_id = parse_signal_key(key)

        assert wf_id == "wf-123"
        assert name == "approval"
        assert timestamp == "2026-01-28T12:00:00Z"
        assert signal_id == "abc123def456"

    def test_parse_signal_key_with_hyphens(self) -> None:
        """Should handle timestamps with hyphens correctly."""
        key = (
            "workflow/wf-order-123/signals/payment/"
            "2026-01-28T12:30:45.123Z_a1b2c3d4-e5f6-7890-abcd-ef1234567890.json"
        )

        wf_id, name, timestamp, signal_id = parse_signal_key(key)

        assert wf_id == "wf-order-123"
        assert name == "payment"
        # Timestamp includes everything up to the last underscore
        assert "2026-01-28" in timestamp
        assert signal_id == "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
