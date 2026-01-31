"""Integration tests for workflow state management.

These tests require S3/Garage to be running.
"""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING

import pytest

import qo
from qow import (
    StepState,
    StepStatus,
    WorkflowState,
    WorkflowStatus,
    create_workflow_state,
    delete_workflow_state,
    get_workflow_state,
    update_workflow_state,
    workflow_exists,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable

# These tests require S3 - only run with: pytest -m integration
pytestmark = pytest.mark.integration


# Note: queue fixture is defined in conftest.py


@pytest.fixture
async def cleanup_workflow(
    queue: qo.Queue,
) -> AsyncGenerator[Callable[[str], str], None]:
    """Fixture to clean up workflow state after test."""
    workflow_ids: list[str] = []

    def track(wf_id: str) -> str:
        workflow_ids.append(wf_id)
        return wf_id

    yield track

    for wf_id in workflow_ids:
        with contextlib.suppress(Exception):
            await delete_workflow_state(queue, wf_id)


class TestWorkflowStateLifecycle:
    """Tests for workflow state CRUD operations."""

    @pytest.mark.asyncio
    async def test_create_and_get(
        self, queue: qo.Queue, cleanup_workflow: Callable[[str], str]
    ) -> None:
        """Should create and retrieve workflow state."""
        wf_id = cleanup_workflow("test-create-get")

        state = WorkflowState(
            id=wf_id,
            type="test_workflow",
            definition_hash="sha256:test",
            status=WorkflowStatus.PENDING,
            current_steps=[],
            data={"key": "value"},
            steps={},
            signals={},
            error=None,
            created_at="2026-01-28T12:00:00Z",
            updated_at="2026-01-28T12:00:00Z",
        )

        await create_workflow_state(queue, wf_id, state)

        retrieved, etag = await get_workflow_state(queue, wf_id)

        assert retrieved.id == wf_id
        assert retrieved.type == "test_workflow"
        assert retrieved.status == WorkflowStatus.PENDING
        assert retrieved.data == {"key": "value"}
        assert etag is not None

    @pytest.mark.asyncio
    async def test_update_with_cas(
        self, queue: qo.Queue, cleanup_workflow: Callable[[str], str]
    ) -> None:
        """Should update state with CAS protection."""
        wf_id = cleanup_workflow("test-cas-update")

        # Create initial state
        state = WorkflowState(
            id=wf_id,
            type="test_workflow",
            definition_hash="sha256:test",
            status=WorkflowStatus.PENDING,
            current_steps=[],
            data={},
            steps={},
            signals={},
            error=None,
            created_at="2026-01-28T12:00:00Z",
            updated_at="2026-01-28T12:00:00Z",
        )
        await create_workflow_state(queue, wf_id, state)

        # Get with etag
        state, etag = await get_workflow_state(queue, wf_id)

        # Update
        state.status = WorkflowStatus.RUNNING
        state.current_steps = ["step_a"]
        new_etag = await update_workflow_state(queue, wf_id, state, etag)

        # Verify update
        updated, _ = await get_workflow_state(queue, wf_id)
        assert updated.status == WorkflowStatus.RUNNING
        assert updated.current_steps == ["step_a"]
        assert new_etag != etag

    @pytest.mark.asyncio
    async def test_cas_conflict(
        self, queue: qo.Queue, cleanup_workflow: Callable[[str], str]
    ) -> None:
        """CAS should fail on conflict."""
        wf_id = cleanup_workflow("test-cas-conflict")

        # Create initial state
        state = WorkflowState(
            id=wf_id,
            type="test_workflow",
            definition_hash="sha256:test",
            status=WorkflowStatus.PENDING,
            current_steps=[],
            data={},
            steps={},
            signals={},
            error=None,
            created_at="2026-01-28T12:00:00Z",
            updated_at="2026-01-28T12:00:00Z",
        )
        await create_workflow_state(queue, wf_id, state)

        # Get state twice (simulating two concurrent operations)
        state1, etag1 = await get_workflow_state(queue, wf_id)
        state2, etag2 = await get_workflow_state(queue, wf_id)

        # First update succeeds
        state1.status = WorkflowStatus.RUNNING
        await update_workflow_state(queue, wf_id, state1, etag1)

        # Second update should fail (stale etag)
        state2.status = WorkflowStatus.COMPLETED
        with pytest.raises(RuntimeError):
            await update_workflow_state(queue, wf_id, state2, etag2)

    @pytest.mark.asyncio
    async def test_workflow_exists(
        self, queue: qo.Queue, cleanup_workflow: Callable[[str], str]
    ) -> None:
        """Should check if workflow exists."""
        wf_id = cleanup_workflow("test-exists")

        assert await workflow_exists(queue, wf_id) is False

        state = WorkflowState(
            id=wf_id,
            type="test",
            definition_hash="sha256:test",
            status=WorkflowStatus.PENDING,
            current_steps=[],
            data={},
            steps={},
            signals={},
            error=None,
            created_at="2026-01-28T12:00:00Z",
            updated_at="2026-01-28T12:00:00Z",
        )
        await create_workflow_state(queue, wf_id, state)

        assert await workflow_exists(queue, wf_id) is True

    @pytest.mark.asyncio
    async def test_get_nonexistent(self, queue: qo.Queue) -> None:
        """Should raise error for nonexistent workflow."""
        with pytest.raises(RuntimeError):
            await get_workflow_state(queue, "nonexistent-workflow-id")


class TestStepStateManagement:
    """Tests for step state within workflow state."""

    @pytest.mark.asyncio
    async def test_step_state_updates(
        self, queue: qo.Queue, cleanup_workflow: Callable[[str], str]
    ) -> None:
        """Should update step states within workflow."""
        wf_id = cleanup_workflow("test-step-states")

        # Create workflow with initial steps
        state = WorkflowState(
            id=wf_id,
            type="test_workflow",
            definition_hash="sha256:test",
            status=WorkflowStatus.RUNNING,
            current_steps=["step_a"],
            data={},
            steps={
                "step_a": StepState(
                    status=StepStatus.RUNNING, started_at="2026-01-28T12:00:00Z"
                ),
            },
            signals={},
            error=None,
            created_at="2026-01-28T12:00:00Z",
            updated_at="2026-01-28T12:00:00Z",
        )
        await create_workflow_state(queue, wf_id, state)

        # Complete step_a
        state, etag = await get_workflow_state(queue, wf_id)
        state.steps["step_a"] = StepState(
            status=StepStatus.COMPLETED,
            started_at="2026-01-28T12:00:00Z",
            completed_at="2026-01-28T12:01:00Z",
            result={"processed": True},
        )
        state.current_steps = []
        await update_workflow_state(queue, wf_id, state, etag)

        # Verify
        final, _ = await get_workflow_state(queue, wf_id)
        assert final.steps["step_a"].status == StepStatus.COMPLETED
        assert final.steps["step_a"].result == {"processed": True}
