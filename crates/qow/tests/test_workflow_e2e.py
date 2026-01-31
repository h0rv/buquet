"""End-to-end tests for workflow execution.

These tests require S3/Garage to be running and a worker processing tasks.
They are the most comprehensive tests but require full infrastructure.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

import qo
from qow import (
    StepContext,
    Workflow,
    WorkflowClient,
    WorkflowStatus,
    delete_workflow_state,
)

if TYPE_CHECKING:
    from typing import Any

# These tests require S3 - only run with: pytest -m integration
pytestmark = pytest.mark.integration


# Note: queue fixture is defined in conftest.py


@pytest.fixture
def simple_workflow() -> Workflow:
    """Create a simple 2-step workflow."""
    wf = Workflow("e2e_simple")

    @wf.step("step_one")
    async def step_one(ctx: StepContext) -> dict[str, Any]:
        return {"one": "done", "input": ctx.data}

    @wf.step("step_two", depends_on=["step_one"])
    async def step_two(_ctx: StepContext) -> dict[str, str]:
        return {"two": "done"}

    return wf


@pytest.fixture
def parallel_workflow() -> Workflow:
    """Create a workflow with parallel steps."""
    wf = Workflow("e2e_parallel")

    @wf.step("start")
    async def start(_ctx: StepContext) -> dict[str, bool]:
        return {"started": True}

    @wf.step("parallel_a", depends_on=["start"])
    async def parallel_a(_ctx: StepContext) -> dict[str, str]:
        return {"a": "done"}

    @wf.step("parallel_b", depends_on=["start"])
    async def parallel_b(_ctx: StepContext) -> dict[str, str]:
        return {"b": "done"}

    @wf.step("finish", depends_on=["parallel_a", "parallel_b"])
    async def finish(_ctx: StepContext) -> dict[str, bool]:
        return {"finished": True}

    return wf


class TestWorkflowLifecycle:
    """Tests for basic workflow lifecycle."""

    @pytest.mark.asyncio
    async def test_start_workflow(
        self, queue: qo.Queue, simple_workflow: Workflow
    ) -> None:
        """Should start a workflow and get initial status."""
        client = WorkflowClient(queue)

        run = await client.start(simple_workflow, {"input": "test"})

        assert run.id.startswith("wf-")
        assert run.status == WorkflowStatus.RUNNING

        # Cleanup
        await delete_workflow_state(queue, run.id)

    @pytest.mark.asyncio
    async def test_get_workflow_status(
        self, queue: qo.Queue, simple_workflow: Workflow
    ) -> None:
        """Should get workflow status."""
        client = WorkflowClient(queue)

        run = await client.start(simple_workflow, {"input": "test"})

        status = await client.get(run.id)
        assert status is not None
        assert status.id == run.id

        # Cleanup
        await delete_workflow_state(queue, run.id)

    @pytest.mark.asyncio
    async def test_cancel_workflow(
        self, queue: qo.Queue, simple_workflow: Workflow
    ) -> None:
        """Should cancel a workflow."""
        client = WorkflowClient(queue)

        run = await client.start(simple_workflow, {"input": "test"})
        cancelled = await client.cancel(run.id, reason="Test cancellation")

        assert cancelled.status == WorkflowStatus.CANCELLED
        assert cancelled.error is not None
        assert cancelled.error.message == "Test cancellation"

        # Cleanup
        await delete_workflow_state(queue, run.id)

    @pytest.mark.asyncio
    async def test_workflow_exists(
        self, queue: qo.Queue, simple_workflow: Workflow
    ) -> None:
        """Should check workflow existence."""
        client = WorkflowClient(queue)

        assert await client.exists("nonexistent-wf-id") is False

        run = await client.start(simple_workflow, {"input": "test"})
        assert await client.exists(run.id) is True

        # Cleanup
        await delete_workflow_state(queue, run.id)


class TestWorkflowSignals:
    """Tests for workflow signals."""

    @pytest.mark.asyncio
    async def test_send_signal(
        self, queue: qo.Queue, simple_workflow: Workflow
    ) -> None:
        """Should send a signal to a workflow."""
        client = WorkflowClient(queue)

        run = await client.start(simple_workflow, {"input": "test"})
        signal_id = await client.signal(run.id, "test_signal", {"data": "value"})

        assert signal_id is not None
        assert len(signal_id) == 36  # UUID

        # Cleanup
        await delete_workflow_state(queue, run.id)


class TestWorkflowPauseResume:
    """Tests for pausing and resuming workflows."""

    @pytest.mark.asyncio
    async def test_pause_and_resume(
        self, queue: qo.Queue, simple_workflow: Workflow
    ) -> None:
        """Should pause and resume a workflow."""
        client = WorkflowClient(queue)

        run = await client.start(simple_workflow, {"input": "test"})

        # Pause
        paused = await client.pause(run.id, reason="Test pause")
        assert paused.status == WorkflowStatus.PAUSED

        # Resume
        resumed = await client.resume(run.id)
        assert resumed.status == WorkflowStatus.RUNNING

        # Cleanup
        await delete_workflow_state(queue, run.id)


class TestWorkflowValidation:
    """Tests for workflow validation."""

    def test_invalid_workflow_cycle(self) -> None:
        """Should detect cycle in workflow."""
        wf = Workflow("invalid_cycle")

        @wf.step("a", depends_on=["b"])
        async def step_a(_ctx: StepContext) -> dict[str, str]:
            return {}

        @wf.step("b", depends_on=["a"])
        async def step_b(_ctx: StepContext) -> dict[str, str]:
            return {}

        errors = wf.validate()
        assert len(errors) == 1
        assert "Cycle" in errors[0]

    def test_invalid_workflow_missing_dep(self) -> None:
        """Should detect missing dependency."""
        wf = Workflow("invalid_missing")

        @wf.step("a", depends_on=["nonexistent"])
        async def step_a(_ctx: StepContext) -> dict[str, str]:
            return {}

        errors = wf.validate()
        assert len(errors) == 1
        assert "unknown step" in errors[0]

    def test_valid_workflow(self, simple_workflow: Workflow) -> None:
        """Valid workflow should have no errors."""
        errors = simple_workflow.validate()
        assert errors == []

    def test_parallel_workflow_valid(self, parallel_workflow: Workflow) -> None:
        """Parallel workflow should be valid."""
        errors = parallel_workflow.validate()
        assert errors == []
