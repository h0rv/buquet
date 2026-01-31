"""Tests for orchestrator logic."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from qo import TaskStatus
from qow import (
    OnFailure,
    StepContext,
    StepDef,
    StepState,
    StepStatus,
    Workflow,
    WorkflowState,
    WorkflowStatus,
)
from qow.orchestrator import (
    _check_workflow_terminal_conditions,
    _reconcile_running_steps,
    orchestrate,
)

# Type alias for mock queue objects (AsyncMock)
MockQueue = object


async def dummy_handler(_ctx: StepContext) -> dict[str, Any]:
    """Dummy step handler for tests."""
    return {}


def make_state(
    steps: dict[str, StepState] | None = None,
    status: WorkflowStatus = WorkflowStatus.RUNNING,
) -> WorkflowState:
    """Create a minimal workflow state for testing."""
    return WorkflowState(
        id="wf-test",
        type="test",
        definition_hash="sha256:test",
        status=status,
        current_steps=[],
        data={},
        steps=steps or {},
        signals={},
        error=None,
        created_at="2026-01-28T12:00:00Z",
        updated_at="2026-01-28T12:00:00Z",
    )


@dataclass
class MockTask:
    """Mock task for testing."""

    id: str
    status: Any
    output: Any = None
    last_error: str | None = None


class TestReconcileRunningSteps:
    """Tests for _reconcile_running_steps function."""

    @pytest.mark.asyncio
    async def test_failed_task_triggers_step_failure(self):
        """Failed task should trigger handle_step_failure with last_error."""
        state = make_state({
            "step_a": StepState(status=StepStatus.RUNNING, task_id="task-123"),
        })

        mock_queue = AsyncMock()
        mock_queue.get = AsyncMock(
            return_value=MockTask(
                id="task-123",
                status=TaskStatus.Failed,
                last_error="Custom error message",
            )
        )
        mock_queue.now = AsyncMock(return_value="2026-01-28T12:00:00Z")

        failed_state = make_state({
            "step_a": StepState(status=StepStatus.FAILED, error="Custom error message"),
        })

        call_args: list[tuple[str, str, str]] = []

        async def mock_handle_failure(
            wf_id: str, step: str, error: str, _queue: MockQueue
        ) -> None:
            call_args.append((wf_id, step, error))

        async def mock_get_state(_queue: MockQueue, wf_id: str) -> tuple[WorkflowState, str]:
            return failed_state, "etag-2"

        with (
            patch(
                "qow.orchestrator.handle_step_failure",
                side_effect=mock_handle_failure,
            ),
            patch(
                "qow.orchestrator.get_workflow_state",
                side_effect=mock_get_state,
            ),
        ):
            result_state, result_etag, cancellation_reason = await _reconcile_running_steps(
                state, mock_queue, "wf-test", "2026-01-28T12:00:00Z"
            )

            # Verify handle_step_failure was called with correct args
            assert len(call_args) == 1
            assert call_args[0] == ("wf-test", "step_a", "Custom error message")
            # Failed task should not cancel workflow (follows on_failure policy)
            assert cancellation_reason is None

    @pytest.mark.asyncio
    async def test_cancelled_task_triggers_step_cancellation(self):
        """Cancelled task should trigger step cancellation, not failure."""
        state = make_state({
            "step_a": StepState(status=StepStatus.RUNNING, task_id="task-123"),
        })

        mock_queue = AsyncMock()
        mock_queue.get = AsyncMock(
            return_value=MockTask(
                id="task-123",
                status=TaskStatus.Cancelled,
                last_error="User requested cancellation",
            )
        )
        mock_queue.now = AsyncMock(return_value="2026-01-28T12:00:00Z")

        cancelled_state = make_state({
            "step_a": StepState(status=StepStatus.CANCELLED),
        })

        cancel_args: list[tuple[str, str, str]] = []

        async def mock_handle_cancellation(
            wf_id: str, step: str, reason: str, _queue: MockQueue
        ) -> None:
            cancel_args.append((wf_id, step, reason))

        async def mock_get_state(_queue: MockQueue, wf_id: str) -> tuple[WorkflowState, str]:
            return cancelled_state, "etag-2"

        with (
            patch(
                "qow.orchestrator.handle_step_cancellation",
                side_effect=mock_handle_cancellation,
            ),
            patch(
                "qow.orchestrator.get_workflow_state",
                side_effect=mock_get_state,
            ),
        ):
            result_state, result_etag, cancellation_reason = await _reconcile_running_steps(
                state, mock_queue, "wf-test", "2026-01-28T12:00:00Z"
            )

            # Should call handle_step_cancellation, not handle_step_failure
            assert len(cancel_args) == 1
            assert cancel_args[0] == ("wf-test", "step_a", "User requested cancellation")
            # Should return the cancellation reason (not just True)
            assert cancellation_reason == "User requested cancellation"

    @pytest.mark.asyncio
    async def test_expired_task_triggers_step_failure(self):
        """Expired task should trigger step failure (not cancellation)."""
        state = make_state({
            "step_a": StepState(status=StepStatus.RUNNING, task_id="task-123"),
        })

        mock_queue = AsyncMock()
        mock_queue.get = AsyncMock(
            return_value=MockTask(
                id="task-123",
                status=TaskStatus.Expired,
                last_error=None,  # Expired tasks may not have last_error
            )
        )
        mock_queue.now = AsyncMock(return_value="2026-01-28T12:00:00Z")

        failed_state = make_state({
            "step_a": StepState(status=StepStatus.FAILED),
        })

        call_args: list[tuple[str, str, str]] = []

        async def mock_handle_failure(
            wf_id: str, step: str, error: str, _queue: MockQueue
        ) -> None:
            call_args.append((wf_id, step, error))

        async def mock_get_state(_queue: MockQueue, wf_id: str) -> tuple[WorkflowState, str]:
            return failed_state, "etag-2"

        with (
            patch(
                "qow.orchestrator.handle_step_failure",
                side_effect=mock_handle_failure,
            ),
            patch(
                "qow.orchestrator.get_workflow_state",
                side_effect=mock_get_state,
            ),
        ):
            result_state, result_etag, cancellation_reason = await _reconcile_running_steps(
                state, mock_queue, "wf-test", "2026-01-28T12:00:00Z"
            )

            assert len(call_args) == 1
            # Should use default error message when last_error is None
            assert call_args[0][2] == "Task failed"
            # Expired task follows on_failure policy, not auto-cancel
            assert cancellation_reason is None

    @pytest.mark.asyncio
    async def test_output_preserved_as_is(self):
        """Output should be preserved exactly as-is, including None."""
        state = make_state({
            "step_a": StepState(status=StepStatus.RUNNING, task_id="task-123"),
        })

        task = MockTask(
            id="task-123",
            status=TaskStatus.Completed,
            output=None,  # None should be preserved, not coerced to {}
        )

        mock_queue = AsyncMock()
        mock_queue.get = AsyncMock(return_value=task)
        mock_queue.load_output = AsyncMock(return_value=None)  # Resolves to None
        mock_queue.now = AsyncMock(return_value="2026-01-28T12:00:00Z")

        completed_state = make_state({
            "step_a": StepState(status=StepStatus.COMPLETED),
        })

        captured_outputs: list[Any] = []

        class MockEngine:
            async def complete_step(
                self, wf_id: str, step: str, output: object, now: str
            ) -> None:
                captured_outputs.append(output)

        async def mock_get_engine() -> MockEngine:
            return MockEngine()

        async def mock_get_state(_queue: MockQueue, wf_id: str) -> tuple[WorkflowState, str]:
            return completed_state, "etag-2"

        with (
            patch("qow.orchestrator.get_engine", side_effect=mock_get_engine),
            patch(
                "qow.orchestrator.get_workflow_state",
                side_effect=mock_get_state,
            ),
        ):
            await _reconcile_running_steps(
                state, mock_queue, "wf-test", "2026-01-28T12:00:00Z"
            )

            # None should be passed as-is
            assert len(captured_outputs) == 1
            assert captured_outputs[0] is None
            # Verify load_output was called with the task
            mock_queue.load_output.assert_called_once_with(task)

    @pytest.mark.asyncio
    async def test_falsy_output_preserved(self):
        """Falsy outputs (empty dict, 0, False) should be preserved."""
        state = make_state({
            "step_a": StepState(status=StepStatus.RUNNING, task_id="task-123"),
        })

        task = MockTask(
            id="task-123",
            status=TaskStatus.Completed,
            output={},  # Empty dict is falsy but should be preserved
        )

        mock_queue = AsyncMock()
        mock_queue.get = AsyncMock(return_value=task)
        mock_queue.load_output = AsyncMock(return_value={})  # Resolves to empty dict
        mock_queue.now = AsyncMock(return_value="2026-01-28T12:00:00Z")

        completed_state = make_state({
            "step_a": StepState(status=StepStatus.COMPLETED),
        })

        captured_outputs: list[Any] = []

        class MockEngine:
            async def complete_step(
                self, wf_id: str, step: str, output: object, now: str
            ) -> None:
                captured_outputs.append(output)

        async def mock_get_engine() -> MockEngine:
            return MockEngine()

        async def mock_get_state(_queue: MockQueue, wf_id: str) -> tuple[WorkflowState, str]:
            return completed_state, "etag-2"

        with (
            patch("qow.orchestrator.get_engine", side_effect=mock_get_engine),
            patch(
                "qow.orchestrator.get_workflow_state",
                side_effect=mock_get_state,
            ),
        ):
            await _reconcile_running_steps(
                state, mock_queue, "wf-test", "2026-01-28T12:00:00Z"
            )

            # Empty dict should be passed as-is
            assert len(captured_outputs) == 1
            assert captured_outputs[0] == {}

    @pytest.mark.asyncio
    async def test_zero_output_preserved(self):
        """Numeric zero output should be preserved."""
        state = make_state({
            "step_a": StepState(status=StepStatus.RUNNING, task_id="task-123"),
        })

        task = MockTask(
            id="task-123",
            status=TaskStatus.Completed,
            output=0,  # Zero is falsy but should be preserved
        )

        mock_queue = AsyncMock()
        mock_queue.get = AsyncMock(return_value=task)
        mock_queue.load_output = AsyncMock(return_value=0)  # Resolves to 0
        mock_queue.now = AsyncMock(return_value="2026-01-28T12:00:00Z")

        completed_state = make_state({
            "step_a": StepState(status=StepStatus.COMPLETED),
        })

        captured_outputs: list[Any] = []

        class MockEngine:
            async def complete_step(
                self, wf_id: str, step: str, output: object, now: str
            ) -> None:
                captured_outputs.append(output)

        async def mock_get_engine() -> MockEngine:
            return MockEngine()

        async def mock_get_state(_queue: MockQueue, wf_id: str) -> tuple[WorkflowState, str]:
            return completed_state, "etag-2"

        with (
            patch("qow.orchestrator.get_engine", side_effect=mock_get_engine),
            patch(
                "qow.orchestrator.get_workflow_state",
                side_effect=mock_get_state,
            ),
        ):
            await _reconcile_running_steps(
                state, mock_queue, "wf-test", "2026-01-28T12:00:00Z"
            )

            assert len(captured_outputs) == 1
            assert captured_outputs[0] == 0

    @pytest.mark.asyncio
    async def test_output_ref_resolved_via_load_output(self):
        """Output with output_ref should be loaded via queue.load_output."""
        state = make_state({
            "step_a": StepState(status=StepStatus.RUNNING, task_id="task-123"),
        })

        # Task with output=None but output_ref pointing to offloaded data
        task = MockTask(
            id="task-123",
            status=TaskStatus.Completed,
            output=None,  # Offloaded - actual output is in S3
        )
        # Simulate output_ref by having load_output return the real data
        offloaded_output = {"large": "payload", "data": list(range(100))}

        mock_queue = AsyncMock()
        mock_queue.get = AsyncMock(return_value=task)
        mock_queue.load_output = AsyncMock(return_value=offloaded_output)
        mock_queue.now = AsyncMock(return_value="2026-01-28T12:00:00Z")

        completed_state = make_state({
            "step_a": StepState(status=StepStatus.COMPLETED),
        })

        captured_outputs: list[Any] = []

        class MockEngine:
            async def complete_step(
                self, wf_id: str, step: str, output: object, now: str
            ) -> None:
                captured_outputs.append(output)

        async def mock_get_engine() -> MockEngine:
            return MockEngine()

        async def mock_get_state(_queue: MockQueue, wf_id: str) -> tuple[WorkflowState, str]:
            return completed_state, "etag-2"

        with (
            patch("qow.orchestrator.get_engine", side_effect=mock_get_engine),
            patch(
                "qow.orchestrator.get_workflow_state",
                side_effect=mock_get_state,
            ),
        ):
            await _reconcile_running_steps(
                state, mock_queue, "wf-test", "2026-01-28T12:00:00Z"
            )

            # The offloaded output should be loaded and passed to complete_step
            assert len(captured_outputs) == 1
            assert captured_outputs[0] == offloaded_output
            # Verify load_output was called (not just task.output accessed)
            mock_queue.load_output.assert_called_once_with(task)


class TestNoStepsAfterFailure:
    """Tests for ensuring no new steps are scheduled after failure detection."""

    @pytest.mark.asyncio
    async def test_no_steps_scheduled_after_failure(self):
        """After detecting a failed step, no new steps should be scheduled."""
        steps = {
            "step_a": StepDef(
                name="step_a",
                handler=dummy_handler,
                depends_on=[],
                on_failure=OnFailure.FAIL_WORKFLOW,
            ),
            "step_b": StepDef(
                name="step_b",
                handler=dummy_handler,
                depends_on=[],  # No dependency on step_a
            ),
        }

        # State where step_a has failed
        state = make_state({
            "step_a": StepState(status=StepStatus.FAILED, error="Some error"),
        })

        wf = Workflow("test")
        wf.steps = steps
        wf._definition_hash = "sha256:test"

        mock_queue = AsyncMock()
        mock_queue.now = AsyncMock(return_value="2026-01-28T12:00:00Z")

        update_calls: list[WorkflowState] = []

        async def mock_update_state(
            _queue: MockQueue, wf_id: str, state: WorkflowState, etag: str
        ) -> str:
            update_calls.append(state)
            return "new-etag"

        with patch(
            "qow.orchestrator.update_workflow_state",
            side_effect=mock_update_state,
        ):
            result = await _check_workflow_terminal_conditions(
                state, wf, mock_queue, "wf-test", "etag-1"
            )

            # Should return failed result
            assert result is not None
            assert result.get("failed") is True

            # State should be updated to FAILED
            assert len(update_calls) == 1
            assert update_calls[0].status == WorkflowStatus.FAILED

    @pytest.mark.asyncio
    async def test_orchestrate_stops_after_reconciliation_failure(self):
        """Orchestrate should stop if reconciliation reveals a failure."""
        wf = Workflow("test")

        @wf.step("step_a", on_failure=OnFailure.FAIL_WORKFLOW)
        async def step_a(_ctx: StepContext) -> dict[str, Any]:
            return {"a": "done"}

        @wf.step("step_b")
        async def step_b(_ctx: StepContext) -> dict[str, Any]:
            return {"b": "done"}

        # Get the actual workflow definition hash to match state
        wf_hash = wf.definition_hash

        # Initial state: step_a is running (hash must match workflow)
        initial_state = WorkflowState(
            id="wf-test",
            type="test",
            definition_hash=wf_hash,
            status=WorkflowStatus.RUNNING,
            current_steps=["step_a"],
            data={},
            steps={"step_a": StepState(status=StepStatus.RUNNING, task_id="task-123")},
            signals={},
            error=None,
            created_at="2026-01-28T12:00:00Z",
            updated_at="2026-01-28T12:00:00Z",
        )

        # After reconciliation, step_a is failed
        failed_state = WorkflowState(
            id="wf-test",
            type="test",
            definition_hash=wf_hash,
            status=WorkflowStatus.RUNNING,
            current_steps=[],
            data={},
            steps={"step_a": StepState(status=StepStatus.FAILED, error="Task failed")},
            signals={},
            error=None,
            created_at="2026-01-28T12:00:00Z",
            updated_at="2026-01-28T12:00:00Z",
        )

        mock_queue = AsyncMock()
        mock_queue.get = AsyncMock(
            return_value=MockTask(
                id="task-123",
                status=TaskStatus.Failed,
                last_error="Task failed",
            )
        )
        mock_queue.now = AsyncMock(return_value="2026-01-28T12:00:00Z")

        call_count = 0

        async def mock_get_state(_queue: MockQueue, wf_id: str) -> tuple[WorkflowState, str]:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return initial_state, "etag-1"
            return failed_state, "etag-2"

        async def mock_handle_failure(
            wf_id: str, step: str, error: str, _queue: MockQueue
        ) -> None:
            pass

        update_calls: list[WorkflowState] = []

        async def mock_update_state(
            _queue: MockQueue, wf_id: str, state: WorkflowState, etag: str
        ) -> str:
            update_calls.append(state)
            return "new-etag"

        with (
            patch(
                "qow.orchestrator.get_workflow_state",
                side_effect=mock_get_state,
            ),
            patch(
                "qow.orchestrator.update_workflow_state",
                side_effect=mock_update_state,
            ),
            patch(
                "qow.orchestrator.handle_step_failure",
                side_effect=mock_handle_failure,
            ),
        ):
            result = await orchestrate(
                {"workflow_id": "wf-test"},
                MagicMock(),
                wf,
                mock_queue,
            )

            # Should return failed, not submit any new steps
            assert result.get("failed") is True
            assert "submitted_steps" not in result

    @pytest.mark.asyncio
    async def test_orchestrate_cancels_workflow_on_cancelled_step(self):
        """Orchestrate should cancel workflow if a step task was cancelled."""
        # WorkflowError imported at top level

        wf = Workflow("test")

        @wf.step("step_a")
        async def step_a(_ctx: StepContext) -> dict[str, Any]:
            return {"a": "done"}

        wf_hash = wf.definition_hash

        initial_state = WorkflowState(
            id="wf-test",
            type="test",
            definition_hash=wf_hash,
            status=WorkflowStatus.RUNNING,
            current_steps=["step_a"],
            data={},
            steps={"step_a": StepState(status=StepStatus.RUNNING, task_id="task-123")},
            signals={},
            error=None,
            created_at="2026-01-28T12:00:00Z",
            updated_at="2026-01-28T12:00:00Z",
        )

        cancelled_state = WorkflowState(
            id="wf-test",
            type="test",
            definition_hash=wf_hash,
            status=WorkflowStatus.RUNNING,
            current_steps=[],
            data={},
            steps={"step_a": StepState(status=StepStatus.CANCELLED)},
            signals={},
            error=None,
            created_at="2026-01-28T12:00:00Z",
            updated_at="2026-01-28T12:00:00Z",
        )

        mock_queue = AsyncMock()
        mock_queue.get = AsyncMock(
            return_value=MockTask(
                id="task-123",
                status=TaskStatus.Cancelled,
                last_error="User requested cancellation",
            )
        )
        mock_queue.now = AsyncMock(return_value="2026-01-28T12:00:00Z")

        call_count = 0

        async def mock_get_state(_queue: MockQueue, wf_id: str) -> tuple[WorkflowState, str]:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return initial_state, "etag-1"
            return cancelled_state, "etag-2"

        async def mock_handle_cancellation(
            wf_id: str, step: str, reason: str, _queue: MockQueue
        ) -> None:
            pass

        update_calls: list[WorkflowState] = []

        async def mock_update_state(
            _queue: MockQueue, wf_id: str, state: WorkflowState, etag: str
        ) -> str:
            update_calls.append(state)
            return "new-etag"

        with (
            patch(
                "qow.orchestrator.get_workflow_state",
                side_effect=mock_get_state,
            ),
            patch(
                "qow.orchestrator.update_workflow_state",
                side_effect=mock_update_state,
            ),
            patch(
                "qow.orchestrator.handle_step_cancellation",
                side_effect=mock_handle_cancellation,
            ),
        ):
            result = await orchestrate(
                {"workflow_id": "wf-test"},
                MagicMock(),
                wf,
                mock_queue,
            )

            # Should return cancelled
            assert result.get("cancelled") is True
            assert "submitted_steps" not in result

            # Workflow state should be updated to CANCELLED with the actual reason
            assert len(update_calls) == 1
            assert update_calls[0].status == WorkflowStatus.CANCELLED
            assert update_calls[0].error is not None
            assert update_calls[0].error.type == "Cancelled"
            assert update_calls[0].error.message == "User requested cancellation"
