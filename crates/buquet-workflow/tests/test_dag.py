"""Tests for DAG computation logic."""

from __future__ import annotations

import pytest

from buquet import (
    OnFailure,
    StepContext,
    StepDef,
    StepState,
    StepStatus,
    Workflow,
    WorkflowState,
    WorkflowStatus,
    compute_ready_steps,
    get_blocked_steps,
    get_compensation_chain,
    has_unrecoverable_failure,
    is_workflow_complete,
    should_pause_workflow,
    topological_sort,
    validate_dag,
)


def make_state(steps: dict[str, StepState] | None = None) -> WorkflowState:
    """Create a minimal workflow state for testing."""
    return WorkflowState(
        id="wf-test",
        type="test",
        definition_hash="sha256:test",
        status=WorkflowStatus.RUNNING,
        current_steps=[],
        data={},
        steps=steps or {},
        signals={},
        error=None,
        created_at="2026-01-28T12:00:00Z",
        updated_at="2026-01-28T12:00:00Z",
    )


async def dummy_handler(ctx: StepContext) -> None:
    pass


class TestComputeReadySteps:
    """Tests for compute_ready_steps function."""

    def test_no_steps(self):
        """Empty workflow has no ready steps."""
        ready = compute_ready_steps({}, make_state())
        assert ready == []

    def test_single_step_no_deps(self):
        """Single step with no deps is ready."""
        steps = {"a": StepDef(name="a", handler=dummy_handler, depends_on=[])}
        ready = compute_ready_steps(steps, make_state())

        assert ready == ["a"]

    def test_step_with_unmet_deps(self):
        """Step with unmet deps is not ready."""
        steps = {
            "a": StepDef(name="a", handler=dummy_handler, depends_on=[]),
            "b": StepDef(name="b", handler=dummy_handler, depends_on=["a"]),
        }
        ready = compute_ready_steps(steps, make_state())

        assert ready == ["a"]
        assert "b" not in ready

    def test_step_with_met_deps(self):
        """Step with met deps is ready."""
        steps = {
            "a": StepDef(name="a", handler=dummy_handler, depends_on=[]),
            "b": StepDef(name="b", handler=dummy_handler, depends_on=["a"]),
        }
        state = make_state({"a": StepState(status=StepStatus.COMPLETED)})
        ready = compute_ready_steps(steps, state)

        assert ready == ["b"]

    def test_running_step_not_ready(self):
        """Already running step is not ready."""
        steps = {"a": StepDef(name="a", handler=dummy_handler, depends_on=[])}
        state = make_state({"a": StepState(status=StepStatus.RUNNING)})
        ready = compute_ready_steps(steps, state)

        assert ready == []

    def test_completed_step_not_ready(self):
        """Completed step is not ready."""
        steps = {"a": StepDef(name="a", handler=dummy_handler, depends_on=[])}
        state = make_state({"a": StepState(status=StepStatus.COMPLETED)})
        ready = compute_ready_steps(steps, state)

        assert ready == []

    def test_parallel_steps(self):
        """Multiple steps with no deps are all ready."""
        steps = {
            "a": StepDef(name="a", handler=dummy_handler, depends_on=[]),
            "b": StepDef(name="b", handler=dummy_handler, depends_on=[]),
            "c": StepDef(name="c", handler=dummy_handler, depends_on=[]),
        }
        ready = compute_ready_steps(steps, make_state())

        assert set(ready) == {"a", "b", "c"}

    def test_diamond_dag(self):
        """Diamond DAG computes correctly."""
        steps = {
            "start": StepDef(name="start", handler=dummy_handler, depends_on=[]),
            "left": StepDef(name="left", handler=dummy_handler, depends_on=["start"]),
            "right": StepDef(name="right", handler=dummy_handler, depends_on=["start"]),
            "end": StepDef(name="end", handler=dummy_handler, depends_on=["left", "right"]),
        }

        # Initially only start is ready
        state = make_state()
        assert compute_ready_steps(steps, state) == ["start"]

        # After start, left and right are ready
        state = make_state({"start": StepState(status=StepStatus.COMPLETED)})
        ready = compute_ready_steps(steps, state)
        assert set(ready) == {"left", "right"}

        # After left, still waiting for right
        state = make_state({
            "start": StepState(status=StepStatus.COMPLETED),
            "left": StepState(status=StepStatus.COMPLETED),
        })
        assert compute_ready_steps(steps, state) == ["right"]

        # After both, end is ready
        state = make_state({
            "start": StepState(status=StepStatus.COMPLETED),
            "left": StepState(status=StepStatus.COMPLETED),
            "right": StepState(status=StepStatus.COMPLETED),
        })
        assert compute_ready_steps(steps, state) == ["end"]


class TestIsWorkflowComplete:
    """Tests for is_workflow_complete function."""

    def test_empty_workflow_is_complete(self):
        """Workflow with no steps is complete."""
        assert is_workflow_complete({}, make_state()) is True

    def test_pending_step_not_complete(self):
        """Workflow with pending step is not complete."""
        steps = {"a": StepDef(name="a", handler=dummy_handler, depends_on=[])}
        assert is_workflow_complete(steps, make_state()) is False

    def test_running_step_not_complete(self):
        """Workflow with running step is not complete."""
        steps = {"a": StepDef(name="a", handler=dummy_handler, depends_on=[])}
        state = make_state({"a": StepState(status=StepStatus.RUNNING)})
        assert is_workflow_complete(steps, state) is False

    def test_all_completed_is_complete(self):
        """Workflow with all completed steps is complete."""
        steps = {
            "a": StepDef(name="a", handler=dummy_handler, depends_on=[]),
            "b": StepDef(name="b", handler=dummy_handler, depends_on=["a"]),
        }
        state = make_state({
            "a": StepState(status=StepStatus.COMPLETED),
            "b": StepState(status=StepStatus.COMPLETED),
        })
        assert is_workflow_complete(steps, state) is True

    def test_partial_completion_not_complete(self):
        """Workflow with some completed steps is not complete."""
        steps = {
            "a": StepDef(name="a", handler=dummy_handler, depends_on=[]),
            "b": StepDef(name="b", handler=dummy_handler, depends_on=["a"]),
        }
        state = make_state({"a": StepState(status=StepStatus.COMPLETED)})
        assert is_workflow_complete(steps, state) is False


class TestHasUnrecoverableFailure:
    """Tests for has_unrecoverable_failure function."""

    def test_no_failures(self):
        """No failures means no unrecoverable failure."""
        steps = {"a": StepDef(name="a", handler=dummy_handler, depends_on=[])}
        state = make_state({"a": StepState(status=StepStatus.COMPLETED)})
        assert has_unrecoverable_failure(steps, state) is False

    def test_failed_with_fail_workflow(self):
        """Failed step with FAIL_WORKFLOW is unrecoverable."""
        steps = {
            "a": StepDef(
                name="a",
                handler=dummy_handler,
                depends_on=[],
                on_failure=OnFailure.FAIL_WORKFLOW,
            ),
        }
        state = make_state({"a": StepState(status=StepStatus.FAILED)})
        assert has_unrecoverable_failure(steps, state) is True

    def test_failed_with_compensate(self):
        """Failed step with COMPENSATE is unrecoverable (triggers compensation)."""
        steps = {
            "a": StepDef(
                name="a",
                handler=dummy_handler,
                depends_on=[],
                on_failure=OnFailure.COMPENSATE,
            ),
        }
        state = make_state({"a": StepState(status=StepStatus.FAILED)})
        assert has_unrecoverable_failure(steps, state) is True

    def test_failed_with_continue_no_dependents(self):
        """Failed step with CONTINUE and no dependents is not blocking."""
        steps = {
            "a": StepDef(
                name="a",
                handler=dummy_handler,
                depends_on=[],
                on_failure=OnFailure.CONTINUE,
            ),
        }
        state = make_state({"a": StepState(status=StepStatus.FAILED)})
        # No other steps depend on 'a', so we can continue
        assert has_unrecoverable_failure(steps, state) is False

    def test_failed_with_continue_has_dependents(self):
        """Failed step with CONTINUE but has dependents is blocking."""
        steps = {
            "a": StepDef(
                name="a",
                handler=dummy_handler,
                depends_on=[],
                on_failure=OnFailure.CONTINUE,
            ),
            "b": StepDef(name="b", handler=dummy_handler, depends_on=["a"]),
        }
        state = make_state({"a": StepState(status=StepStatus.FAILED)})
        assert has_unrecoverable_failure(steps, state) is True


class TestShouldPauseWorkflow:
    """Tests for should_pause_workflow function."""

    def test_no_pause_needed(self):
        """No pause if no failures with PAUSE policy."""
        steps = {"a": StepDef(name="a", handler=dummy_handler, depends_on=[])}
        state = make_state({"a": StepState(status=StepStatus.COMPLETED)})
        assert should_pause_workflow(steps, state) is False

    def test_pause_on_pause_policy(self):
        """Pause if step failed with PAUSE_WORKFLOW policy."""
        steps = {
            "a": StepDef(
                name="a",
                handler=dummy_handler,
                depends_on=[],
                on_failure=OnFailure.PAUSE_WORKFLOW,
            ),
        }
        state = make_state({"a": StepState(status=StepStatus.FAILED)})
        assert should_pause_workflow(steps, state) is True

    def test_no_pause_on_fail_policy(self):
        """No pause if step failed with FAIL_WORKFLOW policy."""
        steps = {
            "a": StepDef(
                name="a",
                handler=dummy_handler,
                depends_on=[],
                on_failure=OnFailure.FAIL_WORKFLOW,
            ),
        }
        state = make_state({"a": StepState(status=StepStatus.FAILED)})
        assert should_pause_workflow(steps, state) is False


class TestGetCompensationChain:
    """Tests for get_compensation_chain function."""

    def test_no_compensation_handlers(self):
        """Empty chain if no compensation handlers."""
        steps = {"a": StepDef(name="a", handler=dummy_handler, depends_on=[])}
        state = make_state({"a": StepState(status=StepStatus.COMPLETED)})
        chain = get_compensation_chain(steps, state)
        assert chain == []

    def test_compensation_chain_order(self):
        """Chain should be in reverse completion order."""

        async def compensate(ctx: StepContext, result: dict[str, object]) -> None:
            pass

        steps = {
            "a": StepDef(
                name="a",
                handler=dummy_handler,
                depends_on=[],
                compensation=compensate,
            ),
            "b": StepDef(
                name="b",
                handler=dummy_handler,
                depends_on=["a"],
                compensation=compensate,
            ),
            "c": StepDef(
                name="c",
                handler=dummy_handler,
                depends_on=["b"],
                compensation=compensate,
            ),
        }
        state = make_state({
            "a": StepState(status=StepStatus.COMPLETED, completed_at="2026-01-28T12:00:00Z"),
            "b": StepState(status=StepStatus.COMPLETED, completed_at="2026-01-28T12:01:00Z"),
            "c": StepState(status=StepStatus.FAILED),  # This one failed, no compensation
        })
        chain = get_compensation_chain(steps, state)

        # Should be b, then a (reverse order)
        assert chain == ["b", "a"]


class TestGetBlockedSteps:
    """Tests for get_blocked_steps function."""

    def test_no_blocked_steps(self):
        """No blocked steps if no failures."""
        steps = {
            "a": StepDef(name="a", handler=dummy_handler, depends_on=[]),
            "b": StepDef(name="b", handler=dummy_handler, depends_on=["a"]),
        }
        state = make_state({"a": StepState(status=StepStatus.COMPLETED)})
        blocked = get_blocked_steps(steps, state)
        assert blocked == []

    def test_blocked_by_failed_dep(self):
        """Step is blocked if its dependency failed."""
        steps = {
            "a": StepDef(name="a", handler=dummy_handler, depends_on=[]),
            "b": StepDef(name="b", handler=dummy_handler, depends_on=["a"]),
        }
        state = make_state({"a": StepState(status=StepStatus.FAILED)})
        blocked = get_blocked_steps(steps, state)
        assert blocked == ["b"]


class TestTopologicalSort:
    """Tests for topological_sort function."""

    def test_empty(self):
        """Empty workflow sorts to empty list."""
        assert topological_sort({}) == []

    def test_single_step(self):
        """Single step sorts correctly."""
        steps = {"a": StepDef(name="a", handler=dummy_handler, depends_on=[])}
        assert topological_sort(steps) == ["a"]

    def test_linear_chain(self):
        """Linear chain sorts in order."""
        steps = {
            "a": StepDef(name="a", handler=dummy_handler, depends_on=[]),
            "b": StepDef(name="b", handler=dummy_handler, depends_on=["a"]),
            "c": StepDef(name="c", handler=dummy_handler, depends_on=["b"]),
        }
        result = topological_sort(steps)

        # a must come before b, b must come before c
        assert result.index("a") < result.index("b")
        assert result.index("b") < result.index("c")

    def test_diamond(self):
        """Diamond DAG sorts correctly."""
        steps = {
            "start": StepDef(name="start", handler=dummy_handler, depends_on=[]),
            "left": StepDef(name="left", handler=dummy_handler, depends_on=["start"]),
            "right": StepDef(name="right", handler=dummy_handler, depends_on=["start"]),
            "end": StepDef(name="end", handler=dummy_handler, depends_on=["left", "right"]),
        }
        result = topological_sort(steps)

        assert result.index("start") < result.index("left")
        assert result.index("start") < result.index("right")
        assert result.index("left") < result.index("end")
        assert result.index("right") < result.index("end")

    def test_cycle_detection(self):
        """Cycle should raise ValueError."""
        steps = {
            "a": StepDef(name="a", handler=dummy_handler, depends_on=["b"]),
            "b": StepDef(name="b", handler=dummy_handler, depends_on=["a"]),
        }
        with pytest.raises(ValueError, match="Cycle"):
            topological_sort(steps)

    def test_missing_dependency(self):
        """Missing dependency should raise ValueError."""
        steps = {
            "a": StepDef(name="a", handler=dummy_handler, depends_on=["nonexistent"]),
        }
        with pytest.raises(ValueError, match="unknown step"):
            topological_sort(steps)


class TestValidateDag:
    """Tests for validate_dag function."""

    def test_valid_dag(self):
        """Valid DAG returns empty error list."""
        steps = {
            "a": StepDef(name="a", handler=dummy_handler, depends_on=[]),
            "b": StepDef(name="b", handler=dummy_handler, depends_on=["a"]),
        }
        errors = validate_dag(steps)
        assert errors == []

    def test_missing_dependency(self):
        """Missing dependency returns error."""
        steps = {
            "a": StepDef(name="a", handler=dummy_handler, depends_on=["nonexistent"]),
        }
        errors = validate_dag(steps)
        assert len(errors) == 1
        assert "unknown step" in errors[0]

    def test_self_dependency(self):
        """Self-dependency returns error."""
        steps = {
            "a": StepDef(name="a", handler=dummy_handler, depends_on=["a"]),
        }
        errors = validate_dag(steps)
        assert len(errors) == 1
        assert "cannot depend on itself" in errors[0]

    def test_cycle(self):
        """Cycle returns error."""
        steps = {
            "a": StepDef(name="a", handler=dummy_handler, depends_on=["b"]),
            "b": StepDef(name="b", handler=dummy_handler, depends_on=["a"]),
        }
        errors = validate_dag(steps)
        assert len(errors) == 1
        assert "Cycle" in errors[0]


class TestWorkflowIntegration:
    """Integration tests using the Workflow class."""

    def test_workflow_definition_hash(self, sample_workflow: Workflow) -> None:
        """Workflow should compute definition hash."""
        assert sample_workflow.definition_hash.startswith("sha256:")

    def test_workflow_validation(self, sample_workflow: Workflow) -> None:
        """Valid workflow should pass validation."""
        errors = sample_workflow.validate()
        assert errors == []

    def test_diamond_workflow(self, diamond_workflow: Workflow) -> None:
        """Diamond workflow should be valid."""
        errors = diamond_workflow.validate()
        assert errors == []

    def test_workflow_steps(self, linear_workflow: Workflow) -> None:
        """Workflow should have correct steps."""
        assert len(linear_workflow.steps) == 3
        assert "step_1" in linear_workflow.steps
        assert "step_2" in linear_workflow.steps
        assert "step_3" in linear_workflow.steps

    def test_workflow_get_step(self, sample_workflow: Workflow) -> None:
        """Should retrieve step by name."""
        step = sample_workflow.get_step("step_a")
        assert step is not None
        assert step.name == "step_a"

        missing = sample_workflow.get_step("nonexistent")
        assert missing is None
