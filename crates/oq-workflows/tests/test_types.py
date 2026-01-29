"""Tests for oq-workflows type serialization."""
from __future__ import annotations

from oq_workflows import (
    OnFailure,
    Signal,
    SignalCursor,
    StepContext,
    StepDef,
    StepState,
    StepStatus,
    WorkflowError,
    WorkflowRun,
    WorkflowState,
    WorkflowStatus,
    compute_definition_hash,
)


class TestWorkflowStatus:
    """Tests for WorkflowStatus enum."""

    def test_all_statuses_exist(self):
        """All documented statuses should exist."""
        assert WorkflowStatus.PENDING == "pending"
        assert WorkflowStatus.RUNNING == "running"
        assert WorkflowStatus.WAITING_SIGNAL == "waiting_signal"
        assert WorkflowStatus.COMPENSATING == "compensating"
        assert WorkflowStatus.PAUSED == "paused"
        assert WorkflowStatus.COMPLETED == "completed"
        assert WorkflowStatus.FAILED == "failed"
        assert WorkflowStatus.CANCELLED == "cancelled"

    def test_status_is_string(self):
        """Status values should be strings for JSON serialization."""
        assert isinstance(WorkflowStatus.RUNNING.value, str)


class TestStepStatus:
    """Tests for StepStatus enum."""

    def test_all_statuses_exist(self):
        """All documented statuses should exist."""
        assert StepStatus.PENDING == "pending"
        assert StepStatus.RUNNING == "running"
        assert StepStatus.COMPLETED == "completed"
        assert StepStatus.FAILED == "failed"
        assert StepStatus.CANCELLED == "cancelled"


class TestOnFailure:
    """Tests for OnFailure enum."""

    def test_all_policies_exist(self):
        """All failure policies should exist."""
        assert OnFailure.FAIL_WORKFLOW == "fail_workflow"
        assert OnFailure.PAUSE_WORKFLOW == "pause_workflow"
        assert OnFailure.CONTINUE == "continue"
        assert OnFailure.COMPENSATE == "compensate"


class TestWorkflowError:
    """Tests for WorkflowError dataclass."""

    def test_to_dict_minimal(self):
        """Minimal error should serialize correctly."""
        error = WorkflowError(type="TestError", message="Test message")
        d = error.to_dict()

        assert d["type"] == "TestError"
        assert d["message"] == "Test message"
        assert d["step"] is None
        assert d["attempt"] is None
        assert d["at"] is None

    def test_to_dict_full(self):
        """Full error should serialize correctly."""
        error = WorkflowError(
            type="StepFailed",
            message="Step timed out",
            step="process_data",
            attempt=3,
            at="2026-01-28T12:00:00Z",
        )
        d = error.to_dict()

        assert d["type"] == "StepFailed"
        assert d["message"] == "Step timed out"
        assert d["step"] == "process_data"
        assert d["attempt"] == 3
        assert d["at"] == "2026-01-28T12:00:00Z"

    def test_from_dict(self):
        """Should deserialize from dict."""
        d = {
            "type": "VersionMismatch",
            "message": "Definition changed",
            "step": None,
            "attempt": None,
            "at": None,
        }
        error = WorkflowError.from_dict(d)

        assert error.type == "VersionMismatch"
        assert error.message == "Definition changed"


class TestStepState:
    """Tests for StepState dataclass."""

    def test_default_values(self):
        """Default values should be set correctly."""
        state = StepState()

        assert state.status == StepStatus.PENDING
        assert state.started_at is None
        assert state.completed_at is None
        assert state.result is None
        assert state.error is None
        assert state.attempt == 0
        assert state.task_id is None

    def test_task_id_roundtrip(self):
        """task_id should survive serialization roundtrip."""
        state = StepState(
            status=StepStatus.RUNNING,
            started_at="2026-01-28T12:00:00Z",
            attempt=1,
            task_id="task-abc123",
        )
        d = state.to_dict()
        assert d["task_id"] == "task-abc123"

        restored = StepState.from_dict(d)
        assert restored.task_id == "task-abc123"

    def test_task_id_none_backwards_compat(self):
        """Missing task_id in dict should default to None."""
        d = {
            "status": "running",
            "started_at": "2026-01-28T12:00:00Z",
            "attempt": 1,
        }
        state = StepState.from_dict(d)
        assert state.task_id is None

    def test_to_dict(self):
        """Should serialize to dict."""
        state = StepState(
            status=StepStatus.COMPLETED,
            started_at="2026-01-28T12:00:00Z",
            completed_at="2026-01-28T12:01:00Z",
            result={"processed": True},
            attempt=1,
        )
        d = state.to_dict()

        assert d["status"] == "completed"
        assert d["started_at"] == "2026-01-28T12:00:00Z"
        assert d["completed_at"] == "2026-01-28T12:01:00Z"
        assert d["result"] == {"processed": True}
        assert d["attempt"] == 1

    def test_from_dict(self):
        """Should deserialize from dict."""
        d = {
            "status": "running",
            "started_at": "2026-01-28T12:00:00Z",
            "completed_at": None,
            "result": None,
            "error": None,
            "attempt": 2,
        }
        state = StepState.from_dict(d)

        assert state.status == StepStatus.RUNNING
        assert state.started_at == "2026-01-28T12:00:00Z"
        assert state.attempt == 2


class TestSignalCursor:
    """Tests for SignalCursor dataclass."""

    def test_default_cursor(self):
        """Default cursor should be None."""
        cursor = SignalCursor()
        assert cursor.cursor is None

    def test_to_dict(self):
        """Should serialize to dict."""
        cursor = SignalCursor(cursor="2026-01-28T12:00:00Z_abc123")
        d = cursor.to_dict()

        assert d["cursor"] == "2026-01-28T12:00:00Z_abc123"

    def test_from_dict(self):
        """Should deserialize from dict."""
        d = {"cursor": "2026-01-28T12:00:00Z_def456"}
        cursor = SignalCursor.from_dict(d)

        assert cursor.cursor == "2026-01-28T12:00:00Z_def456"


class TestWorkflowState:
    """Tests for WorkflowState dataclass."""

    def test_to_dict(self):
        """Should serialize complete state to dict."""
        state = WorkflowState(
            id="wf-123",
            type="order_fulfillment",
            definition_hash="sha256:abc123",
            status=WorkflowStatus.RUNNING,
            current_steps=["process"],
            data={"order_id": "ORD-123"},
            steps={
                "validate": StepState(status=StepStatus.COMPLETED, result={"valid": True}),
                "process": StepState(status=StepStatus.RUNNING, started_at="2026-01-28T12:00:00Z"),
            },
            signals={
                "approval": SignalCursor(cursor="2026-01-28T11:00:00Z_xyz"),
            },
            error=None,
            created_at="2026-01-28T11:55:00Z",
            updated_at="2026-01-28T12:00:00Z",
            orchestrator_task_id="task-xyz",
        )
        d = state.to_dict()

        assert d["id"] == "wf-123"
        assert d["type"] == "order_fulfillment"
        assert d["status"] == "running"
        assert d["current_steps"] == ["process"]
        assert d["data"]["order_id"] == "ORD-123"
        assert d["steps"]["validate"]["status"] == "completed"
        assert d["signals"]["approval"]["cursor"] == "2026-01-28T11:00:00Z_xyz"
        assert d["orchestrator_task_id"] == "task-xyz"

    def test_from_dict(self):
        """Should deserialize from dict."""
        d = {
            "id": "wf-456",
            "type": "test_workflow",
            "definition_hash": "sha256:def456",
            "status": "completed",
            "current_steps": [],
            "data": {"key": "value"},
            "steps": {},
            "signals": {},
            "error": None,
            "created_at": "2026-01-28T10:00:00Z",
            "updated_at": "2026-01-28T11:00:00Z",
        }
        state = WorkflowState.from_dict(d)

        assert state.id == "wf-456"
        assert state.type == "test_workflow"
        assert state.status == WorkflowStatus.COMPLETED
        assert state.data == {"key": "value"}

    def test_json_roundtrip(self):
        """Should survive JSON serialization roundtrip."""
        original = WorkflowState(
            id="wf-roundtrip",
            type="test",
            definition_hash="sha256:test",
            status=WorkflowStatus.RUNNING,
            current_steps=["step1"],
            data={"nested": {"data": [1, 2, 3]}},
            steps={"step1": StepState(status=StepStatus.RUNNING)},
            signals={},
            error=None,
            created_at="2026-01-28T12:00:00Z",
            updated_at="2026-01-28T12:00:00Z",
        )

        json_bytes = original.to_json()
        restored = WorkflowState.from_json(json_bytes)

        assert restored.id == original.id
        assert restored.status == original.status
        assert restored.data == original.data
        assert restored.steps["step1"].status == original.steps["step1"].status


class TestSignal:
    """Tests for Signal dataclass."""

    def test_to_dict(self):
        """Should serialize to dict."""
        signal = Signal(
            id="sig-123",
            name="approval",
            payload={"approved": True, "by": "manager@example.com"},
            created_at="2026-01-28T12:00:00Z",
        )
        d = signal.to_dict()

        assert d["id"] == "sig-123"
        assert d["name"] == "approval"
        assert d["payload"]["approved"] is True
        assert d["created_at"] == "2026-01-28T12:00:00Z"

    def test_json_roundtrip(self):
        """Should survive JSON roundtrip."""
        original = Signal(
            id="sig-roundtrip",
            name="test",
            payload={"data": [1, 2, 3]},
            created_at="2026-01-28T12:00:00Z",
        )

        json_bytes = original.to_json()
        restored = Signal.from_json(json_bytes)

        assert restored.id == original.id
        assert restored.payload == original.payload


class TestComputeDefinitionHash:
    """Tests for definition hash computation."""

    def test_empty_workflow(self):
        """Empty workflow should have a hash."""
        hash_value = compute_definition_hash({})
        assert hash_value.startswith("sha256:")

    def test_deterministic(self):
        """Same input should produce same hash."""
        async def handler(_ctx: StepContext) -> None:
            pass

        steps = {
            "a": StepDef(name="a", handler=handler, depends_on=[]),
            "b": StepDef(name="b", handler=handler, depends_on=["a"]),
        }

        hash1 = compute_definition_hash(steps)
        hash2 = compute_definition_hash(steps)

        assert hash1 == hash2

    def test_different_steps_different_hash(self):
        """Different steps should produce different hashes."""
        async def handler(_ctx: StepContext) -> None:
            pass

        steps1 = {"a": StepDef(name="a", handler=handler, depends_on=[])}
        steps2 = {"b": StepDef(name="b", handler=handler, depends_on=[])}

        hash1 = compute_definition_hash(steps1)
        hash2 = compute_definition_hash(steps2)

        assert hash1 != hash2

    def test_different_deps_different_hash(self):
        """Different dependencies should produce different hashes."""
        async def handler(_ctx: StepContext) -> None:
            pass

        steps1 = {
            "a": StepDef(name="a", handler=handler, depends_on=[]),
            "b": StepDef(name="b", handler=handler, depends_on=[]),
        }
        steps2 = {
            "a": StepDef(name="a", handler=handler, depends_on=[]),
            "b": StepDef(name="b", handler=handler, depends_on=["a"]),
        }

        hash1 = compute_definition_hash(steps1)
        hash2 = compute_definition_hash(steps2)

        assert hash1 != hash2

    def test_order_independent(self):
        """Step order should not affect hash."""
        async def handler(_ctx: StepContext) -> None:
            pass

        # Add steps in different orders
        steps1 = {}
        steps1["a"] = StepDef(name="a", handler=handler, depends_on=[])
        steps1["b"] = StepDef(name="b", handler=handler, depends_on=["a"])

        steps2 = {}
        steps2["b"] = StepDef(name="b", handler=handler, depends_on=["a"])
        steps2["a"] = StepDef(name="a", handler=handler, depends_on=[])

        hash1 = compute_definition_hash(steps1)
        hash2 = compute_definition_hash(steps2)

        assert hash1 == hash2


class TestWorkflowRun:
    """Tests for WorkflowRun dataclass."""

    def test_minimal(self):
        """Minimal run should work."""
        run = WorkflowRun(id="wf-123", status=WorkflowStatus.RUNNING)

        assert run.id == "wf-123"
        assert run.status == WorkflowStatus.RUNNING
        assert run.type is None
        assert run.data is None

    def test_full(self):
        """Full run should work."""
        run = WorkflowRun(
            id="wf-123",
            status=WorkflowStatus.COMPLETED,
            type="order",
            data={"order_id": "ORD-123"},
            current_steps=[],
            error=None,
        )

        assert run.type == "order"
        assert run.data["order_id"] == "ORD-123"
