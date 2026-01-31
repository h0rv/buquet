"""Core types for qow."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, Protocol, cast

from qow._qow import StepState as RustStepState
from qow._qow import StepStatus as RustStepStatus
from qow._qow import WorkflowErrorInfo as RustError
from qow._qow import WorkflowState as RustWorkflowState
from qow._qow import WorkflowStatus as RustWorkflowStatus

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine


class TaskContext(Protocol):
    """Protocol for task context from the qo native module."""

    async def extend_lease(self, additional_secs: int) -> None:
        """Extend the lease on the current task."""
        ...

    async def is_cancellation_requested(self) -> bool:
        """Check if cancellation has been requested."""
        ...


class WorkflowStatus(str, Enum):
    """Status of a workflow instance."""

    PENDING = "pending"
    RUNNING = "running"
    WAITING_SIGNAL = "waiting_signal"
    COMPENSATING = "compensating"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class StepStatus(str, Enum):
    """Status of a workflow step."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class OnFailure(str, Enum):
    """Action to take when a step fails."""

    FAIL_WORKFLOW = "fail_workflow"
    PAUSE_WORKFLOW = "pause_workflow"
    CONTINUE = "continue"
    COMPENSATE = "compensate"


@dataclass
class WorkflowError:
    """Error information for a workflow."""

    type: str
    message: str
    step: str | None = None
    attempt: int | None = None
    at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "type": self.type,
            "message": self.message,
            "step": self.step,
            "attempt": self.attempt,
            "at": self.at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> WorkflowError:
        """Create from dictionary."""
        return cls(
            type=data["type"],
            message=data["message"],
            step=data.get("step"),
            attempt=data.get("attempt"),
            at=data.get("at"),
        )


@dataclass
class SignalCursor:
    """Cursor for signal consumption."""

    cursor: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {"cursor": self.cursor}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SignalCursor:
        """Create from dictionary."""
        return cls(cursor=data.get("cursor"))


@dataclass
class StepState:
    """State of a workflow step."""

    status: StepStatus = StepStatus.PENDING
    started_at: str | None = None
    completed_at: str | None = None
    result: Any | None = None
    error: str | None = None
    attempt: int = 0
    task_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "status": self.status.value,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "result": self.result,
            "error": self.error,
            "attempt": self.attempt,
            "task_id": self.task_id,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> StepState:
        """Create from dictionary."""
        return cls(
            status=StepStatus(data["status"]),
            started_at=data.get("started_at"),
            completed_at=data.get("completed_at"),
            result=data.get("result"),
            error=data.get("error"),
            attempt=data.get("attempt", 0),
            task_id=data.get("task_id"),
        )

    @classmethod
    def from_rust(cls, rust_state: RustStepState) -> StepState:
        """Create from Rust PyStepState."""
        return cls(
            status=StepStatus(str(rust_state.status)),
            started_at=rust_state.started_at,
            completed_at=rust_state.completed_at,
            result=rust_state.result,
            error=rust_state.error,
            attempt=rust_state.attempt,
            task_id=rust_state.task_id,
        )

    def to_rust(self) -> RustStepState:
        """Convert to Rust PyStepState."""
        # Map Python status to Rust status
        status_map = {
            StepStatus.PENDING: RustStepStatus.Pending,
            StepStatus.RUNNING: RustStepStatus.Running,
            StepStatus.COMPLETED: RustStepStatus.Completed,
            StepStatus.FAILED: RustStepStatus.Failed,
            StepStatus.CANCELLED: RustStepStatus.Cancelled,
        }

        return RustStepState(
            status=status_map[self.status],
            started_at=self.started_at,
            completed_at=self.completed_at,
            result=self.result,
            error=self.error,
            attempt=self.attempt,
            task_id=self.task_id,
        )


@dataclass
class WorkflowState:
    """Full state of a workflow instance."""

    id: str
    type: str
    definition_hash: str
    status: WorkflowStatus
    current_steps: list[str]
    data: dict[str, Any]
    steps: dict[str, StepState]
    signals: dict[str, SignalCursor]
    error: WorkflowError | None
    created_at: str
    updated_at: str
    orchestrator_task_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "id": self.id,
            "type": self.type,
            "definition_hash": self.definition_hash,
            "status": self.status.value,
            "current_steps": self.current_steps,
            "data": self.data,
            "steps": {name: step.to_dict() for name, step in self.steps.items()},
            "signals": {name: cursor.to_dict() for name, cursor in self.signals.items()},
            "error": self.error.to_dict() if self.error else None,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "orchestrator_task_id": self.orchestrator_task_id,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> WorkflowState:
        """Create from dictionary."""
        return cls(
            id=data["id"],
            type=data["type"],
            definition_hash=data["definition_hash"],
            status=WorkflowStatus(data["status"]),
            current_steps=data.get("current_steps", []),
            data=data.get("data", {}),
            steps={name: StepState.from_dict(step) for name, step in data.get("steps", {}).items()},
            signals={
                name: SignalCursor.from_dict(cursor)
                for name, cursor in data.get("signals", {}).items()
            },
            error=WorkflowError.from_dict(data["error"]) if data.get("error") else None,
            created_at=data["created_at"],
            updated_at=data["updated_at"],
            orchestrator_task_id=data.get("orchestrator_task_id"),
        )

    def to_json(self) -> bytes:
        """Serialize to JSON bytes."""
        return json.dumps(self.to_dict()).encode("utf-8")

    @classmethod
    def from_json(cls, data: bytes) -> WorkflowState:
        """Deserialize from JSON bytes."""
        return cls.from_dict(json.loads(data))

    @classmethod
    def from_rust(cls, rust_state: RustWorkflowState) -> WorkflowState:
        """Create from Rust PyWorkflowState."""
        # Convert error if present
        error = None
        if rust_state.error is not None:
            rust_error = rust_state.error
            error = WorkflowError(
                type=rust_error.type_,
                message=rust_error.message,
                step=rust_error.step,
                attempt=rust_error.attempt,
                at=rust_error.at,
            )

        # Convert steps
        steps: dict[str, StepState] = {}
        rust_steps = rust_state.steps
        for name in rust_steps:
            rust_step = rust_steps[name]
            steps[str(name)] = StepState.from_rust(rust_step)

        # Convert signals
        signals: dict[str, SignalCursor] = {}
        rust_signals = rust_state.signals
        for name in rust_signals:
            rust_cursor = rust_signals[name]
            signals[str(name)] = SignalCursor(cursor=rust_cursor.cursor)

        return cls(
            id=rust_state.id,
            type=rust_state.workflow_type,
            definition_hash=rust_state.definition_hash,
            status=WorkflowStatus(str(rust_state.status)),
            current_steps=list(rust_state.current_steps),
            data=cast("dict[str, Any]", rust_state.data),
            steps=steps,
            signals=signals,
            error=error,
            created_at=rust_state.created_at,
            updated_at=rust_state.updated_at,
            orchestrator_task_id=rust_state.orchestrator_task_id,
        )

    def to_rust(self) -> RustWorkflowState:
        """Convert to Rust PyWorkflowState."""
        # Map Python status to Rust status
        status_map = {
            WorkflowStatus.PENDING: RustWorkflowStatus.Pending,
            WorkflowStatus.RUNNING: RustWorkflowStatus.Running,
            WorkflowStatus.WAITING_SIGNAL: RustWorkflowStatus.WaitingSignal,
            WorkflowStatus.COMPENSATING: RustWorkflowStatus.Compensating,
            WorkflowStatus.PAUSED: RustWorkflowStatus.Paused,
            WorkflowStatus.COMPLETED: RustWorkflowStatus.Completed,
            WorkflowStatus.FAILED: RustWorkflowStatus.Failed,
            WorkflowStatus.CANCELLED: RustWorkflowStatus.Cancelled,
        }

        # Create error if present
        rust_error = None
        if self.error is not None:
            rust_error = RustError(
                type_=self.error.type,
                message=self.error.message,
                step=self.error.step,
                attempt=self.error.attempt,
                at=self.error.at,
            )

        rust_state = RustWorkflowState(
            id=self.id,
            workflow_type=self.type,
            definition_hash=self.definition_hash,
            data=self.data,
            created_at=self.created_at,
            updated_at=self.updated_at,
            status=status_map[self.status],
            current_steps=self.current_steps,
            error=rust_error,
            orchestrator_task_id=self.orchestrator_task_id,
        )

        # Add steps (must be done after creation due to Rust API)
        for name, step_state in self.steps.items():
            rust_state.set_step(name, step_state.to_rust())

        # Add signals
        for name, cursor in self.signals.items():
            rust_state.set_signal(name, cursor.cursor)

        return rust_state


@dataclass
class StepDef:
    """Definition of a workflow step."""

    name: str
    handler: Callable[..., Coroutine[Any, Any, Any]]
    depends_on: list[str] = field(default_factory=list[str])
    retries: int = 3
    timeout: int = 300
    on_failure: OnFailure = OnFailure.FAIL_WORKFLOW
    compensation: Callable[..., Coroutine[Any, Any, Any]] | None = None


@dataclass
class WorkflowRun:
    """A running workflow instance (client view)."""

    id: str
    status: WorkflowStatus
    type: str | None = None
    data: dict[str, Any] | None = None
    current_steps: list[str] | None = None
    error: WorkflowError | None = None


@dataclass
class Signal:
    """A signal sent to a workflow."""

    id: str
    name: str
    payload: Any
    created_at: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "id": self.id,
            "name": self.name,
            "payload": self.payload,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Signal:
        """Create from dictionary."""
        return cls(
            id=data["id"],
            name=data["name"],
            payload=data.get("payload"),
            created_at=data["created_at"],
        )

    def to_json(self) -> bytes:
        """Serialize to JSON bytes."""
        return json.dumps(self.to_dict()).encode("utf-8")

    @classmethod
    def from_json(cls, data: bytes) -> Signal:
        """Deserialize from JSON bytes."""
        return cls.from_dict(json.loads(data))


def compute_definition_hash(
    steps: dict[str, StepDef],
) -> str:
    """Compute a hash of the workflow definition structure."""
    # Build canonical representation
    canonical = {
        "steps": sorted(steps.keys()),
        "dependencies": {name: sorted(step.depends_on) for name, step in sorted(steps.items())},
    }
    content = json.dumps(canonical, sort_keys=True)
    hash_value = hashlib.sha256(content.encode()).hexdigest()[:16]
    return f"sha256:{hash_value}"
