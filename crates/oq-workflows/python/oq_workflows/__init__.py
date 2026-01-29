"""oq-workflows: Durable workflow orchestration built on oq."""

from __future__ import annotations

# Rust engine (native module - must be built with maturin)
from oq_workflows._oq_workflows import (
    OrchestrateResult,
    RecoveryReason,
    SweepResult,
    WorkflowEngine,
    WorkflowNeedsRecovery,
    WorkflowSweeper,
)

# Core Python classes
from .client import WorkflowClient
from .dag import (
    compute_ready_steps,
    get_blocked_steps,
    get_compensation_chain,
    has_unrecoverable_failure,
    is_workflow_complete,
    should_pause_workflow,
    topological_sort,
    validate_dag,
)
from .orchestrator import create_orchestrator_handler, orchestrate
from .signals import (
    consume_signal,
    count_signals,
    delete_signals,
    get_next_signal,
    list_signals,
    parse_signal_key,
    send_signal,
)
from .state import (
    create_workflow_state,
    delete_workflow_state,
    get_step_result,
    get_workflow_state,
    list_workflows,
    save_step_result,
    update_workflow_state,
    workflow_exists,
)
from .steps import (
    create_step_handler,
    handle_step,
    handle_step_cancellation,
    handle_step_failure,
)
from .types import (
    OnFailure,
    Signal,
    SignalCursor,
    StepDef,
    StepState,
    StepStatus,
    TaskContext,
    WorkflowError,
    WorkflowRun,
    WorkflowState,
    WorkflowStatus,
    compute_definition_hash,
)
from .worker import register_workflow, register_workflows
from .workflow import StepContext, Workflow

__all__ = [
    "OnFailure",
    "OrchestrateResult",
    "RecoveryReason",
    "Signal",
    "SignalCursor",
    "StepContext",
    "StepDef",
    "StepState",
    "StepStatus",
    "SweepResult",
    "TaskContext",
    "Workflow",
    "WorkflowClient",
    "WorkflowEngine",
    "WorkflowError",
    "WorkflowNeedsRecovery",
    "WorkflowRun",
    "WorkflowState",
    "WorkflowStatus",
    "WorkflowSweeper",
    "compute_definition_hash",
    "compute_ready_steps",
    "consume_signal",
    "count_signals",
    "create_orchestrator_handler",
    "create_step_handler",
    "create_workflow_state",
    "delete_signals",
    "delete_workflow_state",
    "get_blocked_steps",
    "get_compensation_chain",
    "get_next_signal",
    "get_step_result",
    "get_workflow_state",
    "handle_step",
    "handle_step_cancellation",
    "handle_step_failure",
    "has_unrecoverable_failure",
    "is_workflow_complete",
    "list_signals",
    "list_workflows",
    "orchestrate",
    "parse_signal_key",
    "register_workflow",
    "register_workflows",
    "save_step_result",
    "send_signal",
    "should_pause_workflow",
    "topological_sort",
    "update_workflow_state",
    "validate_dag",
    "workflow_exists",
]
