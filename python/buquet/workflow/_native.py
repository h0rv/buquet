"""Re-export native workflow bindings.

This module provides access to the native Rust workflow bindings.
The native module is registered as a submodule of buquet._buquet but
can only be accessed as an attribute, not imported directly.
"""

from __future__ import annotations

from buquet import _buquet

_workflow = _buquet.workflow

# Engine
WorkflowEngine = _workflow.WorkflowEngine
OrchestrateResult = _workflow.OrchestrateResult

# Types
OnFailure = _workflow.OnFailure
Signal = _workflow.Signal
SignalCursor = _workflow.SignalCursor
StepDef = _workflow.StepDef
StepState = _workflow.StepState
StepStatus = _workflow.StepStatus
WorkflowErrorInfo = _workflow.WorkflowErrorInfo
WorkflowRun = _workflow.WorkflowRun
WorkflowState = _workflow.WorkflowState
WorkflowStatus = _workflow.WorkflowStatus

# Managers
SignalManager = _workflow.SignalManager
StateManager = _workflow.StateManager

# Sweeper
RecoveryReason = _workflow.RecoveryReason
SweepResult = _workflow.SweepResult
WorkflowNeedsRecovery = _workflow.WorkflowNeedsRecovery
WorkflowSweeper = _workflow.WorkflowSweeper

# DAG functions
compute_definition_hash = _workflow.compute_definition_hash
compute_ready_steps = _workflow.compute_ready_steps
get_blocked_steps = _workflow.get_blocked_steps
get_compensation_chain = _workflow.get_compensation_chain
has_unrecoverable_failure = _workflow.has_unrecoverable_failure
is_workflow_complete = _workflow.is_workflow_complete
should_pause_workflow = _workflow.should_pause_workflow
topological_sort = _workflow.topological_sort
validate_dag = _workflow.validate_dag

# Signal utilities
parse_signal_key = _workflow.parse_signal_key

__all__ = [
    "OnFailure",
    "OrchestrateResult",
    "RecoveryReason",
    "Signal",
    "SignalCursor",
    "SignalManager",
    "StateManager",
    "StepDef",
    "StepState",
    "StepStatus",
    "SweepResult",
    "WorkflowEngine",
    "WorkflowErrorInfo",
    "WorkflowNeedsRecovery",
    "WorkflowRun",
    "WorkflowState",
    "WorkflowStatus",
    "WorkflowSweeper",
    "compute_definition_hash",
    "compute_ready_steps",
    "get_blocked_steps",
    "get_compensation_chain",
    "has_unrecoverable_failure",
    "is_workflow_complete",
    "parse_signal_key",
    "should_pause_workflow",
    "topological_sort",
    "validate_dag",
]
