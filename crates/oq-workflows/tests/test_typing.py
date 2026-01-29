"""Type checking validation tests.

These tests ensure the type stubs are correct and native module types are accessible.
"""

from __future__ import annotations


class TestNativeModuleTypes:
    """Test that native module types are importable and correct."""

    def test_enum_types_importable(self) -> None:
        """Enum types should be importable from native module."""
        from oq_workflows._oq_workflows import (
            OnFailure,
            StepStatus,
            WorkflowStatus,
        )

        # Verify enum values exist
        assert WorkflowStatus.Pending is not None
        assert WorkflowStatus.Running is not None
        assert WorkflowStatus.Completed is not None
        assert WorkflowStatus.Failed is not None
        assert WorkflowStatus.Cancelled is not None
        assert WorkflowStatus.Paused is not None

        assert StepStatus.Pending is not None
        assert StepStatus.Running is not None
        assert StepStatus.Completed is not None
        assert StepStatus.Failed is not None

        assert OnFailure.FailWorkflow is not None
        assert OnFailure.PauseWorkflow is not None
        assert OnFailure.Continue is not None
        assert OnFailure.Compensate is not None

    def test_type_classes_importable(self) -> None:
        """Type classes should be importable."""
        from oq_workflows._oq_workflows import (
            OrchestrateResult,
            RecoveryReason,
            Signal,
            SignalCursor,
            SignalManager,
            StateManager,
            StepDef,
            StepState,
            SweepResult,
            WorkflowEngine,
            WorkflowErrorInfo,
            WorkflowNeedsRecovery,
            WorkflowRun,
            WorkflowState,
            WorkflowSweeper,
        )

        # Verify they're class types (use them to avoid unused import warnings)
        assert isinstance(WorkflowEngine, type)
        assert isinstance(WorkflowState, type)
        assert isinstance(StepState, type)
        assert isinstance(StateManager, type)
        assert isinstance(SignalManager, type)
        assert isinstance(OrchestrateResult, type)
        assert isinstance(RecoveryReason, type)
        assert isinstance(Signal, type)
        assert isinstance(SignalCursor, type)
        assert isinstance(StepDef, type)
        assert isinstance(SweepResult, type)
        assert isinstance(WorkflowErrorInfo, type)
        assert isinstance(WorkflowNeedsRecovery, type)
        assert isinstance(WorkflowRun, type)
        assert isinstance(WorkflowSweeper, type)

    def test_dag_functions_importable(self) -> None:
        """DAG functions should be importable."""
        from oq_workflows._oq_workflows import (
            compute_definition_hash,
            compute_ready_steps,
            get_blocked_steps,
            get_compensation_chain,
            has_unrecoverable_failure,
            is_workflow_complete,
            should_pause_workflow,
            topological_sort,
            validate_dag,
        )

        # Verify they're callable
        assert callable(compute_ready_steps)
        assert callable(is_workflow_complete)
        assert callable(has_unrecoverable_failure)
        assert callable(should_pause_workflow)
        assert callable(get_compensation_chain)
        assert callable(get_blocked_steps)
        assert callable(topological_sort)
        assert callable(validate_dag)
        assert callable(compute_definition_hash)

    def test_parse_signal_key_importable(self) -> None:
        """parse_signal_key function should be importable."""
        from oq_workflows._oq_workflows import parse_signal_key

        assert callable(parse_signal_key)


class TestPublicApiTypes:
    """Test that public API re-exports are typed correctly."""

    def test_public_exports_match_all(self) -> None:
        """All items in __all__ should be importable."""
        import oq_workflows

        for name in oq_workflows.__all__:
            assert hasattr(oq_workflows, name), f"Missing export: {name}"
