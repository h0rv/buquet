"""Workflow definition and step decorator."""

from __future__ import annotations

import asyncio
import os
from typing import TYPE_CHECKING, Any

import buquet
from buquet.workflow._native import WorkflowEngine
from buquet.workflow.dag import validate_dag
from buquet.workflow.types import OnFailure, StepDef, TaskContext, compute_definition_hash

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    from buquet import Queue


class StepContext:
    """Context passed to step handlers."""

    def __init__(
        self,
        workflow_id: str,
        step: str,
        data: dict[str, Any],
        queue: Queue,
        task_context: TaskContext,
    ) -> None:
        self.workflow_id = workflow_id
        self.step = step
        self.data = data
        self.queue = queue
        self._task_context = task_context

    async def extend_lease(self, additional_secs: int) -> None:
        """Extend the lease on the current step task."""
        await self._task_context.extend_lease(additional_secs)

    async def is_cancellation_requested(self) -> bool:
        """Check if cancellation has been requested."""
        return await self._task_context.is_cancellation_requested()

    async def wait_for_signal(
        self,
        name: str,
        timeout_seconds: int | None = None,
    ) -> object:
        """
        Wait for and consume a signal.

        This uses atomic cursor advancement to ensure correct at-least-once
        semantics. The signal is consumed and won't be returned again.

        Args:
            name: Signal name to wait for
            timeout_seconds: Maximum time to wait (None = wait indefinitely)

        Returns:
            Signal payload, or None if timeout or cancellation
        """
        # Get or create engine
        # Note: This creates a new engine per call, but that's fine for signal consumption
        # In a production setting, you'd want to cache this
        try:
            config = buquet.load_config()
            bucket = os.environ.get("S3_BUCKET") or config.bucket
            endpoint = os.environ.get("S3_ENDPOINT") or config.endpoint
            region = os.environ.get("S3_REGION") or os.environ.get("AWS_REGION") or config.region
        except (OSError, ValueError, KeyError):
            bucket = os.environ.get("S3_BUCKET")
            endpoint = os.environ.get("S3_ENDPOINT")
            region = os.environ.get("S3_REGION") or os.environ.get("AWS_REGION", "us-east-1")

        if bucket is None:
            err_msg = "S3_BUCKET environment variable or .buquet.toml config required"
            raise RuntimeError(err_msg)

        engine = await WorkflowEngine.create(bucket, endpoint, region)

        poll_interval = 1.0  # seconds
        elapsed = 0.0

        while timeout_seconds is None or elapsed < timeout_seconds:
            now = await self.queue.now()

            # Use engine.consume_signal which atomically reads and advances cursor
            signal = await engine.consume_signal(self.workflow_id, name, now)
            if signal is not None:
                return signal.payload

            await asyncio.sleep(poll_interval)
            elapsed += poll_interval

            # Check for cancellation while waiting
            if await self.is_cancellation_requested():
                return None

        return None


class Workflow:
    """
    Workflow definition.

    Example:
        wf = Workflow("order_fulfillment")

        @wf.step("validate")
        async def validate(ctx: StepContext) -> dict:
            return {"valid": True}

        @wf.step("process", depends_on=["validate"])
        async def process(ctx: StepContext) -> dict:
            return {"processed": True}
    """

    def __init__(self, name: str) -> None:
        self.name = name
        self.steps: dict[str, StepDef] = {}
        self._definition_hash: str | None = None

    def step(
        self,
        name: str,
        *,
        depends_on: list[str] | None = None,
        retries: int = 3,
        timeout: int = 300,
        on_failure: str | OnFailure = OnFailure.FAIL_WORKFLOW,
    ) -> Callable[
        [Callable[..., Coroutine[Any, Any, Any]]],
        Callable[..., Coroutine[Any, Any, Any]],
    ]:
        """
        Decorator to register a step handler.

        Args:
            name: Step name (must be unique within workflow)
            depends_on: List of step names that must complete first
            retries: Maximum retry attempts
            timeout: Timeout in seconds
            on_failure: Action on failure (fail_workflow, pause_workflow, continue, compensate)

        Returns:
            Decorator function
        """
        if not isinstance(on_failure, OnFailure):
            on_failure = OnFailure(on_failure)

        def decorator(
            func: Callable[..., Coroutine[Any, Any, Any]],
        ) -> Callable[..., Coroutine[Any, Any, Any]]:
            step_def = StepDef(
                name=name,
                handler=func,
                depends_on=depends_on or [],
                retries=retries,
                timeout=timeout,
                on_failure=on_failure,
            )
            self.steps[name] = step_def
            self._definition_hash = None  # Invalidate cache
            return func

        return decorator

    @property
    def definition_hash(self) -> str:
        """Get the hash of the workflow definition."""
        if self._definition_hash is None:
            self._definition_hash = compute_definition_hash(self.steps)
        return self._definition_hash

    def validate(self) -> list[str]:
        """
        Validate the workflow definition.

        Returns:
            List of validation errors (empty if valid)
        """
        return validate_dag(self.steps)

    def get_step(self, name: str) -> StepDef | None:
        """Get a step definition by name."""
        return self.steps.get(name)

    def __repr__(self) -> str:
        return f"Workflow({self.name!r}, steps={list(self.steps.keys())})"
