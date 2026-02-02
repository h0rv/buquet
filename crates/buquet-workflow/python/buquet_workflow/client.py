"""Workflow client API."""

from __future__ import annotations

import asyncio
import os
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from buquet_workflow._buquet_workflow import WorkflowEngine

from .signals import send_signal
from .state import (
    get_workflow_state,
    list_workflows,
    update_workflow_state,
    workflow_exists,
)
from .types import (
    WorkflowError,
    WorkflowRun,
    WorkflowState,
    WorkflowStatus,
)

if TYPE_CHECKING:
    from datetime import timedelta

    from buquet import Queue

    from .workflow import Workflow


class WorkflowClient:
    """
    Client for managing workflow instances.

    Example:
        client = WorkflowClient(queue)

        # Start a workflow
        run = await client.start(my_workflow, {"order_id": "ORD-123"})

        # Check status
        run = await client.get(run.id)
        print(f"Status: {run.status}")

        # Wait for completion
        run = await client.wait(run.id, timeout=timedelta(minutes=30))

        # Send a signal
        await client.signal(run.id, "approval", {"approved": True})

        # Cancel
        await client.cancel(run.id, reason="User requested")
    """

    def __init__(self, queue: Queue, engine: WorkflowEngine | None = None) -> None:
        """
        Initialize the workflow client.

        Args:
            queue: The buquet queue
            engine: Optional WorkflowEngine instance. If not provided, will be
                    created lazily from environment variables.
        """
        self.queue = queue
        self._engine: WorkflowEngine | None = engine

    async def _get_engine(self) -> WorkflowEngine:
        """Get or create the workflow engine."""
        if self._engine is None:
            import buquet

            # Try config file, fall back to env vars
            try:
                config = buquet.load_config()
                bucket = os.environ.get("S3_BUCKET") or config.bucket
                endpoint = os.environ.get("S3_ENDPOINT") or config.endpoint
                region = os.environ.get("S3_REGION") or os.environ.get("AWS_REGION") or config.region
            except Exception:
                # Config loading failed, use env vars only
                bucket = os.environ.get("S3_BUCKET")
                endpoint = os.environ.get("S3_ENDPOINT")
                region = os.environ.get("S3_REGION") or os.environ.get("AWS_REGION", "us-east-1")

            if bucket is None:
                err_msg = "S3_BUCKET environment variable or .buquet.toml config required"
                raise RuntimeError(err_msg)

            self._engine = await WorkflowEngine.create(bucket, endpoint, region)

        return self._engine

    async def start(
        self,
        workflow: Workflow,
        data: dict[str, Any],
        *,
        workflow_id: str | None = None,
    ) -> WorkflowRun:
        """
        Start a new workflow instance.

        Uses idempotent creation - if workflow_id already exists, returns the
        existing workflow without modification.

        Args:
            workflow: The workflow definition
            data: Initial workflow data
            workflow_id: Optional custom workflow ID (auto-generated if not provided)

        Returns:
            WorkflowRun with the workflow ID and initial status
        """
        # Validate workflow
        errors = workflow.validate()
        if errors:
            msg = f"Invalid workflow: {errors}"
            raise ValueError(msg)

        wf_id = workflow_id or f"wf-{uuid4()}"
        now = await self.queue.now()

        # Use engine for idempotent creation
        engine = await self._get_engine()
        created, state = await engine.start_workflow(
            wf_id,
            workflow.name,
            workflow.definition_hash,
            data,
            now,
        )

        if not created:
            # Workflow already exists - convert Rust state to Python and return
            py_state = WorkflowState.from_rust(state)
            return WorkflowRun(
                id=wf_id,
                status=py_state.status,
                type=py_state.type,
                data=py_state.data,
                current_steps=py_state.current_steps,
                error=py_state.error,
            )

        # Submit orchestrator task for new workflow
        task = await self.queue.submit(
            "workflow.orchestrate",
            {"workflow_id": wf_id},
            idempotency_key=f"{wf_id}:orchestrate:start",
        )

        # Update state with orchestrator task ID
        state, etag = await get_workflow_state(self.queue, wf_id)
        state.orchestrator_task_id = task.id
        await update_workflow_state(self.queue, wf_id, state, etag)

        return WorkflowRun(
            id=wf_id,
            status=WorkflowStatus.RUNNING,
            type=workflow.name,
            data=data,
        )

    async def get(self, wf_id: str) -> WorkflowRun | None:
        """
        Get the current status of a workflow.

        Args:
            wf_id: Workflow ID

        Returns:
            WorkflowRun or None if not found
        """
        try:
            state, _ = await get_workflow_state(self.queue, wf_id)
            return WorkflowRun(
                id=wf_id,
                status=state.status,
                type=state.type,
                data=state.data,
                current_steps=state.current_steps,
                error=state.error,
            )
        except RuntimeError:
            return None

    async def wait(
        self,
        wf_id: str,
        *,
        timeout: timedelta | None = None,  # noqa: ASYNC109 - intentional timeout for workflow polling
        poll_interval: float = 1.0,
    ) -> WorkflowRun:
        """
        Wait for a workflow to complete.

        Args:
            wf_id: Workflow ID
            timeout: Maximum time to wait (None = wait indefinitely)
            poll_interval: Polling interval in seconds

        Returns:
            Final WorkflowRun

        Raises:
            TimeoutError: If timeout exceeded
            RuntimeError: If workflow not found
        """
        timeout_secs = timeout.total_seconds() if timeout else None
        elapsed = 0.0

        while timeout_secs is None or elapsed < timeout_secs:
            run = await self.get(wf_id)
            if run is None:
                err_msg = f"Workflow {wf_id} not found"
                raise RuntimeError(err_msg)

            if run.status in (
                WorkflowStatus.COMPLETED,
                WorkflowStatus.FAILED,
                WorkflowStatus.CANCELLED,
            ):
                return run

            await asyncio.sleep(poll_interval)
            elapsed += poll_interval

        err_msg = f"Workflow {wf_id} did not complete within timeout"
        raise TimeoutError(err_msg)

    async def cancel(
        self,
        wf_id: str,
        *,
        reason: str | None = None,
    ) -> WorkflowRun:
        """
        Cancel a workflow.

        Args:
            wf_id: Workflow ID
            reason: Optional cancellation reason

        Returns:
            Updated WorkflowRun

        Raises:
            RuntimeError: If workflow not found or already terminal
        """
        state, etag = await get_workflow_state(self.queue, wf_id)

        if state.status in (
            WorkflowStatus.COMPLETED,
            WorkflowStatus.FAILED,
            WorkflowStatus.CANCELLED,
        ):
            err_msg = f"Cannot cancel workflow in {state.status.value} status"
            raise RuntimeError(err_msg)

        state.status = WorkflowStatus.CANCELLED
        state.error = WorkflowError(
            type="Cancelled",
            message=reason or "User requested cancellation",
        )

        await update_workflow_state(self.queue, wf_id, state, etag)

        return WorkflowRun(
            id=wf_id,
            status=state.status,
            type=state.type,
            data=state.data,
            error=state.error,
        )

    async def signal(
        self,
        wf_id: str,
        name: str,
        payload: object,
    ) -> str:
        """
        Send a signal to a workflow.

        Args:
            wf_id: Workflow ID
            name: Signal name
            payload: Signal data

        Returns:
            Signal ID
        """
        return await send_signal(self.queue, wf_id, name, payload)

    async def pause(
        self,
        wf_id: str,
        *,
        reason: str | None = None,
    ) -> WorkflowRun:
        """
        Pause a workflow.

        Args:
            wf_id: Workflow ID
            reason: Optional pause reason

        Returns:
            Updated WorkflowRun
        """
        state, etag = await get_workflow_state(self.queue, wf_id)

        if state.status not in (
            WorkflowStatus.RUNNING,
            WorkflowStatus.WAITING_SIGNAL,
        ):
            err_msg = f"Cannot pause workflow in {state.status.value} status"
            raise RuntimeError(err_msg)

        state.status = WorkflowStatus.PAUSED
        state.error = WorkflowError(
            type="Paused",
            message=reason or "Manually paused",
        )

        await update_workflow_state(self.queue, wf_id, state, etag)

        return WorkflowRun(
            id=wf_id,
            status=state.status,
            type=state.type,
        )

    async def resume(self, wf_id: str) -> WorkflowRun:
        """
        Resume a paused workflow.

        Args:
            wf_id: Workflow ID

        Returns:
            Updated WorkflowRun
        """
        state, etag = await get_workflow_state(self.queue, wf_id)

        if state.status != WorkflowStatus.PAUSED:
            err_msg = f"Cannot resume workflow in {state.status.value} status"
            raise RuntimeError(err_msg)

        state.status = WorkflowStatus.RUNNING
        state.error = None

        await update_workflow_state(self.queue, wf_id, state, etag)

        # Re-trigger orchestrator
        now = await self.queue.now()
        await self.queue.submit(
            "workflow.orchestrate",
            {"workflow_id": wf_id},
            idempotency_key=f"{wf_id}:orchestrate:resume:{now}",
        )

        return WorkflowRun(
            id=wf_id,
            status=state.status,
            type=state.type,
        )

    async def exists(self, wf_id: str) -> bool:
        """Check if a workflow exists."""
        return await workflow_exists(self.queue, wf_id)

    async def list(
        self,
        *,
        prefix: str = "",
        limit: int = 100,
    ) -> list[str]:
        """
        List workflow IDs.

        Args:
            prefix: Optional prefix to filter workflow IDs
            limit: Maximum number of IDs to return

        Returns:
            List of workflow IDs
        """
        return await list_workflows(self.queue, prefix=prefix, limit=limit)
