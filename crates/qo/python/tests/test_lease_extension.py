"""
Blind test for Task Lease Extension feature.

This test verifies the TaskContext API works as documented in:
- docs/features/task-lease-extension.md
- crates/qo/python/qo/_qo.pyi (type stubs)

Tests verify:
1. Task handlers can accept TaskContext as optional second argument
2. ctx.task_id returns the task ID
3. ctx.extend_lease(additional_secs) can be called
4. ctx.lease_expires_at() returns a timestamp
"""

from __future__ import annotations

import asyncio
import uuid

import pytest

from qo import Queue, TaskContext, Worker, WorkerRunOptions

pytestmark = pytest.mark.integration


def unique_task_type(prefix: str) -> str:
    """Generate a unique task type name to avoid collisions with existing tasks."""
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


# Note: queue fixture is defined in conftest.py


@pytest.fixture
async def worker(queue: Queue) -> Worker:
    """Create a worker instance."""
    shards = [f"{i:x}" for i in range(16)]  # ["0", "1", ..., "f"]
    return Worker(queue, f"test-worker-{uuid.uuid4().hex[:8]}", shards)


class TestTaskContextAPI:
    """Test that TaskContext API matches documentation."""

    @pytest.mark.asyncio
    async def test_handler_receives_task_context(self, queue: Queue, worker: Worker) -> None:
        """
        Test that a task handler can accept TaskContext as second argument.

        Documentation shows:
            @worker.task("long_running")
            async def long_running(input: dict, ctx: TaskContext) -> dict:
                ...
        """
        context_received: list[TaskContext] = []
        task_type = unique_task_type("context_received")

        @worker.task(task_type)
        async def handler(_data: dict[str, object], ctx: TaskContext) -> dict[str, object]:
            # Store context info for verification
            context_received.append(ctx)
            return {"received_context": True}

        assert callable(handler)

        # Submit a task
        await queue.submit(task_type, {"test": "data"})

        # Run worker to process tasks until our handler is called or timeout
        # We loop with max_tasks=10 to handle any stray tasks
        for _ in range(20):
            await worker.run(WorkerRunOptions(max_tasks=1, poll_interval_ms=50, with_monitor=False))
            if context_received:
                break

        # Verify handler received context
        assert len(context_received) >= 1
        assert context_received[0] is not None

    @pytest.mark.asyncio
    async def test_task_context_task_id_property(self, queue: Queue, worker: Worker) -> None:
        """
        Test that ctx.task_id returns the task ID.

        From type stubs:
            @property
            def task_id(self) -> str:
                '''Returns the task ID.'''
        """
        captured_task_id: list[str] = []
        task_type = unique_task_type("task_id_property")

        @worker.task(task_type)
        async def handler(_data: dict[str, object], ctx: TaskContext) -> dict[str, object]:
            captured_task_id.append(ctx.task_id)
            return {"task_id": ctx.task_id}

        assert callable(handler)

        # Submit a task
        task = await queue.submit(task_type, {"test": "data"})
        submitted_task_id = task.id

        # Run worker
        for _ in range(20):
            await worker.run(WorkerRunOptions(max_tasks=1, poll_interval_ms=50, with_monitor=False))
            if captured_task_id:
                break

        # Verify task_id matches
        assert len(captured_task_id) >= 1
        assert captured_task_id[0] == submitted_task_id

    @pytest.mark.asyncio
    async def test_extend_lease_method(self, queue: Queue, worker: Worker) -> None:
        """
        Test that ctx.extend_lease(additional_secs) can be called.

        Documentation shows:
            await ctx.extend_lease(60)  # Add 60 seconds

        From type stubs:
            async def extend_lease(self, additional_secs: int) -> None:
        """
        extend_lease_called: list[bool] = []
        task_type = unique_task_type("extend_lease")

        @worker.task(task_type)
        async def handler(_data: dict[str, object], ctx: TaskContext) -> dict[str, object]:
            # Call extend_lease - should not raise
            await ctx.extend_lease(60)
            extend_lease_called.append(True)
            return {"lease_extended": True}

        assert callable(handler)

        # Submit a task
        await queue.submit(task_type, {"test": "data"})

        # Run worker
        for _ in range(20):
            await worker.run(WorkerRunOptions(max_tasks=1, poll_interval_ms=50, with_monitor=False))
            if extend_lease_called:
                break

        # Verify extend_lease was called successfully
        assert len(extend_lease_called) >= 1
        assert extend_lease_called[0] is True

    @pytest.mark.asyncio
    async def test_lease_expires_at_method(self, queue: Queue, worker: Worker) -> None:
        """
        Test that ctx.lease_expires_at() returns a timestamp.

        From type stubs:
            async def lease_expires_at(self) -> str | None:
                '''Returns the current lease expiration time as an ISO 8601 string.'''
        """
        lease_expiry_values: list[str | None] = []
        task_type = unique_task_type("lease_expires_at")

        @worker.task(task_type)
        async def handler(_data: dict[str, object], ctx: TaskContext) -> dict[str, object]:
            expiry = await ctx.lease_expires_at()
            lease_expiry_values.append(expiry)
            return {"expiry": expiry}

        assert callable(handler)

        # Submit a task
        await queue.submit(task_type, {"test": "data"})

        # Run worker
        for _ in range(20):
            await worker.run(WorkerRunOptions(max_tasks=1, poll_interval_ms=50, with_monitor=False))
            if lease_expiry_values:
                break

        # Verify lease_expires_at returned a value
        assert len(lease_expiry_values) >= 1
        expiry = lease_expiry_values[0]
        # Should be a string (ISO 8601 timestamp) or None
        assert expiry is None or isinstance(expiry, str)
        # If it's a string, it should look like an ISO 8601 timestamp
        if expiry is not None:
            # Basic format check: should contain date-like patterns
            assert "T" in expiry or "-" in expiry, f"Expected ISO 8601 format, got: {expiry}"

    @pytest.mark.asyncio
    async def test_extend_lease_updates_expiry(self, queue: Queue, worker: Worker) -> None:
        """
        Test that calling extend_lease actually updates the lease expiry.

        Per documentation, extend_lease adds additional seconds to the lease.
        """
        expiry_before: list[str | None] = []
        expiry_after: list[str | None] = []
        task_type = unique_task_type("extend_updates_expiry")

        @worker.task(task_type)
        async def handler(_data: dict[str, object], ctx: TaskContext) -> dict[str, object]:
            # Get initial expiry
            before = await ctx.lease_expires_at()
            expiry_before.append(before)

            # Extend lease by 60 seconds
            await ctx.extend_lease(60)

            # Get new expiry
            after = await ctx.lease_expires_at()
            expiry_after.append(after)

            return {"before": before, "after": after}

        assert callable(handler)

        # Submit a task
        await queue.submit(task_type, {"test": "data"})

        # Run worker
        for _ in range(20):
            await worker.run(WorkerRunOptions(max_tasks=1, poll_interval_ms=50, with_monitor=False))
            if expiry_before:
                break

        # Verify lease was extended
        assert len(expiry_before) >= 1
        assert len(expiry_after) >= 1

        # Both should be timestamps (or at least after should be if lease was extended)
        if expiry_before[0] is not None and expiry_after[0] is not None:
            # After extending, the expiry should be different (later)
            assert expiry_after[0] != expiry_before[0], "Expiry should change after extend_lease"

    @pytest.mark.asyncio
    async def test_handler_without_context_still_works(self, queue: Queue, worker: Worker) -> None:
        """
        Test that handlers without TaskContext parameter still work.

        Documentation shows TaskContext is optional:
            # Simple handler (input only)
            @worker.task("send_email")
            async def handle_send_email(input: dict) -> dict:
                return {"sent": True}
        """
        handler_called: list[bool] = []
        task_type = unique_task_type("simple_handler")

        @worker.task(task_type)
        async def handler(_data: dict[str, object]) -> dict[str, object]:
            handler_called.append(True)
            return {"simple": True}

        assert callable(handler)

        # Submit a task
        await queue.submit(task_type, {"test": "data"})

        # Run worker
        for _ in range(20):
            await worker.run(WorkerRunOptions(max_tasks=1, poll_interval_ms=50, with_monitor=False))
            if handler_called:
                break

        # Verify handler was called
        assert len(handler_called) >= 1

    @pytest.mark.asyncio
    async def test_multiple_extend_lease_calls(self, queue: Queue, worker: Worker) -> None:
        """
        Test that extend_lease can be called multiple times during task execution.

        This simulates a long-running task that periodically extends its lease.
        """
        extend_count: list[int] = []
        task_type = unique_task_type("multiple_extends")

        @worker.task(task_type)
        async def handler(_data: dict[str, object], ctx: TaskContext) -> dict[str, object]:
            count = 0
            # Simulate work in chunks, extending lease after each
            for _ in range(3):
                await asyncio.sleep(0.01)  # Small delay to simulate work
                await ctx.extend_lease(30)
                count += 1
            extend_count.append(count)
            return {"extends": count}

        assert callable(handler)

        # Submit a task
        await queue.submit(task_type, {"test": "data"})

        # Run worker
        for _ in range(20):
            await worker.run(WorkerRunOptions(max_tasks=1, poll_interval_ms=50, with_monitor=False))
            if extend_count:
                break

        # Verify all extends succeeded
        assert len(extend_count) >= 1
        assert extend_count[0] == 3


class TestTaskContextTypes:
    """Test that TaskContext types are correct."""

    def test_task_context_importable_from_native_module(self) -> None:
        """Test that TaskContext can be imported from qo._qo native module."""
        # TaskContext is defined in the native module
        from qo._qo import TaskContext as NativeTaskContext  # noqa: PLC0415

        assert NativeTaskContext is not None

    @pytest.mark.asyncio
    async def test_task_id_is_string(self, queue: Queue, worker: Worker) -> None:
        """Test that task_id property returns a string."""
        task_id_type: list[str] = []
        task_type = unique_task_type("task_id_type")

        @worker.task(task_type)
        async def handler(_data: dict[str, object], ctx: TaskContext) -> dict[str, object]:
            task_id_type.append(type(ctx.task_id).__name__)
            return {"type": type(ctx.task_id).__name__}

        assert callable(handler)

        await queue.submit(task_type, {"test": "data"})

        for _ in range(20):
            await worker.run(WorkerRunOptions(max_tasks=1, poll_interval_ms=50, with_monitor=False))
            if task_id_type:
                break

        assert len(task_id_type) >= 1
        assert task_id_type[0] == "str"

    @pytest.mark.asyncio
    async def test_lease_expires_at_returns_string_or_none(
        self, queue: Queue, worker: Worker
    ) -> None:
        """Test that lease_expires_at returns str | None per type stubs."""
        return_type: list[str | None] = []
        task_type = unique_task_type("expiry_type")

        @worker.task(task_type)
        async def handler(_data: dict[str, object], ctx: TaskContext) -> dict[str, object]:
            expiry = await ctx.lease_expires_at()
            return_type.append(expiry)
            return {"expiry": expiry}

        assert callable(handler)

        await queue.submit(task_type, {"test": "data"})

        for _ in range(20):
            await worker.run(WorkerRunOptions(max_tasks=1, poll_interval_ms=50, with_monitor=False))
            if return_type:
                break

        assert len(return_type) >= 1
        # Should be str or None
        assert return_type[0] is None or isinstance(return_type[0], str)
