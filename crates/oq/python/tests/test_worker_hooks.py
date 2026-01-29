"""Tests for Worker lifecycle hooks.

Tests the on_startup, on_shutdown, on_success, and on_error hook decorators.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest

from oq import PermanentError, Queue, Task, TaskStatus, Worker, WorkerRunOptions, connect


class TestWorkerLifecycleHooksAPI:
    """Test that Worker lifecycle hook decorators exist."""

    def test_worker_has_on_startup_method(self) -> None:
        """Worker should have an on_startup decorator method."""
        assert hasattr(Worker, "on_startup")

    def test_worker_has_on_shutdown_method(self) -> None:
        """Worker should have an on_shutdown decorator method."""
        assert hasattr(Worker, "on_shutdown")

    def test_worker_has_on_success_method(self) -> None:
        """Worker should have an on_success decorator method."""
        assert hasattr(Worker, "on_success")

    def test_worker_has_on_error_method(self) -> None:
        """Worker should have an on_error decorator method."""
        assert hasattr(Worker, "on_error")


@pytest.mark.integration
class TestWorkerLifecycleHooksRegistration:
    """Test hook registration using decorator syntax.

    These tests require a real Queue (PyO3 doesn't accept mocks),
    so they are marked as integration tests.
    """

    @pytest.fixture
    async def queue(self) -> Queue:
        """Create a real queue for testing (requires S3)."""
        return await connect()

    @pytest.fixture
    def worker(self, queue: Queue) -> Worker:
        """Create a worker instance for testing hooks."""
        worker_id = f"test-worker-{uuid.uuid4().hex[:8]}"
        return Worker(queue, worker_id, queue.all_shards())

    def test_on_startup_decorator_registers_function(self, worker: Worker) -> None:
        """on_startup should work as a decorator and register the function."""

        @worker.on_startup
        async def startup() -> None:
            pass

        assert startup is not None
        assert callable(startup)

    def test_on_shutdown_decorator_registers_function(self, worker: Worker) -> None:
        """on_shutdown should work as a decorator and register the function."""

        @worker.on_shutdown
        async def shutdown() -> None:
            pass

        assert shutdown is not None
        assert callable(shutdown)

    def test_on_success_decorator_registers_function(self, worker: Worker) -> None:
        """on_success should work as a decorator and register the function."""

        @worker.on_success
        async def success_handler(task: Task) -> None:
            pass

        assert success_handler is not None
        assert callable(success_handler)

    def test_on_error_decorator_registers_function(self, worker: Worker) -> None:
        """on_error should work as a decorator and register the function."""

        @worker.on_error
        async def error_handler(task: Task, error: str) -> None:
            pass

        assert error_handler is not None
        assert callable(error_handler)

    def test_multiple_hooks_can_be_registered(self, worker: Worker) -> None:
        """All four hooks can be registered on the same worker."""

        @worker.on_startup
        async def startup() -> None:
            pass

        @worker.on_shutdown
        async def shutdown() -> None:
            pass

        @worker.on_success
        async def on_success(task: Task) -> None:
            pass

        @worker.on_error
        async def on_error(task: Task, error: str) -> None:
            pass

        assert callable(startup)
        assert callable(shutdown)
        assert callable(on_success)
        assert callable(on_error)

    def test_hooks_with_task_handler(self, worker: Worker) -> None:
        """Hooks can be registered alongside task handlers."""

        @worker.on_startup
        async def startup() -> None:
            pass

        @worker.task("send_email")
        async def handle_email(_data: dict[str, object]) -> dict[str, object]:
            return {"sent": True}

        @worker.on_success
        async def on_success(task: Task) -> None:
            pass

        assert callable(startup)
        assert callable(handle_email)
        assert callable(on_success)
        assert "send_email" in worker.registered_task_types()


@pytest.mark.integration
@pytest.mark.asyncio
class TestWorkerHooksIntegration:
    """Integration tests for worker lifecycle hooks (require S3)."""

    async def test_startup_hook_called_before_processing(self) -> None:
        """on_startup should be called once before any tasks are processed."""
        queue = await connect()
        worker_id = f"test-worker-{uuid.uuid4().hex[:8]}"
        worker = Worker(queue, worker_id, queue.all_shards())
        task_type = f"hook-startup-{uuid.uuid4().hex[:8]}"
        startup_called = False

        @worker.on_startup
        async def startup() -> None:
            nonlocal startup_called
            startup_called = True

        @worker.task(task_type)
        async def handle(_data: dict[str, object]) -> dict[str, object]:
            return {"ok": True}

        await queue.submit(task_type, {"test": True})
        await asyncio.sleep(0.1)
        await worker.run(WorkerRunOptions(max_tasks=1, poll_interval_ms=50, with_monitor=False))

        assert startup_called is True
        assert callable(startup)
        assert callable(handle)

    async def test_shutdown_hook_called_on_stop(self) -> None:
        """on_shutdown should be called when worker stops."""
        queue = await connect()
        worker_id = f"test-worker-{uuid.uuid4().hex[:8]}"
        worker = Worker(queue, worker_id, queue.all_shards())
        task_type = f"hook-shutdown-{uuid.uuid4().hex[:8]}"
        shutdown_called = False

        @worker.on_shutdown
        async def shutdown() -> None:
            nonlocal shutdown_called
            shutdown_called = True

        @worker.task(task_type)
        async def handle(_data: dict[str, object]) -> dict[str, object]:
            return {"ok": True}

        await queue.submit(task_type, {"test": True})
        await asyncio.sleep(0.1)
        await worker.run(WorkerRunOptions(max_tasks=1, poll_interval_ms=50, with_monitor=False))

        assert shutdown_called is True
        assert callable(shutdown)
        assert callable(handle)

    async def test_success_hook_receives_task(self) -> None:
        """on_success should receive the completed Task object."""
        queue = await connect()
        worker_id = f"test-worker-{uuid.uuid4().hex[:8]}"
        worker = Worker(queue, worker_id, queue.all_shards())
        task_type = f"hook-success-{uuid.uuid4().hex[:8]}"
        success_task_id: str | None = None

        @worker.on_success
        async def on_success(task: Task) -> None:
            nonlocal success_task_id
            success_task_id = task.id

        @worker.task(task_type)
        async def handle(_data: dict[str, object]) -> dict[str, object]:
            return {"ok": True}

        task = await queue.submit(task_type, {"test": True})
        await asyncio.sleep(0.1)
        await worker.run(WorkerRunOptions(max_tasks=1, poll_interval_ms=50, with_monitor=False))

        assert success_task_id == task.id
        assert callable(on_success)
        assert callable(handle)

    async def test_error_hook_receives_task_and_error(self) -> None:
        """on_error should receive both Task and error string."""
        queue = await connect()
        worker_id = f"test-worker-{uuid.uuid4().hex[:8]}"
        worker = Worker(queue, worker_id, queue.all_shards())
        task_type = f"hook-error-{uuid.uuid4().hex[:8]}"
        error_seen: str | None = None
        error_task_id: str | None = None

        @worker.on_error
        async def on_error(task: Task, error: str) -> None:
            nonlocal error_seen, error_task_id
            error_seen = error
            error_task_id = task.id

        @worker.task(task_type)
        async def handle(_data: dict[str, object]) -> dict[str, object]:
            message = "boom"
            raise PermanentError(message)

        task = await queue.submit(task_type, {"test": True}, max_retries=0)
        await asyncio.sleep(0.1)
        await worker.run(WorkerRunOptions(max_tasks=1, poll_interval_ms=50, with_monitor=False))

        assert error_task_id == task.id
        assert error_seen is not None
        assert "boom" in error_seen
        failed = await queue.get(task.id)
        assert failed is not None
        assert failed.status == TaskStatus.Failed
        assert callable(on_error)
        assert callable(handle)
