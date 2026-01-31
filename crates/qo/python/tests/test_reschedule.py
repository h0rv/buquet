"""Tests for task rescheduling (durable sleep) functionality."""

from __future__ import annotations

import asyncio
import uuid

import pytest

from qo import (
    PollingStrategy,
    Queue,
    RescheduleError,
    RetryableError,
    TaskStatus,
    Worker,
    WorkerRunOptions,
)

pytestmark = pytest.mark.integration


# Note: queue fixture is defined in conftest.py


class TestRescheduleError:
    """Tests for RescheduleError exception."""

    def test_reschedule_error_stores_delay(self) -> None:
        """Test that RescheduleError stores delay_seconds."""
        delay = 60
        error = RescheduleError(delay)
        assert error.delay_seconds == delay

    def test_reschedule_error_message(self) -> None:
        """Test that RescheduleError has descriptive message."""
        error = RescheduleError(120)
        assert "120" in str(error)
        assert "seconds" in str(error).lower()

    def test_reschedule_error_is_exception(self) -> None:
        """Test that RescheduleError is an Exception."""
        assert issubclass(RescheduleError, Exception)
        with pytest.raises(RescheduleError):
            raise RescheduleError(30)


class TestTaskRescheduleFields:
    """Tests for task reschedule fields."""

    @pytest.mark.asyncio
    async def test_task_has_reschedule_count_zero_initially(self, queue: Queue) -> None:
        """Test that new tasks have reschedule_count = 0."""
        task = await queue.submit("test_task", {"data": "test"})
        assert task.reschedule_count == 0

    @pytest.mark.asyncio
    async def test_task_max_reschedules_none_by_default(self, queue: Queue) -> None:
        """Test that max_reschedules is None by default (unlimited)."""
        task = await queue.submit("test_task", {"data": "test"})
        assert task.max_reschedules is None

    @pytest.mark.asyncio
    async def test_task_with_max_reschedules(self, queue: Queue) -> None:
        """Test submitting task with max_reschedules."""
        max_reschedules = 10
        task = await queue.submit("test_task", {"data": "test"}, max_reschedules=max_reschedules)
        assert task.max_reschedules == max_reschedules
        assert task.reschedule_count == 0

    @pytest.mark.asyncio
    async def test_submit_many_with_max_reschedules(self, queue: Queue) -> None:
        """Test submit_many with max_reschedules parameter."""
        max_reschedules = 5
        tasks = await queue.submit_many(
            [
                ("task_a", {"id": 1}),
                ("task_b", {"id": 2}),
            ],
            max_reschedules=max_reschedules,
        )
        assert len(tasks) == 2
        for task in tasks:
            assert task.max_reschedules == max_reschedules


class TestRescheduleTransition:
    """Integration tests for reschedule_task transition function."""

    @pytest.mark.asyncio
    async def test_reschedule_increments_count(self, queue: Queue) -> None:
        """Test that rescheduling increments reschedule_count."""
        unique_type = f"reschedule_test_{uuid.uuid4().hex[:8]}"
        task = await queue.submit(unique_type, {"attempt": 1})
        await asyncio.sleep(0.1)  # Allow S3 index to propagate

        worker = Worker(queue, f"test-worker-{uuid.uuid4().hex[:8]}", [task.shard])
        reschedule_count = 0

        @worker.task(unique_type)
        async def handle(_data: dict[str, object]) -> dict[str, object]:
            nonlocal reschedule_count
            reschedule_count += 1
            if reschedule_count < 3:
                raise RescheduleError(delay_seconds=0)
            return {"done": True}

        assert callable(handle)

        # max_tasks=3: 1 initial + 2 reschedules = 3 total executions
        options = WorkerRunOptions(
            max_tasks=3,
            polling=PollingStrategy.fixed(interval_ms=50),
            with_monitor=False,
        )
        await worker.run(options)

        final_task = await queue.get(task.id)
        assert final_task is not None
        assert final_task.status == TaskStatus.Completed
        assert final_task.reschedule_count == 2

    @pytest.mark.asyncio
    async def test_reschedule_updates_available_at(self, queue: Queue) -> None:
        """Test that rescheduling updates available_at to future time."""
        unique_type = f"reschedule_delay_test_{uuid.uuid4().hex[:8]}"
        task = await queue.submit(unique_type, {"check": "delay"})
        original_available_at = task.available_at
        await asyncio.sleep(0.1)  # Allow S3 index to propagate

        worker = Worker(queue, f"test-worker-{uuid.uuid4().hex[:8]}", [task.shard])
        rescheduled = False

        @worker.task(unique_type)
        async def handle(_data: dict[str, object]) -> dict[str, object]:
            nonlocal rescheduled
            if not rescheduled:
                rescheduled = True
                raise RescheduleError(delay_seconds=60)
            return {"done": True}

        assert callable(handle)

        # max_tasks=1: only process initial execution (which reschedules)
        options = WorkerRunOptions(
            max_tasks=1,
            polling=PollingStrategy.fixed(interval_ms=50),
            with_monitor=False,
        )
        await worker.run(options)

        updated_task = await queue.get(task.id)
        assert updated_task is not None
        assert updated_task.status == TaskStatus.Pending
        assert updated_task.reschedule_count == 1
        assert updated_task.available_at > original_available_at

    @pytest.mark.asyncio
    async def test_max_reschedules_exceeded_fails_task(self, queue: Queue) -> None:
        """Test that exceeding max_reschedules fails the task."""
        unique_type = f"max_reschedule_test_{uuid.uuid4().hex[:8]}"
        task = await queue.submit(
            unique_type,
            {"test": "max"},
            max_reschedules=2,
        )
        await asyncio.sleep(0.1)  # Allow S3 index to propagate

        worker = Worker(queue, f"test-worker-{uuid.uuid4().hex[:8]}", [task.shard])

        @worker.task(unique_type)
        async def handle(_data: dict[str, object]) -> dict[str, object]:
            raise RescheduleError(delay_seconds=0)

        assert callable(handle)

        # max_tasks=3: attempt 1 (reschedule), attempt 2 (reschedule), attempt 3 (fail)
        options = WorkerRunOptions(
            max_tasks=3,
            polling=PollingStrategy.fixed(interval_ms=50),
            with_monitor=False,
        )
        await worker.run(options)

        final_task = await queue.get(task.id)
        assert final_task is not None
        assert final_task.status == TaskStatus.Failed
        assert final_task.reschedule_count == 2
        assert final_task.last_error is not None
        assert "max reschedules" in final_task.last_error.lower()

    @pytest.mark.asyncio
    async def test_reschedule_separate_from_retry(self, queue: Queue) -> None:
        """Test that reschedule_count and retry_count are independent."""
        unique_type = f"mixed_test_{uuid.uuid4().hex[:8]}"
        task = await queue.submit(
            unique_type,
            {"action": "mixed"},
            max_retries=3,
            max_reschedules=3,
        )
        await asyncio.sleep(0.1)  # Allow S3 index to propagate

        worker = Worker(queue, f"test-worker-{uuid.uuid4().hex[:8]}", [task.shard])
        attempt = 0

        @worker.task(unique_type)
        async def handle(_data: dict[str, object]) -> dict[str, object]:
            nonlocal attempt
            attempt += 1
            if attempt == 1:
                raise RescheduleError(delay_seconds=0)
            if attempt == 2:
                msg = "transient error"
                raise RetryableError(msg)
            if attempt == 3:
                raise RescheduleError(delay_seconds=0)
            return {"done": True}

        assert callable(handle)

        # max_tasks=4: reschedule, retry, reschedule, success
        options = WorkerRunOptions(
            max_tasks=4,
            polling=PollingStrategy.fixed(interval_ms=50),
            with_monitor=False,
        )
        await worker.run(options)

        final_task = await queue.get(task.id)
        assert final_task is not None
        assert final_task.status == TaskStatus.Completed
        assert final_task.reschedule_count == 2
        assert final_task.retry_count == 1


class TestWorkerHandlerFiltering:
    """Tests for worker handler filtering (bug fix: workers should not claim unhandled tasks)."""

    @pytest.mark.asyncio
    async def test_worker_does_not_claim_unhandled_task_types(self, queue: Queue) -> None:
        """Test that worker skips tasks it doesn't have handlers for.

        This is a regression test for the bug where workers claimed ALL tasks
        in the ready index regardless of whether they had a handler registered.

        The test runs a worker that only handles type B in the presence of type A
        tasks. The worker should skip type A tasks (leaving them pending) and only
        process type B tasks when they exist.
        """
        unique_type_a = f"task_type_a_{uuid.uuid4().hex[:8]}"
        unique_type_b = f"task_type_b_{uuid.uuid4().hex[:8]}"

        # Submit a task of type A (which the worker can't handle)
        task_a = await queue.submit(unique_type_a, {"type": "a"})
        # Submit a task of type B (which the worker CAN handle) - this allows the
        # worker to complete with max_tasks=1
        task_b = await queue.submit(unique_type_b, {"type": "b"})
        await asyncio.sleep(0.1)  # Allow S3 index to propagate

        # Create worker that polls all shards but only handles type B (NOT type A)
        worker = Worker(queue, f"test-worker-{uuid.uuid4().hex[:8]}", queue.all_shards())

        @worker.task(unique_type_b)
        async def handle_b(_data: dict[str, object]) -> dict[str, object]:
            return {"handled": "b"}

        assert callable(handle_b)

        # Run worker with max_tasks=1 - it should skip task_a and claim task_b
        options = WorkerRunOptions(
            max_tasks=1,
            polling=PollingStrategy.fixed(interval_ms=50),
            with_monitor=False,
        )
        await worker.run(options)

        # Task A should still be pending, NOT failed
        # This is the key assertion - before the bug fix, task A would be claimed
        # and then failed with "No handler registered"
        task_a_after = await queue.get(task_a.id)
        assert task_a_after is not None
        assert task_a_after.status == TaskStatus.Pending, (
            f"Task A should remain Pending, not {task_a_after.status}. "
            "Worker should not have claimed a task it cannot handle."
        )

        # Task B should be completed since we have a handler for it
        task_b_after = await queue.get(task_b.id)
        assert task_b_after is not None
        assert task_b_after.status == TaskStatus.Completed

    @pytest.mark.asyncio
    async def test_worker_claims_only_handled_task_types(self, queue: Queue) -> None:
        """Test that worker correctly claims and processes tasks it has handlers for."""
        unique_type_a = f"task_type_a_{uuid.uuid4().hex[:8]}"
        unique_type_b = f"task_type_b_{uuid.uuid4().hex[:8]}"

        # Submit tasks of both types
        task_a = await queue.submit(unique_type_a, {"type": "a"})
        task_b = await queue.submit(unique_type_b, {"type": "b"})
        await asyncio.sleep(0.1)  # Allow S3 index to propagate

        # Create worker that polls all shards but only handles type B
        worker = Worker(queue, f"test-worker-{uuid.uuid4().hex[:8]}", queue.all_shards())

        @worker.task(unique_type_b)
        async def handle_b(_data: dict[str, object]) -> dict[str, object]:
            return {"handled": "b"}

        assert callable(handle_b)

        # Run worker with max_tasks=1 - it should claim task_b and skip task_a
        options = WorkerRunOptions(
            max_tasks=1,
            polling=PollingStrategy.fixed(interval_ms=50),
            with_monitor=False,
        )
        await worker.run(options)

        # Task A should still be pending
        task_a_after = await queue.get(task_a.id)
        assert task_a_after is not None
        assert task_a_after.status == TaskStatus.Pending

        # Task B should be completed
        task_b_after = await queue.get(task_b.id)
        assert task_b_after is not None
        assert task_b_after.status == TaskStatus.Completed

    @pytest.mark.asyncio
    async def test_multiple_workers_with_different_handlers(self, queue: Queue) -> None:
        """Test that different workers can handle different task types correctly."""
        unique_type_a = f"task_type_a_{uuid.uuid4().hex[:8]}"
        unique_type_b = f"task_type_b_{uuid.uuid4().hex[:8]}"

        # Submit tasks of both types
        task_a = await queue.submit(unique_type_a, {"type": "a"})
        task_b = await queue.submit(unique_type_b, {"type": "b"})
        await asyncio.sleep(0.1)  # Allow S3 index to propagate

        # Worker 1 handles type A
        worker1 = Worker(queue, f"worker-1-{uuid.uuid4().hex[:8]}", queue.all_shards())

        @worker1.task(unique_type_a)
        async def handle_a(_data: dict[str, object]) -> dict[str, object]:
            return {"handled": "a"}

        assert callable(handle_a)

        # Worker 2 handles type B
        worker2 = Worker(queue, f"worker-2-{uuid.uuid4().hex[:8]}", queue.all_shards())

        @worker2.task(unique_type_b)
        async def handle_b(_data: dict[str, object]) -> dict[str, object]:
            return {"handled": "b"}

        assert callable(handle_b)

        # Run both workers
        options = WorkerRunOptions(
            max_tasks=1,
            polling=PollingStrategy.fixed(interval_ms=50),
            with_monitor=False,
        )
        await worker1.run(options)
        await worker2.run(options)

        # Both tasks should be completed
        task_a_after = await queue.get(task_a.id)
        task_b_after = await queue.get(task_b.id)

        assert task_a_after is not None
        assert task_a_after.status == TaskStatus.Completed

        assert task_b_after is not None
        assert task_b_after.status == TaskStatus.Completed
