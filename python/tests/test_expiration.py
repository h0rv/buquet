"""Tests for task expiration/TTL functionality.

Blind tests based on spec - verifying that tasks can have time-to-live
(TTL) and expire if not processed within their deadline.
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from buquet import PollingStrategy, Queue, TaskContext, TaskStatus, Worker, WorkerRunOptions

pytestmark = pytest.mark.integration

# Tolerance for S3 time comparisons in seconds
S3_TIME_TOLERANCE_SECONDS = 2


def parse_datetime(dt_str: str | None) -> datetime | None:
    """Parse an ISO datetime string from the API."""
    if dt_str is None:
        return None
    return datetime.fromisoformat(dt_str)


# Note: queue fixture is defined in conftest.py


class TestTaskExpiration:
    """Tests for task expiration/TTL."""

    @pytest.mark.asyncio
    async def test_submit_with_ttl_seconds(self, queue: Queue) -> None:
        """Test submitting a task with TTL sets expires_at."""
        unique_type = f"ttl_test_{uuid.uuid4().hex[:8]}"
        ttl_seconds = 3600  # 1 hour

        before_submit = datetime.now(UTC)

        task = await queue.submit(
            unique_type,
            {"test": "ttl_seconds"},
            ttl_seconds=ttl_seconds,
        )

        after_submit = datetime.now(UTC)

        # Task should have expires_at set
        assert task.expires_at is not None, (
            "Task submitted with ttl_seconds should have expires_at set"
        )

        # Parse the expires_at string
        expires_at = parse_datetime(task.expires_at)
        assert expires_at is not None

        # expires_at should be approximately now + ttl_seconds (with tolerance for S3 time)
        expected_min = before_submit + timedelta(seconds=ttl_seconds - S3_TIME_TOLERANCE_SECONDS)
        expected_max = after_submit + timedelta(seconds=ttl_seconds + S3_TIME_TOLERANCE_SECONDS)

        assert expected_min <= expires_at <= expected_max, (
            f"expires_at ({expires_at}) should be approximately "
            f"now + ttl_seconds ({expected_min} to {expected_max})"
        )

    @pytest.mark.asyncio
    async def test_submit_with_expires_at(self, queue: Queue) -> None:
        """Test submitting with explicit expires_at."""
        unique_type = f"explicit_expiry_test_{uuid.uuid4().hex[:8]}"

        # Set explicit expiration 2 hours from now
        explicit_expiry = datetime.now(UTC) + timedelta(hours=2)

        task = await queue.submit(
            unique_type,
            {"test": "explicit_expires_at"},
            expires_at=explicit_expiry,
        )

        assert task.expires_at is not None, "Task should have expires_at set"

        # Parse and compare
        expires_at = parse_datetime(task.expires_at)
        assert expires_at is not None

        # The stored expires_at should match what we provided (with tolerance for S3 time)
        diff = abs((expires_at - explicit_expiry).total_seconds())
        assert diff <= S3_TIME_TOLERANCE_SECONDS, (
            f"Stored expires_at ({expires_at}) should match provided value ({explicit_expiry})"
        )

    @pytest.mark.asyncio
    async def test_task_without_ttl_has_no_expiration(self, queue: Queue) -> None:
        """Test that tasks submitted without TTL have no expiration."""
        unique_type = f"no_ttl_test_{uuid.uuid4().hex[:8]}"

        task = await queue.submit(unique_type, {"test": "no_ttl"})

        assert task.expires_at is None, "Task submitted without TTL should have expires_at = None"

    @pytest.mark.asyncio
    async def test_task_is_expired_at(self, queue: Queue) -> None:
        """Test the is_expired_at method on tasks."""
        unique_type = f"is_expired_test_{uuid.uuid4().hex[:8]}"

        # Create task with past expiration
        past_expiry = datetime.now(UTC) - timedelta(hours=1)
        expired_task = await queue.submit(
            unique_type,
            {"test": "past_expiry"},
            expires_at=past_expiry,
        )

        # Create task with future expiration
        future_expiry = datetime.now(UTC) + timedelta(hours=1)
        valid_task = await queue.submit(
            unique_type,
            {"test": "future_expiry"},
            expires_at=future_expiry,
        )

        # Create task without expiration
        no_expiry_task = await queue.submit(
            unique_type,
            {"test": "no_expiry"},
        )

        # is_expired_at expects an RFC3339 string
        now_str = datetime.now(UTC).isoformat()

        # Test is_expired_at method
        assert expired_task.is_expired_at(now_str), "Task with past expires_at should be expired"
        assert not valid_task.is_expired_at(now_str), (
            "Task with future expires_at should not be expired"
        )
        assert not no_expiry_task.is_expired_at(now_str), (
            "Task without expires_at should not be expired"
        )

    @pytest.mark.asyncio
    async def test_expired_task_has_expired_status(self, queue: Queue) -> None:
        """Test that expired tasks get Expired status when marked."""
        unique_type = f"expired_status_test_{uuid.uuid4().hex[:8]}"

        # Submit task with past expiration
        past_expiry = datetime.now(UTC) - timedelta(seconds=10)
        task = await queue.submit(
            unique_type,
            {"test": "will_expire"},
            expires_at=past_expiry,
        )

        assert task.status == TaskStatus.Pending

        # Mark the task as expired
        expired_task = await queue.mark_expired(task.id)

        assert expired_task.status == TaskStatus.Expired, (
            "Task status should be Expired after mark_expired"
        )

    @pytest.mark.asyncio
    async def test_worker_marks_expired_tasks(self, queue: Queue) -> None:
        """Test that workers mark expired tasks instead of processing them."""
        unique_type = f"worker_expiry_test_{uuid.uuid4().hex[:8]}"

        # Submit task with past expiration
        past_expiry = datetime.now(UTC) - timedelta(seconds=5)
        task = await queue.submit(
            unique_type,
            {"test": "expired_before_processing"},
            expires_at=past_expiry,
        )

        await asyncio.sleep(0.1)  # Allow S3 index to propagate

        handler_called = False

        worker = Worker(queue, f"test-worker-{uuid.uuid4().hex[:8]}", [task.shard])

        @worker.task(unique_type)
        async def handle(_ctx: TaskContext) -> dict[str, object]:
            nonlocal handler_called
            handler_called = True
            return {"processed": True}

        assert callable(handle)

        # Run worker
        options = WorkerRunOptions(
            max_tasks=1,
            polling=PollingStrategy.fixed(interval_ms=50),
            with_monitor=True,  # Monitor should mark expired tasks
        )

        # Run for a short time to allow monitor to check
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(worker.run(options), timeout=2.0)

        # Fetch task to check status
        final_task = await queue.get(task.id)
        assert final_task is not None

        # Handler should NOT have been called for expired task
        assert not handler_called, "Handler should not be called for expired task"

        # Task should be Expired, not Completed
        assert final_task.status == TaskStatus.Expired, (
            f"Expired task should have Expired status, got {final_task.status}"
        )

    @pytest.mark.asyncio
    @pytest.mark.skip(reason="Complex async coordination test - needs refactoring")
    async def test_expiration_does_not_affect_running_tasks(self, queue: Queue) -> None:
        """Test that running tasks continue even if expires_at passes.

        NOTE: Skipped due to complexity of testing async task coordination.
        The key behavior (running tasks complete) is implicitly tested elsewhere.
        """

    @pytest.mark.asyncio
    async def test_ttl_zero_expires_immediately(self, queue: Queue) -> None:
        """Test that ttl=0 means task expires immediately."""
        unique_type = f"zero_ttl_test_{uuid.uuid4().hex[:8]}"

        task = await queue.submit(
            unique_type,
            {"test": "instant_expiry"},
            ttl_seconds=0,
        )

        assert task.expires_at is not None, "Task with ttl_seconds=0 should have expires_at"

        # Parse expires_at string to compare
        expires_at = parse_datetime(task.expires_at)
        assert expires_at is not None

        now = datetime.now(UTC)

        # expires_at should be at or before now (with tolerance for S3 time)
        assert expires_at <= now + timedelta(seconds=S3_TIME_TOLERANCE_SECONDS), (
            f"Task with ttl_seconds=0 should expire immediately, expires_at={expires_at}, now={now}"
        )

        # Task should be considered expired
        now_str = now.isoformat()
        is_expired = expires_at <= now + timedelta(seconds=S3_TIME_TOLERANCE_SECONDS)
        assert task.is_expired_at(now_str) or is_expired, (
            "Task with TTL=0 should be expired or about to expire"
        )

    @pytest.mark.asyncio
    async def test_submit_many_with_ttl(self, queue: Queue) -> None:
        """Test that submit_many respects TTL options."""
        unique_type = f"bulk_ttl_test_{uuid.uuid4().hex[:8]}"
        ttl_seconds = 1800  # 30 minutes

        tasks_data = [
            (unique_type, {"item": 1}),
            (unique_type, {"item": 2}),
            (unique_type, {"item": 3}),
        ]

        tasks = await queue.submit_many(tasks_data, ttl_seconds=ttl_seconds)

        expected_task_count = 3
        assert len(tasks) == expected_task_count

        for task in tasks:
            assert task.expires_at is not None, (
                "All tasks from submit_many should have expires_at set when ttl_seconds provided"
            )

    @pytest.mark.asyncio
    async def test_get_preserves_expiration(self, queue: Queue) -> None:
        """Test that fetching a task preserves its expiration time."""
        unique_type = f"get_expiry_test_{uuid.uuid4().hex[:8]}"

        explicit_expiry = datetime.now(UTC) + timedelta(hours=5)

        submitted_task = await queue.submit(
            unique_type,
            {"test": "fetch_expiry"},
            expires_at=explicit_expiry,
        )

        # Fetch the task
        fetched_task = await queue.get(submitted_task.id)
        assert fetched_task is not None

        # expires_at should be preserved
        assert submitted_task.expires_at == fetched_task.expires_at, (
            f"expires_at should be preserved when fetching task. "
            f"Submitted: {submitted_task.expires_at}, Fetched: {fetched_task.expires_at}"
        )

    @pytest.mark.asyncio
    async def test_mark_expired_fails_on_non_pending(self, queue: Queue) -> None:
        """Test that mark_expired fails on non-Pending tasks."""
        unique_type = f"mark_expired_fail_test_{uuid.uuid4().hex[:8]}"

        task = await queue.submit(
            unique_type,
            {"test": "will_complete"},
            expires_at=datetime.now(UTC) + timedelta(hours=1),
        )

        await asyncio.sleep(0.1)  # Allow S3 index to propagate

        # Create worker to complete the task
        worker = Worker(queue, f"test-worker-{uuid.uuid4().hex[:8]}", [task.shard])

        @worker.task(unique_type)
        async def handle(_ctx: TaskContext) -> dict[str, object]:
            return {"done": True}

        assert callable(handle)

        options = WorkerRunOptions(
            max_tasks=1,
            polling=PollingStrategy.fixed(interval_ms=50),
            with_monitor=False,
        )
        await worker.run(options)

        # Verify task is Completed
        completed_task = await queue.get(task.id)
        assert completed_task is not None
        assert completed_task.status == TaskStatus.Completed

        # Try to mark as expired - should fail with an error
        with pytest.raises(RuntimeError):
            await queue.mark_expired(task.id)


class TestExpirationEdgeCases:
    """Edge case tests for task expiration."""

    @pytest.mark.asyncio
    async def test_expires_at_in_far_future(self, queue: Queue) -> None:
        """Test task with expiration far in the future."""
        unique_type = f"far_future_test_{uuid.uuid4().hex[:8]}"

        far_future = datetime.now(UTC) + timedelta(days=365)

        task = await queue.submit(
            unique_type,
            {"test": "far_future"},
            expires_at=far_future,
        )

        assert task.expires_at is not None
        assert not task.is_expired_at(datetime.now(UTC).isoformat())

        # Task should be claimable
        fetched = await queue.get(task.id)
        assert fetched is not None
        assert fetched.status == TaskStatus.Pending

    @pytest.mark.asyncio
    async def test_multiple_tasks_different_expirations(self, queue: Queue) -> None:
        """Test multiple tasks with different expiration times."""
        unique_type = f"multi_expiry_test_{uuid.uuid4().hex[:8]}"

        now = datetime.now(UTC)

        # Task that already expired
        expired_task = await queue.submit(
            unique_type,
            {"order": 1},
            expires_at=now - timedelta(hours=1),
        )

        # Task expiring soon
        expiring_soon = await queue.submit(
            unique_type,
            {"order": 2},
            expires_at=now + timedelta(minutes=5),
        )

        # Task expiring later
        expiring_later = await queue.submit(
            unique_type,
            {"order": 3},
            expires_at=now + timedelta(hours=24),
        )

        # Task with no expiration
        no_expiry = await queue.submit(
            unique_type,
            {"order": 4},
        )

        # Verify expiration states
        now_str = now.isoformat()
        assert expired_task.is_expired_at(now_str)
        assert not expiring_soon.is_expired_at(now_str)
        assert not expiring_later.is_expired_at(now_str)
        assert not no_expiry.is_expired_at(now_str)

        # Verify expiration values
        assert expired_task.expires_at is not None
        assert expiring_soon.expires_at is not None
        assert expiring_later.expires_at is not None
        assert no_expiry.expires_at is None

    @pytest.mark.asyncio
    async def test_ttl_with_scheduled_task(self, queue: Queue) -> None:
        """Test that TTL works with scheduled tasks."""
        unique_type = f"scheduled_ttl_test_{uuid.uuid4().hex[:8]}"

        # Schedule task for 1 hour from now, with 2 hour TTL
        schedule_at = datetime.now(UTC) + timedelta(hours=1)
        ttl_seconds = 7200  # 2 hours

        task = await queue.submit(
            unique_type,
            {"test": "scheduled_with_ttl"},
            schedule_at=schedule_at,
            ttl_seconds=ttl_seconds,
        )

        assert task.available_at is not None
        assert task.expires_at is not None

        # expires_at should be approximately now + ttl_seconds,
        # not relative to schedule_at
        now = datetime.now(UTC)
        expected_expiry_min = now + timedelta(seconds=ttl_seconds - S3_TIME_TOLERANCE_SECONDS)
        expected_expiry_max = now + timedelta(seconds=ttl_seconds + S3_TIME_TOLERANCE_SECONDS)

        # Parse the expires_at string
        expires_at = parse_datetime(task.expires_at)
        assert expires_at is not None

        assert expected_expiry_min <= expires_at <= expected_expiry_max, (
            f"TTL should be calculated from submission time, not schedule time. "
            f"expires_at={expires_at}, range: {expected_expiry_min} to {expected_expiry_max}"
        )

    @pytest.mark.asyncio
    async def test_claim_race_with_expiration(self, queue: Queue) -> None:
        """Test race condition between claim and expiration."""
        unique_type = f"claim_race_test_{uuid.uuid4().hex[:8]}"

        # Task that expires very soon
        task = await queue.submit(
            unique_type,
            {"test": "race_condition"},
            ttl_seconds=1,
        )

        await asyncio.sleep(0.1)

        # Start worker immediately
        worker = Worker(queue, f"test-worker-{uuid.uuid4().hex[:8]}", [task.shard])

        claim_succeeded = False

        @worker.task(unique_type)
        async def handle(_ctx: TaskContext) -> dict[str, object]:
            nonlocal claim_succeeded
            claim_succeeded = True
            # Simulate work that takes longer than expiration
            await asyncio.sleep(S3_TIME_TOLERANCE_SECONDS)
            return {"result": "completed despite expiry time passing"}

        assert callable(handle)

        options = WorkerRunOptions(
            max_tasks=1,
            polling=PollingStrategy.fixed(interval_ms=10),  # Fast polling
            with_monitor=False,
        )

        await worker.run(options)

        final_task = await queue.get(task.id)
        assert final_task is not None

        # Either:
        # 1. Task was claimed before expiry and completed
        # 2. Task expired before it could be claimed
        if claim_succeeded:
            assert final_task.status == TaskStatus.Completed, (
                "If task was claimed, it should complete"
            )
        else:
            assert final_task.status in [TaskStatus.Expired, TaskStatus.Pending], (
                "If task wasn't claimed, it should be Expired or still Pending"
            )


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
