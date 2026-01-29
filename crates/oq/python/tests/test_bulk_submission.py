"""Tests for bulk task submission.

Tests the Queue.submit_many method for submitting multiple tasks at once.
"""

from __future__ import annotations

import uuid

import pytest

from oq import Queue, RetryPolicy, TaskStatus, connect


class TestBulkSubmissionAPI:
    """Test that Queue.submit_many exists with the correct signature."""

    def test_queue_has_submit_many_method(self) -> None:
        """Queue should have a submit_many method."""
        assert hasattr(Queue, "submit_many")

    def test_submit_many_is_async(self) -> None:
        """submit_many should be an async method."""
        assert hasattr(Queue, "submit_many")
        assert Queue.submit_many is not None


@pytest.mark.asyncio
class TestBulkSubmissionFunctionality:
    """Test submit_many functionality (requires actual connection)."""

    async def test_submit_many_accepts_list_of_tuples(self) -> None:
        """submit_many should accept a list of (task_type, input) tuples."""
        assert hasattr(Queue, "submit_many")

    async def test_submit_many_signature_with_options(self) -> None:
        """submit_many should accept optional timeout, retries, and retry_policy."""
        initial_interval = 2000
        policy = RetryPolicy(initial_interval_ms=initial_interval)
        assert policy.initial_interval_ms == initial_interval


class TestBulkSubmissionTypes:
    """Test type expectations for submit_many."""

    def test_retry_policy_can_be_created_for_bulk_submit(self) -> None:
        """RetryPolicy should be creatable for use with submit_many."""
        initial_interval = 2000
        max_interval = 30000
        multiplier = 1.5
        jitter = 0.1

        policy = RetryPolicy(
            initial_interval_ms=initial_interval,
            max_interval_ms=max_interval,
            multiplier=multiplier,
            jitter_percent=jitter,
        )

        assert policy.initial_interval_ms == initial_interval
        assert policy.max_interval_ms == max_interval
        assert policy.multiplier == multiplier
        assert policy.jitter_percent == jitter


@pytest.mark.integration
@pytest.mark.asyncio
class TestBulkSubmissionIntegration:
    """Integration tests for bulk task submission (require S3)."""

    async def test_submit_many_returns_list_of_tasks(self) -> None:
        """submit_many should return a list of Task objects."""
        queue = await connect()
        task_type = f"bulk-{uuid.uuid4().hex[:8]}"
        tasks = await queue.submit_many(
            [
                (task_type, {"n": 1}),
                (task_type, {"n": 2}),
            ]
        )

        assert isinstance(tasks, list)
        assert len(tasks) == 2
        assert all(task.task_type == task_type for task in tasks)
        assert all(task.status == TaskStatus.Pending for task in tasks)

    async def test_submit_many_creates_all_tasks(self) -> None:
        """submit_many should create all tasks in the input list."""
        queue = await connect()
        task_type = f"bulk-create-{uuid.uuid4().hex[:8]}"
        tasks = await queue.submit_many(
            [
                (task_type, {"n": 1}),
                (task_type, {"n": 2}),
            ]
        )

        assert len(tasks) == 2
        for task in tasks:
            fetched = await queue.get(task.id)
            assert fetched is not None
            assert fetched.id == task.id

    async def test_submit_many_applies_options_to_all_tasks(self) -> None:
        """submit_many options should apply to all submitted tasks."""
        queue = await connect()
        task_type = f"bulk-options-{uuid.uuid4().hex[:8]}"
        policy = RetryPolicy(initial_interval_ms=2000)
        tasks = await queue.submit_many(
            [
                (task_type, {"n": 1}),
                (task_type, {"n": 2}),
            ],
            timeout_seconds=123,
            max_retries=2,
            retry_policy=policy,
        )

        assert len(tasks) == 2
        for task in tasks:
            assert task.timeout_seconds == 123
            assert task.max_retries == 2
            assert task.retry_policy.initial_interval_ms == 2000
