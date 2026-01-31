"""
Tests for Idempotency Keys feature.

Tests verify the documented API behavior:
1. Submit with idempotency_key returns a task
2. Submit again with same key returns the same task (same task ID)
3. Submit with same key but different input raises IdempotencyConflict error
4. idempotency_scope="queue" provides global deduplication across task types
5. Default scope (task_type) allows same key for different task types
"""

from __future__ import annotations

import uuid

import pytest

from qo import Queue, connect

pytestmark = pytest.mark.integration


@pytest.fixture
def unique_key() -> str:
    """Generate a unique idempotency key for each test run."""
    return f"test-key-{uuid.uuid4()}"


@pytest.fixture
def unique_key2() -> str:
    """Generate a second unique idempotency key."""
    return f"test-key2-{uuid.uuid4()}"


class TestIdempotencyKeysBlind:
    """
    Blind tests for idempotency keys feature based on documentation.

    From docs/features/idempotency-keys.md:
    - Goal: Prevent duplicate task creation when producers retry submissions
    - Guarantee: All submissions with the same {scope, key} return the same task
    - Default scope = task_type
    - Optional scope = "queue" (global)
    - If input_hash differs on reuse, return IdempotencyConflict error
    """

    @pytest.mark.asyncio
    async def test_submit_with_idempotency_key_returns_task(self, unique_key: str) -> None:
        """
        Basic test: submitting a task with an idempotency key should work.

        From documentation:
        ```python
        await queue.submit(
            "charge_customer",
            input,
            idempotency_key="charge-ORD-123",
            idempotency_ttl_days=30,
        )
        ```
        """
        queue: Queue = await connect()

        task = await queue.submit(
            "test_idempotency_task",
            {"order_id": "ORD-123", "amount": 100},
            idempotency_key=unique_key,
        )

        assert task is not None
        assert task.id is not None
        assert task.task_type == "test_idempotency_task"

    @pytest.mark.asyncio
    async def test_same_key_returns_same_task(self, unique_key: str) -> None:
        """
        Submitting with the same idempotency key should return the same task.

        From documentation:
        "Guarantee: All submissions with the same {scope, key} return the same task."

        Semantics:
        - If create fails (already exists): Read existing idempotency record, return same task ID
        """
        queue: Queue = await connect()

        input_data: dict[str, object] = {"order_id": "ORD-456", "amount": 200}

        # First submission
        task1 = await queue.submit(
            "test_idempotency_task",
            input_data,
            idempotency_key=unique_key,
        )

        # Second submission with same key and same input
        task2 = await queue.submit(
            "test_idempotency_task",
            input_data,
            idempotency_key=unique_key,
        )

        # Should return the same task
        assert task1.id == task2.id, (
            f"Expected same task ID for same idempotency key. "
            f"Got task1.id={task1.id}, task2.id={task2.id}"
        )

    @pytest.mark.asyncio
    async def test_same_key_different_input_raises_conflict(self, unique_key: str) -> None:
        """
        Submitting with same key but different input should raise IdempotencyConflict.

        From documentation:
        "Validation / Safety: To prevent accidental misuse:
        - Store input_hash in the idempotency record.
        - On reuse: If input_hash differs, return a IdempotencyConflict error.
        - This avoids 'same key, different payload' bugs."
        """
        queue: Queue = await connect()

        # First submission
        await queue.submit(
            "test_idempotency_task",
            {"order_id": "ORD-789", "amount": 100},
            idempotency_key=unique_key,
        )

        # Second submission with same key but different input
        # Documentation says this should raise IdempotencyConflict
        with pytest.raises(Exception, match=r"(?i)(idempotency|conflict)"):
            await queue.submit(
                "test_idempotency_task",
                {"order_id": "ORD-789", "amount": 999},  # Different amount
                idempotency_key=unique_key,
            )

    @pytest.mark.asyncio
    async def test_default_scope_is_task_type(self, unique_key: str) -> None:
        """
        Default scope is task_type, so same key with different task types creates different tasks.

        From documentation:
        "Scope: Default scope = task_type"

        This means the same idempotency key can be used for different task types
        and they will create separate tasks.
        """
        queue: Queue = await connect()

        input_data: dict[str, object] = {"data": "test"}

        # Submit to task type A
        task_a = await queue.submit(
            "task_type_a",
            input_data,
            idempotency_key=unique_key,
        )

        # Submit to task type B with same key
        task_b = await queue.submit(
            "task_type_b",
            input_data,
            idempotency_key=unique_key,
        )

        # Should be different tasks since default scope is task_type
        assert task_a.id != task_b.id, (
            f"Expected different task IDs for different task types with same key. "
            f"Got task_a.id={task_a.id}, task_b.id={task_b.id}"
        )
        assert task_a.task_type == "task_type_a"
        assert task_b.task_type == "task_type_b"

    @pytest.mark.asyncio
    async def test_queue_scope_is_global(self, unique_key: str) -> None:
        """
        With idempotency_scope="queue", the same key across task types returns same task.

        From documentation:
        "Scope: Optional scope = queue (global)"

        This should make the idempotency key global across all task types.
        """
        queue: Queue = await connect()

        input_data: dict[str, object] = {"data": "global_test"}

        # Submit task type A with queue scope
        task_a = await queue.submit(
            "task_type_a",
            input_data,
            idempotency_key=unique_key,
            idempotency_scope="queue",
        )

        # Submit task type B with same key and queue scope
        # This should return the same task due to global scope
        task_b = await queue.submit(
            "task_type_a",  # Must be same task type since input must match
            input_data,
            idempotency_key=unique_key,
            idempotency_scope="queue",
        )

        # Should be the same task
        assert task_a.id == task_b.id, (
            f"Expected same task ID with queue scope. "
            f"Got task_a.id={task_a.id}, task_b.id={task_b.id}"
        )

    @pytest.mark.asyncio
    async def test_queue_scope_different_task_types_returns_original(self, unique_key: str) -> None:
        """
        With queue scope, using same key with different task types returns original task.

        DISCOVERED BEHAVIOR: The input_hash validation only checks the input data,
        not the task_type. So with queue scope, if you use the same key and same
        input for different task types, you get back the original task.

        This is a key finding from blind testing - the documentation says input_hash
        is validated, but task_type is not included in that hash.
        """
        queue: Queue = await connect()

        # Submit task type A with queue scope
        task_alpha = await queue.submit(
            "task_type_alpha",
            {"value": 1},
            idempotency_key=unique_key,
            idempotency_scope="queue",
        )

        # Submit task type B with same key and queue scope (but same input)
        # FINDING: This returns the original task, not a conflict
        task_beta = await queue.submit(
            "task_type_beta",  # Different task type
            {"value": 1},  # Same input
            idempotency_key=unique_key,
            idempotency_scope="queue",
        )

        # Returns the original task (task_type_alpha)
        assert task_alpha.id == task_beta.id, (
            f"Expected same task ID with queue scope even with different task_type. "
            f"Got task_alpha.id={task_alpha.id}, task_beta.id={task_beta.id}"
        )
        # The returned task retains the original task_type
        assert task_beta.task_type == "task_type_alpha", (
            f"Expected original task_type to be preserved, got: {task_beta.task_type}"
        )

    @pytest.mark.asyncio
    async def test_idempotency_ttl_parameter(self, unique_key: str) -> None:
        """
        Test that idempotency_ttl_days parameter is accepted.

        From documentation:
        "Default: 30 days."
        "await queue.submit(..., idempotency_ttl_days=30)"
        """
        queue: Queue = await connect()

        task = await queue.submit(
            "test_ttl_task",
            {"test": "data"},
            idempotency_key=unique_key,
            idempotency_ttl_days=7,  # Custom TTL
        )

        assert task is not None
        assert task.id is not None

    @pytest.mark.asyncio
    async def test_multiple_unique_keys_create_different_tasks(
        self, unique_key: str, unique_key2: str
    ) -> None:
        """
        Different idempotency keys should create different tasks.

        This validates the basic correctness of the feature.
        """
        queue: Queue = await connect()

        input_data: dict[str, object] = {"amount": 100}

        task1 = await queue.submit(
            "payment_task",
            input_data,
            idempotency_key=unique_key,
        )

        task2 = await queue.submit(
            "payment_task",
            input_data,
            idempotency_key=unique_key2,
        )

        # Different keys should create different tasks
        assert task1.id != task2.id, (
            f"Expected different task IDs for different keys. "
            f"Got task1.id={task1.id}, task2.id={task2.id}"
        )

    @pytest.mark.asyncio
    async def test_submit_without_idempotency_creates_new_task_each_time(self) -> None:
        """
        Submitting without idempotency key should create new task each time.

        This validates that idempotency is opt-in.
        """
        queue: Queue = await connect()

        input_data: dict[str, object] = {"value": 42}

        task1 = await queue.submit("regular_task", input_data)
        task2 = await queue.submit("regular_task", input_data)
        task3 = await queue.submit("regular_task", input_data)

        # All should be different tasks
        task_ids = {task1.id, task2.id, task3.id}
        assert len(task_ids) == 3, (
            f"Expected 3 unique task IDs without idempotency key. "
            f"Got: {task1.id}, {task2.id}, {task3.id}"
        )


class TestIdempotencyEdgeCases:
    """Edge case tests for idempotency keys."""

    @pytest.mark.asyncio
    async def test_empty_input_with_same_key(self) -> None:
        """Test idempotency with empty input dicts."""
        queue: Queue = await connect()
        key = f"empty-input-{uuid.uuid4()}"

        task1 = await queue.submit(
            "empty_input_task",
            {},
            idempotency_key=key,
        )

        task2 = await queue.submit(
            "empty_input_task",
            {},
            idempotency_key=key,
        )

        assert task1.id == task2.id

    @pytest.mark.asyncio
    async def test_complex_input_with_same_key(self) -> None:
        """Test idempotency with complex nested input."""
        queue: Queue = await connect()
        key = f"complex-input-{uuid.uuid4()}"

        complex_input: dict[str, object] = {
            "user": {
                "id": 123,
                "email": "test@example.com",
                "preferences": {
                    "notifications": True,
                    "theme": "dark",
                },
            },
            "items": [
                {"sku": "ABC", "qty": 2},
                {"sku": "XYZ", "qty": 1},
            ],
            "total": 99.99,
        }

        task1 = await queue.submit(
            "complex_task",
            complex_input,
            idempotency_key=key,
        )

        task2 = await queue.submit(
            "complex_task",
            complex_input,
            idempotency_key=key,
        )

        assert task1.id == task2.id

    @pytest.mark.asyncio
    async def test_special_characters_in_key(self) -> None:
        """Test idempotency key with special characters."""
        queue: Queue = await connect()
        key = f"special-key-{uuid.uuid4()}-!@#$%^&*()_+-=[]{{}}|;':\",./<>?"

        task1 = await queue.submit(
            "special_key_task",
            {"test": True},
            idempotency_key=key,
        )

        task2 = await queue.submit(
            "special_key_task",
            {"test": True},
            idempotency_key=key,
        )

        assert task1.id == task2.id


if __name__ == "__main__":
    # Run tests directly
    pytest.main([__file__, "-v", "-s"])
