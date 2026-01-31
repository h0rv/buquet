#!/usr/bin/env python3
"""
Blind test for Payload References feature.

This test was written solely from documentation without reading source code.
It verifies that the payload references feature works as documented in:
- docs/features/payload-references.md

Test methodology:
1. Submit a task with use_payload_refs=True and a large input
2. Verify task.input_ref is set (payload was offloaded)
3. Use queue.load_input(task) to retrieve the full payload
4. Submit a task with use_payload_refs=False (default)
5. Verify input_ref is None (payload is inline)

Run with:
    pytest crates/qo/tests/blind/test_payload_refs_blind.py -v

Requires LocalStack or S3 to be running with appropriate env vars set.
"""

from __future__ import annotations

import asyncio
import os

import pytest

from qo import connect

# Mark as integration test (requires S3/LocalStack)
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("S3_BUCKET"),
        reason="S3_BUCKET environment variable not set - requires S3/LocalStack",
    ),
]


@pytest.fixture
def large_payload() -> dict[str, object]:
    """Create a payload large enough to trigger offloading.

    The exact threshold is implementation-defined, but we make it
    large enough that it should definitely trigger offloading.
    The default threshold is 256KB, so we create a ~300KB payload.
    """
    # Create a ~300KB payload (above the default 256KB threshold)
    return {
        "data": "x" * 300_000,
        "metadata": {
            "description": "Large payload for testing payload references",
            "size": "~300KB",
        },
    }


@pytest.fixture
def small_payload() -> dict[str, object]:
    """Create a small payload that should remain inline."""
    return {
        "message": "Hello, world!",
        "count": 42,
    }


class TestPayloadReferences:
    """Test the payload references (input/output offloading) feature."""

    @pytest.mark.asyncio
    async def test_submit_with_payload_refs_enabled(self, large_payload: dict[str, object]):
        """
        When submitting with use_payload_refs=True and a large input,
        the input should be offloaded and task.input_ref should be set.

        According to docs/features/payload-references.md:
        - Task object stores only small metadata and a reference key
        - Payload stored at payloads/{task_id}/input.json
        """
        queue = await connect()

        # Submit with payload refs enabled
        task = await queue.submit(
            "test_payload_refs",
            large_payload,
            use_payload_refs=True,
        )

        assert task is not None, "Task should be created"
        assert task.id is not None, "Task should have an ID"

        # According to the feature doc, input_ref should be set when offloaded
        # The exact value should be a reference to the payload location
        assert hasattr(task, "input_ref"), "Task should have input_ref attribute"
        assert task.input_ref is not None, (
            "input_ref should be set when use_payload_refs=True with large payload"
        )

        # The input_ref should point to the payloads location per the storage layout
        # documented as: payloads/{id}/input.json
        assert task.id in task.input_ref, f"input_ref should contain task id; got {task.input_ref}"

    @pytest.mark.asyncio
    async def test_load_input_retrieves_full_payload(self, large_payload: dict[str, object]):
        """
        When input is offloaded, queue.load_input(task) should retrieve
        the full original payload.
        """
        queue = await connect()

        # Submit with payload refs enabled
        task = await queue.submit(
            "test_payload_refs_load",
            large_payload,
            use_payload_refs=True,
        )

        # Load the full input
        loaded_input = await queue.load_input(task)

        # Verify we got back the original payload
        assert loaded_input is not None, "load_input should return the payload"
        assert loaded_input == large_payload, "load_input should return the exact original payload"

    @pytest.mark.asyncio
    async def test_submit_without_payload_refs_keeps_inline(self, small_payload: dict[str, object]):
        """
        When submitting with use_payload_refs=False (the default),
        input should remain inline and input_ref should be None.

        According to docs/features/payload-references.md:
        - Default remains inline payloads (no behavior change)
        - Payload refs are opt-in per task or per queue
        """
        queue = await connect()

        # Submit without payload refs (default behavior)
        task = await queue.submit(
            "test_inline_payload",
            small_payload,
            # use_payload_refs defaults to False
        )

        assert task is not None, "Task should be created"
        assert task.id is not None, "Task should have an ID"

        # With inline payloads, input_ref should be None
        if hasattr(task, "input_ref"):
            assert task.input_ref is None, "input_ref should be None when use_payload_refs=False"

        # The input should be directly accessible
        assert task.input == small_payload, "input should be accessible directly when inline"

    @pytest.mark.asyncio
    async def test_explicit_payload_refs_false(self, small_payload: dict[str, object]):
        """
        Explicitly setting use_payload_refs=False should keep payload inline.
        """
        queue = await connect()

        # Explicitly disable payload refs
        task = await queue.submit(
            "test_explicit_inline",
            small_payload,
            use_payload_refs=False,
        )

        assert task is not None, "Task should be created"

        # input_ref should be None or not present
        if hasattr(task, "input_ref"):
            assert task.input_ref is None, "input_ref should be None when use_payload_refs=False"

        # Input should be directly accessible
        assert task.input == small_payload

    @pytest.mark.asyncio
    async def test_load_input_on_inline_task(self, small_payload: dict[str, object]):
        """
        Calling load_input on a task with inline payload should still work,
        returning the inline input.
        """
        queue = await connect()

        # Submit with inline payload
        task = await queue.submit(
            "test_load_inline",
            small_payload,
            use_payload_refs=False,
        )

        # load_input should work even for inline payloads
        loaded_input = await queue.load_input(task)

        assert loaded_input == small_payload, (
            "load_input should return inline payload when input_ref is None"
        )


class TestPayloadReferencesEdgeCases:
    """Edge cases for payload references feature."""

    @pytest.mark.asyncio
    async def test_empty_payload_with_refs(self):
        """
        Empty payload with use_payload_refs=True should still work.
        """
        queue = await connect()

        # Empty input dict
        empty_input: dict[str, object] = {}

        task = await queue.submit(
            "test_empty_payload",
            empty_input,
            use_payload_refs=True,
        )

        assert task is not None

        # Even empty payloads should be loadable
        loaded = await queue.load_input(task)
        assert loaded == empty_input

    @pytest.mark.asyncio
    async def test_nested_payload_with_refs(self):
        """
        Complex nested payload should serialize and deserialize correctly.
        """
        queue = await connect()

        nested_input: dict[str, object] = {
            "level1": {
                "level2": {
                    "level3": {
                        "data": ["a", "b", "c"],
                        "numbers": [1, 2, 3],
                    }
                },
                "siblings": ["x", "y", "z"],
            },
            "metadata": {
                "created_by": "test",
                "version": 1,
            },
        }

        task = await queue.submit(
            "test_nested_payload",
            nested_input,
            use_payload_refs=True,
        )

        loaded = await queue.load_input(task)
        assert loaded == nested_input, (
            "Nested payload should round-trip correctly through payload refs"
        )


if __name__ == "__main__":
    # Allow running directly for quick testing
    async def run_tests():
        queue = await connect()

        # Test 1: Large payload with refs
        large_data: dict[str, object] = {"data": "x" * 100_000}
        task = await queue.submit("test_large", large_data, use_payload_refs=True)
        if hasattr(task, "input_ref"):
            pass

        # Test 2: Load the input back
        await queue.load_input(task)

        # Test 3: Small payload without refs
        small_data: dict[str, object] = {"message": "hello"}
        task2 = await queue.submit("test_small", small_data, use_payload_refs=False)
        if hasattr(task2, "input_ref"):
            pass

    asyncio.run(run_tests())
