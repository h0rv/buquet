"""Tests for low-level storage operations.

These tests verify the queue.storage API for direct S3 access,
which is used by buquet-workflow for workflow state and signals.
"""

from __future__ import annotations

import json
import uuid
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from buquet import Queue

pytestmark = pytest.mark.integration


# Note: queue fixture is defined in conftest.py


class TestStorageGet:
    """Tests for storage.get()."""

    @pytest.mark.asyncio
    async def test_get_returns_data_and_etag(self, queue: Queue) -> None:
        """Test that get() returns (data, etag) tuple."""
        key = f"_test/storage_get_{uuid.uuid4().hex[:8]}.json"
        data = json.dumps({"test": "data"}).encode()

        try:
            # Put first
            await queue.storage.put(key, data)

            # Get
            got_data, etag = await queue.storage.get(key)

            assert got_data == data
            assert etag is not None
            assert len(etag) > 0
        finally:
            await queue.storage.delete(key)

    @pytest.mark.asyncio
    async def test_get_nonexistent_raises_error(self, queue: Queue) -> None:
        """Test that get() on nonexistent key raises RuntimeError."""
        key = f"_test/nonexistent_{uuid.uuid4().hex[:8]}.json"

        with pytest.raises(RuntimeError):
            await queue.storage.get(key)


class TestStoragePut:
    """Tests for storage.put()."""

    @pytest.mark.asyncio
    async def test_put_returns_etag(self, queue: Queue) -> None:
        """Test that put() returns the new etag."""
        key = f"_test/storage_put_{uuid.uuid4().hex[:8]}.json"
        data = json.dumps({"test": "data"}).encode()

        try:
            etag = await queue.storage.put(key, data)

            assert etag is not None
            assert len(etag) > 0
        finally:
            await queue.storage.delete(key)

    @pytest.mark.asyncio
    async def test_put_overwrites_existing(self, queue: Queue) -> None:
        """Test that put() without if_match overwrites existing data."""
        key = f"_test/storage_overwrite_{uuid.uuid4().hex[:8]}.json"
        data1 = json.dumps({"version": 1}).encode()
        data2 = json.dumps({"version": 2}).encode()

        try:
            await queue.storage.put(key, data1)
            await queue.storage.put(key, data2)

            got_data, _ = await queue.storage.get(key)
            assert json.loads(got_data)["version"] == 2
        finally:
            await queue.storage.delete(key)

    @pytest.mark.asyncio
    async def test_put_with_if_match_succeeds_on_match(self, queue: Queue) -> None:
        """Test that put() with matching if_match succeeds (CAS)."""
        key = f"_test/storage_cas_ok_{uuid.uuid4().hex[:8]}.json"
        data1 = json.dumps({"version": 1}).encode()
        data2 = json.dumps({"version": 2}).encode()

        try:
            etag1 = await queue.storage.put(key, data1)

            # CAS update should succeed
            etag2 = await queue.storage.put(key, data2, if_match=etag1)

            assert etag2 != etag1

            got_data, _ = await queue.storage.get(key)
            assert json.loads(got_data)["version"] == 2
        finally:
            await queue.storage.delete(key)

    @pytest.mark.asyncio
    async def test_put_with_if_match_fails_on_mismatch(self, queue: Queue) -> None:
        """Test that put() with wrong if_match fails (CAS conflict)."""
        key = f"_test/storage_cas_fail_{uuid.uuid4().hex[:8]}.json"
        data1 = json.dumps({"version": 1}).encode()
        data2 = json.dumps({"version": 2}).encode()

        try:
            await queue.storage.put(key, data1)

            # CAS update with wrong etag should fail
            with pytest.raises(RuntimeError):
                await queue.storage.put(key, data2, if_match="wrong-etag")

            # Original data should be unchanged
            got_data, _ = await queue.storage.get(key)
            assert json.loads(got_data)["version"] == 1
        finally:
            await queue.storage.delete(key)


class TestStorageDelete:
    """Tests for storage.delete()."""

    @pytest.mark.asyncio
    async def test_delete_removes_object(self, queue: Queue) -> None:
        """Test that delete() removes the object."""
        key = f"_test/storage_delete_{uuid.uuid4().hex[:8]}.json"
        data = json.dumps({"test": "data"}).encode()

        await queue.storage.put(key, data)
        await queue.storage.delete(key)

        with pytest.raises(RuntimeError):
            await queue.storage.get(key)

    @pytest.mark.asyncio
    async def test_delete_nonexistent_succeeds(self, queue: Queue) -> None:
        """Test that delete() on nonexistent key succeeds (idempotent)."""
        key = f"_test/nonexistent_{uuid.uuid4().hex[:8]}.json"

        # Should not raise
        await queue.storage.delete(key)


class TestStorageList:
    """Tests for storage.list()."""

    @pytest.mark.asyncio
    async def test_list_returns_matching_keys(self, queue: Queue) -> None:
        """Test that list() returns keys matching prefix."""
        prefix = f"_test/list_{uuid.uuid4().hex[:8]}/"
        keys = [f"{prefix}a.json", f"{prefix}b.json", f"{prefix}c.json"]

        try:
            for key in keys:
                await queue.storage.put(key, b"{}")

            result = await queue.storage.list(prefix)

            assert sorted(result) == sorted(keys)
        finally:
            for key in keys:
                await queue.storage.delete(key)

    @pytest.mark.asyncio
    async def test_list_with_start_after(self, queue: Queue) -> None:
        """Test that list() with start_after skips earlier keys."""
        prefix = f"_test/list_after_{uuid.uuid4().hex[:8]}/"
        keys = [f"{prefix}a.json", f"{prefix}b.json", f"{prefix}c.json"]

        try:
            for key in keys:
                await queue.storage.put(key, b"{}")

            # List starting after 'a'
            result = await queue.storage.list(prefix, start_after=f"{prefix}a.json")

            assert f"{prefix}a.json" not in result
            assert f"{prefix}b.json" in result
            assert f"{prefix}c.json" in result
        finally:
            for key in keys:
                await queue.storage.delete(key)

    @pytest.mark.asyncio
    async def test_list_with_limit(self, queue: Queue) -> None:
        """Test that list() respects limit parameter."""
        prefix = f"_test/list_limit_{uuid.uuid4().hex[:8]}/"
        keys = [f"{prefix}{i:02d}.json" for i in range(5)]

        try:
            for key in keys:
                await queue.storage.put(key, b"{}")

            result = await queue.storage.list(prefix, limit=2)

            assert len(result) == 2
        finally:
            for key in keys:
                await queue.storage.delete(key)

    @pytest.mark.asyncio
    async def test_list_empty_prefix_returns_empty(self, queue: Queue) -> None:
        """Test that list() with non-matching prefix returns empty list."""
        prefix = f"_test/nonexistent_{uuid.uuid4().hex[:8]}/"

        result = await queue.storage.list(prefix)

        assert result == []


class TestStorageExists:
    """Tests for storage.exists()."""

    @pytest.mark.asyncio
    async def test_exists_returns_true_for_existing(self, queue: Queue) -> None:
        """Test that exists() returns True for existing key."""
        key = f"_test/exists_{uuid.uuid4().hex[:8]}.json"

        try:
            await queue.storage.put(key, b"{}")

            assert await queue.storage.exists(key) is True
        finally:
            await queue.storage.delete(key)

    @pytest.mark.asyncio
    async def test_exists_returns_false_for_nonexistent(self, queue: Queue) -> None:
        """Test that exists() returns False for nonexistent key."""
        key = f"_test/nonexistent_{uuid.uuid4().hex[:8]}.json"

        assert await queue.storage.exists(key) is False


class TestStorageWorkflowPattern:
    """Tests for workflow-style usage patterns."""

    @pytest.mark.asyncio
    async def test_workflow_state_cas_update(self, queue: Queue) -> None:
        """Test CAS update pattern for workflow state."""
        wf_id = uuid.uuid4().hex[:8]
        key = f"workflow/{wf_id}/state.json"

        try:
            # Initial state
            state1 = {"status": "running", "step": 1}
            etag1 = await queue.storage.put(key, json.dumps(state1).encode())

            # Read-modify-write with CAS
            data, etag = await queue.storage.get(key)
            state = json.loads(data)
            state["step"] = 2
            await queue.storage.put(key, json.dumps(state).encode(), if_match=etag)

            # Verify
            data, _ = await queue.storage.get(key)
            assert json.loads(data)["step"] == 2

            # Concurrent update should fail with old etag
            state["step"] = 3
            with pytest.raises(RuntimeError):
                await queue.storage.put(key, json.dumps(state).encode(), if_match=etag1)
        finally:
            await queue.storage.delete(key)

    @pytest.mark.asyncio
    async def test_signal_cursor_pattern(self, queue: Queue) -> None:
        """Test cursor-based signal consumption pattern."""
        wf_id = uuid.uuid4().hex[:8]
        prefix = f"workflow/{wf_id}/signals/approval/"
        signals: list[str] = []

        try:
            # Send three signals with ordered timestamps
            for i in range(3):
                ts = f"2026-01-28T12:00:0{i}Z"
                signal_id = uuid.uuid4().hex[:8]
                key = f"{prefix}{ts}_{signal_id}.json"
                await queue.storage.put(key, json.dumps({"seq": i}).encode())
                signals.append(key)

            # List all signals
            all_keys = await queue.storage.list(prefix)
            assert len(all_keys) == 3

            # Consume first signal (cursor at None)
            first = sorted(all_keys)[0]

            # List signals after cursor
            remaining = await queue.storage.list(prefix, start_after=first)
            assert len(remaining) == 2
            assert first not in remaining

            # Consume second signal
            second = sorted(remaining)[0]
            remaining = await queue.storage.list(prefix, start_after=second)
            assert len(remaining) == 1
        finally:
            for key in signals:
                await queue.storage.delete(key)
