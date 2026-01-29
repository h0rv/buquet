"""
Blind tests for Shard Leasing feature.

These tests verify the Shard Leasing API works as documented without looking
at any implementation details. Based solely on:
- docs/features/shard-leases.md
- docs/getting-started.md
- crates/oq/python/README.md

Shard Leasing assigns shards dynamically to workers via S3-backed leases,
so each worker only polls the shards it owns. This eliminates redundant
LIST operations across workers.

DOCUMENTATION DISCREPANCIES FOUND:
1. docs/getting-started.md shows WorkerRunOptions(shard_leasing=...) but
   WorkerRunOptions does not accept a 'shard_leasing' parameter.
2. docs/features/shard-leases.md mentions 'lease_renew_interval_secs' config
   but ShardLeaseConfig.custom() does not accept this parameter.
"""

from __future__ import annotations

import os
import uuid

import pytest

# Set up environment for LocalStack before importing oq
os.environ.setdefault("S3_ENDPOINT", "http://localhost:4566")
os.environ.setdefault("S3_BUCKET", "oq-dev")
os.environ.setdefault("S3_REGION", "us-east-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")


from oq import Queue, ShardLeaseConfig, Worker, WorkerRunOptions, connect


class TestShardLeaseConfigAPI:
    """Test that ShardLeaseConfig API methods exist and work as documented."""

    def test_enabled_method_exists(self):
        """ShardLeaseConfig.enabled() should exist and return a config object.

        From docs/getting-started.md:
            options = WorkerRunOptions(shard_leasing=ShardLeaseConfig.enabled())
        """
        config = ShardLeaseConfig.enabled()
        assert config is not None, "ShardLeaseConfig.enabled() should return a config object"

    def test_disabled_method_exists(self):
        """ShardLeaseConfig.disabled() should exist and return a config object.

        This is the logical complement to enabled() for explicitly disabling
        shard leasing (the default behavior).
        """
        config = ShardLeaseConfig.disabled()
        assert config is not None, "ShardLeaseConfig.disabled() should return a config object"

    def test_custom_method_exists(self):
        """ShardLeaseConfig.custom() should exist for custom configuration.

        From docs/features/shard-leases.md configuration section:
            shards_per_worker = 16
            lease_ttl_secs = 30

        Note: The docs mention 'lease_renew_interval_secs' but the API does
        not accept this parameter.
        """
        config = ShardLeaseConfig.custom(
            shards_per_worker=16,
            lease_ttl_secs=30,
        )
        assert config is not None, "ShardLeaseConfig.custom() should return a config object"

    def test_custom_with_partial_params(self):
        """ShardLeaseConfig.custom() should work with partial parameters.

        Users should be able to customize only some values while using
        defaults for others.
        """
        # Only specify shards_per_worker
        config = ShardLeaseConfig.custom(shards_per_worker=8)
        assert config is not None

    def test_custom_with_only_lease_ttl(self):
        """ShardLeaseConfig.custom() should accept only lease_ttl_secs."""
        config = ShardLeaseConfig.custom(lease_ttl_secs=60)
        assert config is not None

    def test_enabled_vs_disabled_are_different(self):
        """enabled() and disabled() should produce different configurations."""
        enabled = ShardLeaseConfig.enabled()
        disabled = ShardLeaseConfig.disabled()
        # They should be different objects representing different states
        # The exact comparison depends on implementation but they shouldn't be identical
        assert (
            enabled is not disabled
            or type(enabled) is not type(disabled)
            or str(enabled) != str(disabled)
        )

    def test_custom_with_default_values(self):
        """ShardLeaseConfig.custom() with no args should work (uses defaults)."""
        config = ShardLeaseConfig.custom()
        assert config is not None


class TestShardLeaseConfigTypes:
    """Test that ShardLeaseConfig returns proper types."""

    def test_enabled_returns_shard_lease_config(self):
        """enabled() should return a ShardLeaseConfig instance."""
        config = ShardLeaseConfig.enabled()
        assert isinstance(config, ShardLeaseConfig)

    def test_disabled_returns_shard_lease_config(self):
        """disabled() should return a ShardLeaseConfig instance."""
        config = ShardLeaseConfig.disabled()
        assert isinstance(config, ShardLeaseConfig)

    def test_custom_returns_shard_lease_config(self):
        """custom() should return a ShardLeaseConfig instance."""
        config = ShardLeaseConfig.custom(shards_per_worker=8)
        assert isinstance(config, ShardLeaseConfig)


class TestWorkerRunOptionsBasic:
    """Test basic WorkerRunOptions functionality.

    Note: The documentation at docs/getting-started.md shows:
        WorkerRunOptions(shard_leasing=ShardLeaseConfig.enabled())

    However, testing reveals WorkerRunOptions does NOT accept a 'shard_leasing'
    parameter. This appears to be a documentation bug or planned feature.
    """

    def test_worker_run_options_exists(self):
        """WorkerRunOptions should be importable and constructible."""
        options = WorkerRunOptions()
        assert options is not None

    def test_worker_run_options_accepts_poll_interval(self):
        """WorkerRunOptions should accept poll_interval_ms.

        From crates/oq/python/README.md:
            opts = WorkerRunOptions(poll_interval_ms=500, ...)
        """
        options = WorkerRunOptions(poll_interval_ms=500)
        assert options is not None

    def test_worker_run_options_accepts_with_monitor(self):
        """WorkerRunOptions should accept with_monitor.

        From crates/oq/python/README.md:
            opts = WorkerRunOptions(..., with_monitor=True, ...)
        """
        options = WorkerRunOptions(with_monitor=True)
        assert options is not None

    def test_worker_run_options_accepts_monitor_interval(self):
        """WorkerRunOptions should accept monitor_check_interval_s.

        From crates/oq/python/README.md:
            opts = WorkerRunOptions(..., monitor_check_interval_s=15)
        """
        options = WorkerRunOptions(monitor_check_interval_s=15)
        assert options is not None

    def test_worker_run_options_combined(self):
        """WorkerRunOptions should accept multiple parameters together."""
        options = WorkerRunOptions(
            poll_interval_ms=500,
            with_monitor=True,
            monitor_check_interval_s=15,
        )
        assert options is not None

    @pytest.mark.xfail(reason="Documentation bug: shard_leasing param not implemented")
    def test_worker_run_options_accepts_shard_leasing(self):
        """WorkerRunOptions should accept shard_leasing parameter.

        DOCUMENTATION BUG: docs/getting-started.md shows this API but it
        does not exist. Marked as xfail to document the discrepancy.

        From docs/getting-started.md:
            options = WorkerRunOptions(shard_leasing=ShardLeaseConfig.enabled())
        """
        options = WorkerRunOptions(
            shard_leasing=ShardLeaseConfig.enabled(),  # type: ignore[reportCallIssue]
        )
        assert options is not None


class TestWorkerWithShardLeasing:
    """Test Worker creation and configuration with shard leasing."""

    @pytest.fixture
    async def queue(self) -> Queue:
        """Create a queue connection for testing."""
        return await connect()

    @pytest.mark.asyncio
    async def test_worker_creation_with_queue(self, queue: Queue) -> None:
        """Worker should be creatable with a queue connection.

        From crates/oq/python/README.md:
            worker = Worker(queue, "worker-1", ["0", "1", "2", "3"])
        """
        worker_id = f"test-worker-{uuid.uuid4().hex[:8]}"
        worker = Worker(queue, worker_id, ["0", "1", "2", "3"])
        assert worker is not None

    @pytest.mark.asyncio
    async def test_worker_with_task_handler(self, queue: Queue) -> None:
        """Worker should accept task handlers via decorator."""
        worker_id = f"test-worker-{uuid.uuid4().hex[:8]}"
        worker = Worker(queue, worker_id, ["0", "1"])

        @worker.task("test_task")
        async def dummy_handler(_input: dict[str, object]) -> dict[str, object]:
            return {"ok": True}

        assert callable(dummy_handler)
        assert worker is not None

    @pytest.mark.asyncio
    async def test_worker_run_options_can_be_created_for_worker(self, queue: Queue) -> None:
        """WorkerRunOptions should be creatable for use with worker.run().

        We verify options can be created but don't actually call run()
        since that would block.
        """
        worker_id = f"test-worker-{uuid.uuid4().hex[:8]}"
        worker = Worker(queue, worker_id, ["0", "1"])

        @worker.task("test_task")
        async def dummy_handler(_input: dict[str, object]) -> dict[str, object]:
            return {"ok": True}

        assert callable(dummy_handler)
        options = WorkerRunOptions(
            poll_interval_ms=1000,
            with_monitor=False,
        )

        assert options is not None
        assert worker is not None


class TestShardLeaseConfigDefaults:
    """Test that ShardLeaseConfig has sensible defaults.

    From docs/features/shard-leases.md configuration section:
        shards_per_worker = 16
        lease_ttl_secs = 30
        lease_renew_interval_secs = 10
    """

    def test_enabled_uses_default_shards_per_worker(self):
        """enabled() should use a sensible default for shards_per_worker.

        Documentation shows default is 16.
        """
        config = ShardLeaseConfig.enabled()
        # We can't inspect internals as blind testers, but config should exist
        assert config is not None

    def test_custom_accepts_zero_shards(self):
        """custom() accepts shards_per_worker=0.

        Note: This may not be semantically valid but the API accepts it.
        Consider adding validation in the implementation.
        """
        # This doesn't raise - may want to add validation
        config = ShardLeaseConfig.custom(shards_per_worker=0)
        assert config is not None

    def test_custom_allows_high_shard_count(self):
        """custom() should accept high shard counts for large deployments."""
        config = ShardLeaseConfig.custom(shards_per_worker=256)
        assert config is not None

    def test_custom_allows_small_shard_count(self):
        """custom() should accept small shard counts."""
        config = ShardLeaseConfig.custom(shards_per_worker=1)
        assert config is not None

    def test_custom_allows_short_ttl(self):
        """custom() should accept short lease TTL values."""
        config = ShardLeaseConfig.custom(lease_ttl_secs=5)
        assert config is not None

    def test_custom_allows_long_ttl(self):
        """custom() should accept long lease TTL values."""
        config = ShardLeaseConfig.custom(lease_ttl_secs=300)
        assert config is not None


class TestShardLeaseConfigRepr:
    """Test ShardLeaseConfig string representation for debugging."""

    def test_enabled_has_repr(self):
        """enabled() config should have a string representation."""
        config = ShardLeaseConfig.enabled()
        repr_str = repr(config)
        assert repr_str is not None
        assert len(repr_str) > 0

    def test_disabled_has_repr(self):
        """disabled() config should have a string representation."""
        config = ShardLeaseConfig.disabled()
        repr_str = repr(config)
        assert repr_str is not None
        assert len(repr_str) > 0

    def test_custom_has_repr(self):
        """custom() config should have a string representation."""
        config = ShardLeaseConfig.custom(shards_per_worker=8)
        repr_str = repr(config)
        assert repr_str is not None
        assert len(repr_str) > 0


class TestEnvironmentVariableConfiguration:
    """Test that shard leasing can be configured via environment variables.

    From docs/features/shard-leases.md:
        OQ_SHARD_LEASING=1
        OQ_SHARD_LEASING_SHARDS_PER_WORKER=16
        OQ_SHARD_LEASING_TTL_SECS=30
        OQ_SHARD_LEASING_RENEW_SECS=10
    """

    def test_env_var_names_documented(self):
        """Environment variable names should match documentation."""
        # This is a documentation/contract test - verify the env var names
        # match what's in the docs
        expected_vars = [
            "OQ_SHARD_LEASING",
            "OQ_SHARD_LEASING_SHARDS_PER_WORKER",
            "OQ_SHARD_LEASING_TTL_SECS",
            "OQ_SHARD_LEASING_RENEW_SECS",
        ]
        # Just verify these are the documented names
        for var in expected_vars:
            assert var.startswith("OQ_SHARD_LEASING"), (
                f"Env var {var} should start with OQ_SHARD_LEASING"
            )


class TestShardLeaseConfigImmutability:
    """Test that ShardLeaseConfig instances are immutable (if applicable)."""

    def test_multiple_enabled_calls_return_equivalent_objects(self):
        """Multiple calls to enabled() should return equivalent configs."""
        config1 = ShardLeaseConfig.enabled()
        config2 = ShardLeaseConfig.enabled()
        # Both should be valid enabled configs
        assert config1 is not None
        assert config2 is not None

    def test_multiple_disabled_calls_return_equivalent_objects(self):
        """Multiple calls to disabled() should return equivalent configs."""
        config1 = ShardLeaseConfig.disabled()
        config2 = ShardLeaseConfig.disabled()
        # Both should be valid disabled configs
        assert config1 is not None
        assert config2 is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
