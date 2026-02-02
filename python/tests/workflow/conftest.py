"""Shared test fixtures for buquet-workflow."""

from __future__ import annotations

import os

import pytest

import buquet
from buquet.workflow import StepContext, Workflow


def pytest_configure(config: pytest.Config) -> None:
    """Register custom markers."""
    config.addinivalue_line("markers", "integration: marks tests as integration tests (require S3)")


def _get_s3_endpoint() -> str | None:
    """Get S3 endpoint (test default: LocalStack).

    TEST-ONLY: Defaults to LocalStack for developer convenience.
    The library itself defaults to real AWS when no endpoint is set.
    """
    endpoint_env = os.environ.get("S3_ENDPOINT")
    if endpoint_env is None:
        return "http://localhost:4566"  # Test default
    if endpoint_env == "":
        return None  # Explicitly empty = real AWS
    return endpoint_env


def _get_s3_bucket() -> str:
    """Get S3 bucket (test default: buquet-dev)."""
    return os.environ.get("S3_BUCKET", "buquet-dev")


def _get_s3_region() -> str:
    """Get S3 region (test default: us-east-1)."""
    return os.environ.get("S3_REGION", "us-east-1")


@pytest.fixture
async def queue() -> buquet.Queue:
    """Connect to buquet queue (test default: LocalStack)."""
    return await buquet.connect(
        endpoint=_get_s3_endpoint(),
        bucket=_get_s3_bucket(),
        region=_get_s3_region(),
    )


@pytest.fixture
def sample_workflow() -> Workflow:
    """Create a simple 3-step workflow for testing."""
    wf = Workflow("test_workflow")

    @wf.step("step_a")
    async def step_a(_ctx: StepContext) -> dict[str, str]:
        return {"a": "done"}

    @wf.step("step_b", depends_on=["step_a"])
    async def step_b(_ctx: StepContext) -> dict[str, str]:
        return {"b": "done"}

    @wf.step("step_c", depends_on=["step_a"])
    async def step_c(_ctx: StepContext) -> dict[str, str]:
        return {"c": "done"}

    return wf


@pytest.fixture
def diamond_workflow() -> Workflow:
    """Create a diamond-shaped DAG workflow for testing."""
    wf = Workflow("diamond_workflow")

    @wf.step("start")
    async def start(_ctx: StepContext) -> dict[str, bool]:
        return {"started": True}

    @wf.step("left", depends_on=["start"])
    async def left(_ctx: StepContext) -> dict[str, bool]:
        return {"left": True}

    @wf.step("right", depends_on=["start"])
    async def right(_ctx: StepContext) -> dict[str, bool]:
        return {"right": True}

    @wf.step("end", depends_on=["left", "right"])
    async def end(_ctx: StepContext) -> dict[str, bool]:
        return {"ended": True}

    return wf


@pytest.fixture
def linear_workflow() -> Workflow:
    """Create a linear (sequential) workflow for testing."""
    wf = Workflow("linear_workflow")

    @wf.step("step_1")
    async def step_1(_ctx: StepContext) -> dict[str, int]:
        return {"step": 1}

    @wf.step("step_2", depends_on=["step_1"])
    async def step_2(_ctx: StepContext) -> dict[str, int]:
        return {"step": 2}

    @wf.step("step_3", depends_on=["step_2"])
    async def step_3(_ctx: StepContext) -> dict[str, int]:
        return {"step": 3}

    return wf
