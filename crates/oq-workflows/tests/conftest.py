"""Shared test fixtures for oq-workflows."""

from __future__ import annotations

import os

import pytest

from oq_workflows import StepContext, Workflow

# Skip marker for tests requiring S3
requires_s3 = pytest.mark.skipif(
    not os.environ.get("S3_BUCKET"),
    reason="S3_BUCKET not set - skipping S3-dependent tests",
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
