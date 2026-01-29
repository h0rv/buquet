"""Shared utilities for Rust binding management.

This module provides access to the WorkflowEngine singleton.
Rust bindings are required - no Python fallback.
"""

from __future__ import annotations

import os

from oq_workflows._oq_workflows import WorkflowEngine


class _EngineHolder:
    """Singleton holder for the WorkflowEngine."""

    engine: WorkflowEngine | None = None


_holder = _EngineHolder()


async def get_engine() -> WorkflowEngine:
    """
    Get or create the WorkflowEngine singleton.

    The engine is created lazily on first access and reused thereafter.
    Requires S3_BUCKET environment variable to be set.

    Returns:
        The WorkflowEngine instance

    Raises:
        RuntimeError: If S3_BUCKET is not set
    """
    if _holder.engine is None:
        bucket = os.environ.get("S3_BUCKET")
        if bucket is None:
            msg = "S3_BUCKET environment variable is required"
            raise RuntimeError(msg)

        endpoint = os.environ.get("S3_ENDPOINT")
        region = os.environ.get("S3_REGION") or os.environ.get("AWS_REGION", "us-east-1")

        _holder.engine = await WorkflowEngine.create(bucket, endpoint, region)

    return _holder.engine
