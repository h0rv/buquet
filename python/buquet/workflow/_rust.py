"""Shared utilities for Rust binding management.

This module provides access to the WorkflowEngine singleton.
Rust bindings are required - no Python fallback.
"""

from __future__ import annotations

import os

import buquet
from buquet.workflow._native import WorkflowEngine


class _EngineHolder:
    """Singleton holder for the WorkflowEngine."""

    engine: WorkflowEngine | None = None


_holder = _EngineHolder()


async def get_engine() -> WorkflowEngine:
    """
    Get or create the WorkflowEngine singleton.

    The engine is created lazily on first access and reused thereafter.
    Uses .buquet.toml config file or environment variables.

    Returns:
        The WorkflowEngine instance

    Raises:
        RuntimeError: If no config is available
    """
    if _holder.engine is None:
        # Try config file, fall back to env vars
        try:
            config = buquet.load_config()
            bucket = os.environ.get("S3_BUCKET") or config.bucket
            endpoint = os.environ.get("S3_ENDPOINT") or config.endpoint
            region = os.environ.get("S3_REGION") or os.environ.get("AWS_REGION") or config.region
        except (OSError, ValueError, KeyError):
            bucket = os.environ.get("S3_BUCKET")
            endpoint = os.environ.get("S3_ENDPOINT")
            region = os.environ.get("S3_REGION") or os.environ.get("AWS_REGION", "us-east-1")

        if bucket is None:
            msg = "S3_BUCKET environment variable or .buquet.toml config required"
            raise RuntimeError(msg)

        _holder.engine = await WorkflowEngine.create(bucket, endpoint, region)

    return _holder.engine
