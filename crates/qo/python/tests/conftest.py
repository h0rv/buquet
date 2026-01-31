"""Pytest configuration and shared fixtures for qo tests."""

from __future__ import annotations

import os
from typing import Any, cast

import boto3  # pyright: ignore[reportMissingTypeStubs]
import pytest
from botocore.config import Config as BotoConfig  # pyright: ignore[reportMissingTypeStubs]
from botocore.exceptions import (  # pyright: ignore[reportMissingTypeStubs]
    ClientError,
    EndpointConnectionError,
)

# =============================================================================
# Pytest Configuration
# =============================================================================


def pytest_configure(config: pytest.Config) -> None:
    """Register custom markers."""
    config.addinivalue_line("markers", "integration: marks tests as integration tests (require S3)")


def _get_s3_endpoint_for_test() -> str | None:
    """Get S3 endpoint for tests.

    TEST-ONLY: Defaults to LocalStack for developer convenience.
    The library itself does NOT have this default - it uses real AWS.

    To test against real AWS, set S3_ENDPOINT= (empty).
    """
    endpoint_env = os.environ.get("S3_ENDPOINT")
    if endpoint_env is None:
        return "http://localhost:4566"  # Test default = LocalStack
    if endpoint_env == "":
        return None  # Explicitly empty = real AWS
    return endpoint_env


@pytest.fixture(scope="session", autouse=True)
def clean_s3_bucket_before_tests():
    """Clean the S3 test bucket before running tests.

    This prevents stale data from previous test runs from causing
    workers to hang while polling for tasks in unexpected states.
    """
    endpoint = _get_s3_endpoint_for_test()
    bucket = os.environ.get("S3_BUCKET", "qo-dev")
    region = os.environ.get("S3_REGION", "us-east-1")

    # Create S3 client
    # For real AWS, don't pass fake credentials - use the default credential chain
    client_kwargs: dict[str, Any] = {
        "region_name": region,
        "config": BotoConfig(signature_version="s3v4"),
    }
    if endpoint is not None:
        client_kwargs["endpoint_url"] = endpoint
        # LocalStack doesn't require real credentials
        client_kwargs["aws_access_key_id"] = os.environ.get("AWS_ACCESS_KEY_ID", "test")
        client_kwargs["aws_secret_access_key"] = os.environ.get("AWS_SECRET_ACCESS_KEY", "test")

    s3: Any = boto3.client("s3", **client_kwargs)  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

    # Delete all objects in the bucket
    try:
        paginator: Any = s3.get_paginator(  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            "list_objects_v2"
        )
        for page in paginator.paginate(  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            Bucket=bucket
        ):
            page_dict = cast("dict[str, Any]", page)
            objects = cast("list[dict[str, Any]]", page_dict.get("Contents", []))
            if objects:
                delete_keys: list[dict[str, str]] = [
                    {"Key": cast("str", obj["Key"])} for obj in objects
                ]
                s3.delete_objects(  # pyright: ignore[reportUnknownMemberType]
                    Bucket=bucket, Delete={"Objects": delete_keys}
                )
    except ClientError:
        # Bucket doesn't exist yet or permission issues - that's fine
        pass
    except EndpointConnectionError:
        # S3/LocalStack not running - tests will fail anyway
        pass


# =============================================================================
# S3 Configuration Helpers
# =============================================================================


def get_s3_endpoint() -> str | None:
    """Get S3 endpoint from environment (for tests).

    TEST-ONLY: Defaults to LocalStack for developer convenience.
    The library itself defaults to real AWS when no endpoint is set.

    Returns:
        - None if S3_ENDPOINT="" (use real AWS)
        - "http://localhost:4566" if S3_ENDPOINT not set (test default)
        - The value of S3_ENDPOINT otherwise
    """
    return _get_s3_endpoint_for_test()


def get_s3_bucket() -> str:
    """Get S3 bucket from environment."""
    return os.environ.get("S3_BUCKET", "qo-dev")


def get_s3_region() -> str:
    """Get S3 region from environment."""
    return os.environ.get("S3_REGION", "us-east-1")


# =============================================================================
# Shared Fixtures
# =============================================================================


@pytest.fixture
def sample_schema() -> dict[str, Any]:
    """Sample JSON Schema for testing."""
    return {
        "input": {
            "type": "object",
            "properties": {
                "to": {"type": "string"},
                "subject": {"type": "string"},
            },
            "required": ["to", "subject"],
        },
        "output": {
            "type": "object",
            "properties": {
                "sent": {"type": "boolean"},
                "message_id": {"type": "string"},
            },
            "required": ["sent"],
        },
    }


@pytest.fixture
def minimal_schema() -> dict[str, Any]:
    """Minimal schema with basic input/output."""
    return {
        "input": {
            "type": "object",
            "properties": {"value": {"type": "string"}},
            "required": ["value"],
        },
        "output": {
            "type": "object",
            "properties": {"result": {"type": "boolean"}},
        },
    }


@pytest.fixture
async def queue():  # noqa: ANN201
    """Create a queue connection for testing.

    This is a shared fixture that handles S3 endpoint configuration correctly
    for both LocalStack (default) and real AWS (when S3_ENDPOINT="").
    """
    # Import here to avoid import-time S3 connection issues in test collection
    from qo import connect  # noqa: PLC0415

    return await connect(
        endpoint=get_s3_endpoint(),
        bucket=get_s3_bucket(),
        region=get_s3_region(),
    )
