"""Pytest configuration and shared fixtures for oq tests."""

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


@pytest.fixture(scope="session", autouse=True)
def clean_s3_bucket_before_tests():
    """Clean the S3 test bucket before running tests.

    This prevents stale data from previous test runs from causing
    workers to hang while polling for tasks in unexpected states.
    """
    endpoint = os.environ.get("S3_ENDPOINT", "http://localhost:4566")
    bucket = os.environ.get("S3_BUCKET", "oq-dev")
    region = os.environ.get("S3_REGION", "us-east-1")

    # Create S3 client
    s3: Any = boto3.client(  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
        "s3",
        endpoint_url=endpoint,
        region_name=region,
        config=BotoConfig(signature_version="s3v4"),
        # LocalStack doesn't require real credentials
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
    )

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
