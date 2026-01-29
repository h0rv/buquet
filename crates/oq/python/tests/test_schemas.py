"""Tests for task schema feature.

Tests the schema API methods on Queue for publishing, retrieving, and validating schemas.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

import pytest

from oq import Queue, connect

if TYPE_CHECKING:
    from oq.typing import TaskSchema

# =============================================================================
# Schema API Methods Exist
# =============================================================================


class TestSchemaAPIExists:
    """Test that schema API methods exist on Queue."""

    def test_publish_schema_exists(self) -> None:
        """Queue should have publish_schema method."""
        assert hasattr(Queue, "publish_schema")

    def test_get_schema_exists(self) -> None:
        """Queue should have get_schema method."""
        assert hasattr(Queue, "get_schema")

    def test_list_schemas_exists(self) -> None:
        """Queue should have list_schemas method."""
        assert hasattr(Queue, "list_schemas")

    def test_delete_schema_exists(self) -> None:
        """Queue should have delete_schema method."""
        assert hasattr(Queue, "delete_schema")

    def test_validate_input_exists(self) -> None:
        """Queue should have validate_input method."""
        assert hasattr(Queue, "validate_input")

    def test_validate_output_exists(self) -> None:
        """Queue should have validate_output method."""
        assert hasattr(Queue, "validate_output")


# =============================================================================
# Schema Validation Logic
# =============================================================================


class TestSchemaValidation:
    """Test schema validation logic."""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_validate_input_valid(self, sample_schema: TaskSchema) -> None:
        """Valid input should pass validation."""
        queue = await connect()
        queue.validate_input(sample_schema, {"to": "user@example.com", "subject": "Hi"})

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_validate_input_invalid_missing_required(self, sample_schema: TaskSchema) -> None:
        """Input missing required field should raise ValueError."""
        queue = await connect()
        with pytest.raises(ValueError, match=r".*"):
            queue.validate_input(sample_schema, {"to": "user@example.com"})

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_validate_input_invalid_wrong_type(self, sample_schema: TaskSchema) -> None:
        """Input with wrong type should raise ValueError."""
        queue = await connect()
        with pytest.raises(ValueError, match=r".*"):
            queue.validate_input(sample_schema, {"to": 123, "subject": "Hi"})

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_validate_output_valid(self, sample_schema: TaskSchema) -> None:
        """Valid output should pass validation."""
        queue = await connect()
        queue.validate_output(sample_schema, {"sent": True, "message_id": "abc"})

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_validate_output_invalid(self, sample_schema: TaskSchema) -> None:
        """Invalid output should raise ValueError."""
        queue = await connect()
        with pytest.raises(ValueError, match=r".*"):
            queue.validate_output(sample_schema, {"sent": "yes"})


def _unique_task_type(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestSchemaStructure:
    """Test schema structure expectations."""

    def test_schema_has_input_and_output(self, sample_schema: TaskSchema) -> None:
        """Schema should contain input and output JSON Schema definitions."""
        assert "input" in sample_schema
        assert "output" in sample_schema
        assert "type" in sample_schema["input"]
        assert "type" in sample_schema["output"]


# =============================================================================
# Integration Tests (require actual S3 connection)
# =============================================================================


@pytest.mark.integration
@pytest.mark.asyncio
class TestSchemaPublishAndRetrieve:
    """Integration tests for schema publish and retrieve operations."""

    async def test_publish_schema_stores_schema(self, sample_schema: TaskSchema) -> None:
        """publish_schema should store the schema for later retrieval."""
        queue = await connect()
        task_type = _unique_task_type("schema-publish")
        try:
            await queue.publish_schema(task_type, sample_schema)
            stored = await queue.get_schema(task_type)
            assert stored is not None
            assert stored["input"].get("type") == "object"
        finally:
            await queue.delete_schema(task_type)

    async def test_get_schema_returns_stored_schema(self, sample_schema: TaskSchema) -> None:
        """get_schema should return previously published schema."""
        queue = await connect()
        task_type = _unique_task_type("schema-get")
        try:
            await queue.publish_schema(task_type, sample_schema)
            stored = await queue.get_schema(task_type)
            assert stored == sample_schema
        finally:
            await queue.delete_schema(task_type)

    async def test_get_schema_returns_none_for_nonexistent(self) -> None:
        """get_schema should return None for non-existent task type."""
        queue = await connect()
        missing = await queue.get_schema(_unique_task_type("schema-missing"))
        assert missing is None

    async def test_list_schemas_returns_task_types(self, sample_schema: TaskSchema) -> None:
        """list_schemas should return list of task type names with schemas."""
        queue = await connect()
        task_type = _unique_task_type("schema-list")
        try:
            await queue.publish_schema(task_type, sample_schema)
            schemas = await queue.list_schemas()
            assert task_type in schemas
        finally:
            await queue.delete_schema(task_type)

    async def test_delete_schema_removes_schema(self, sample_schema: TaskSchema) -> None:
        """delete_schema should remove the schema."""
        queue = await connect()
        task_type = _unique_task_type("schema-delete")
        await queue.publish_schema(task_type, sample_schema)
        await queue.delete_schema(task_type)
        assert await queue.get_schema(task_type) is None


@pytest.mark.integration
@pytest.mark.asyncio
class TestSchemaValidationIntegration:
    """Integration tests for schema validation."""

    async def test_validate_input_with_valid_data(self, sample_schema: TaskSchema) -> None:
        """validate_input should not raise for valid data."""
        queue = await connect()
        task_type = _unique_task_type("schema-validate-input-ok")
        try:
            await queue.publish_schema(task_type, sample_schema)
            schema = await queue.get_schema(task_type)
            assert schema is not None
            queue.validate_input(schema, {"to": "user@example.com", "subject": "Hi"})
        finally:
            await queue.delete_schema(task_type)

    async def test_validate_input_raises_valueerror_for_invalid(
        self, sample_schema: TaskSchema
    ) -> None:
        """validate_input should raise ValueError for invalid data."""
        queue = await connect()
        task_type = _unique_task_type("schema-validate-input-bad")
        try:
            await queue.publish_schema(task_type, sample_schema)
            schema = await queue.get_schema(task_type)
            assert schema is not None
            with pytest.raises(ValueError, match=r".*"):
                queue.validate_input(schema, {"to": "user@example.com"})
        finally:
            await queue.delete_schema(task_type)

    async def test_validate_output_with_valid_data(self, sample_schema: TaskSchema) -> None:
        """validate_output should not raise for valid data."""
        queue = await connect()
        task_type = _unique_task_type("schema-validate-output-ok")
        try:
            await queue.publish_schema(task_type, sample_schema)
            schema = await queue.get_schema(task_type)
            assert schema is not None
            queue.validate_output(schema, {"sent": True, "message_id": "abc"})
        finally:
            await queue.delete_schema(task_type)

    async def test_validate_output_raises_valueerror_for_invalid(
        self, sample_schema: TaskSchema
    ) -> None:
        """validate_output should raise ValueError for invalid data."""
        queue = await connect()
        task_type = _unique_task_type("schema-validate-output-bad")
        try:
            await queue.publish_schema(task_type, sample_schema)
            schema = await queue.get_schema(task_type)
            assert schema is not None
            with pytest.raises(ValueError, match=r".*"):
                queue.validate_output(schema, {"sent": "yes"})
        finally:
            await queue.delete_schema(task_type)


@pytest.mark.integration
@pytest.mark.asyncio
class TestSchemaWorkflow:
    """Integration tests for complete schema workflow."""

    async def test_full_schema_lifecycle(self, sample_schema: TaskSchema) -> None:
        """Test publish -> get -> validate -> delete workflow."""
        queue = await connect()
        task_type = _unique_task_type("schema-lifecycle")
        await queue.publish_schema(task_type, sample_schema)
        schema = await queue.get_schema(task_type)
        assert schema is not None
        queue.validate_input(schema, {"to": "user@example.com", "subject": "Hi"})
        queue.validate_output(schema, {"sent": True, "message_id": "abc"})
        await queue.delete_schema(task_type)
        assert await queue.get_schema(task_type) is None

    async def test_schema_update_overwrites_existing(self, sample_schema: TaskSchema) -> None:
        """Publishing schema for existing task type should overwrite."""
        queue = await connect()
        task_type = _unique_task_type("schema-update")
        updated_schema: TaskSchema = {
            "input": {
                "type": "object",
                "properties": {"value": {"type": "string"}},
                "required": ["value"],
            },
            "output": {"type": "object", "properties": {"ok": {"type": "boolean"}}},
        }
        try:
            await queue.publish_schema(task_type, sample_schema)
            await queue.publish_schema(task_type, updated_schema)
            schema = await queue.get_schema(task_type)
            assert schema == updated_schema
        finally:
            await queue.delete_schema(task_type)

    async def test_multiple_schemas_can_coexist(self, sample_schema: TaskSchema) -> None:
        """Multiple task types can have their own schemas."""
        queue = await connect()
        task_type_a = _unique_task_type("schema-multi-a")
        task_type_b = _unique_task_type("schema-multi-b")
        try:
            await queue.publish_schema(task_type_a, sample_schema)
            await queue.publish_schema(task_type_b, sample_schema)
            schemas = await queue.list_schemas()
            assert task_type_a in schemas
            assert task_type_b in schemas
        finally:
            await queue.delete_schema(task_type_a)
            await queue.delete_schema(task_type_b)
