"""Typed schema definitions for oq."""

from __future__ import annotations

from typing import TypedDict


class JsonSchemaObject(TypedDict, total=False):
    """Type representing a JSON Schema object."""

    type: str
    properties: dict[str, dict[str, str]]
    required: list[str]


class TaskSchema(TypedDict):
    """Type representing a task schema with input and output JSON schemas."""

    input: JsonSchemaObject
    output: JsonSchemaObject
