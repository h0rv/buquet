"""Type aliases for buquet-workflow.

This module provides reusable type definitions for the buquet-workflow library.
These are primarily for documentation and can be used with generics in the future.
"""

from __future__ import annotations

from typing import Any, TypeVar

# Generic type variables for workflow data
TData = TypeVar("TData", bound="dict[str, Any]")
TOutput = TypeVar("TOutput")
TInput = TypeVar("TInput")
