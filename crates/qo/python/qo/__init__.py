"""qo - S3-Only Task Queue for Python."""

from __future__ import annotations

from qo._qo import (
    Config,
    MonitorConfig,
    PollingStrategy,
    Queue,
    RetryPolicy,
    Schedule,
    ScheduleLastRun,
    ShardLeaseConfig,
    Task,
    TaskContext,
    TaskStatus,
    Worker,
    WorkerConfig,
    WorkerRunOptions,
    connect,
    metrics,
)
from qo._qo import (
    py_load_config as load_config,
)
from qo.typing import JsonSchemaObject, TaskSchema


class RetryableError(Exception):
    """Raise to retry the task."""


class PermanentError(Exception):
    """Raise to fail permanently without retry."""


class SchemaValidationError(ValueError):
    """Exception raised when schema validation fails."""


class RescheduleError(Exception):
    """Raise to reschedule the task for later execution."""

    def __init__(self, delay_seconds: int):
        self.delay_seconds = delay_seconds
        super().__init__(f"Reschedule after {delay_seconds} seconds")


__version__ = "0.1.0"

__all__ = [
    "Config",
    "JsonSchemaObject",
    "MonitorConfig",
    "PermanentError",
    "PollingStrategy",
    "Queue",
    "RescheduleError",
    "RetryPolicy",
    "RetryableError",
    "Schedule",
    "ScheduleLastRun",
    "SchemaValidationError",
    "ShardLeaseConfig",
    "Task",
    "TaskContext",
    "TaskSchema",
    "TaskStatus",
    "Worker",
    "WorkerConfig",
    "WorkerRunOptions",
    "connect",
    "load_config",
    "metrics",
]
