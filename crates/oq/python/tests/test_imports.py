"""Test that all oq imports work correctly."""

from __future__ import annotations


def test_import_connect() -> None:
    """Test connect function import."""
    from oq import connect  # noqa: PLC0415

    assert callable(connect)


def test_import_queue() -> None:
    """Test Queue class import."""
    from oq import Queue  # noqa: PLC0415

    assert Queue is not None


def test_import_task() -> None:
    """Test Task class import."""
    from oq import Task  # noqa: PLC0415

    assert Task is not None


def test_import_task_status() -> None:
    """Test TaskStatus enum import."""
    from oq import TaskStatus  # noqa: PLC0415

    assert TaskStatus is not None
    # Verify enum members exist
    assert hasattr(TaskStatus, "Pending")
    assert hasattr(TaskStatus, "Running")
    assert hasattr(TaskStatus, "Completed")
    assert hasattr(TaskStatus, "Failed")


def test_import_worker() -> None:
    """Test Worker class import."""
    from oq import Worker  # noqa: PLC0415

    assert Worker is not None


def test_import_retry_policy() -> None:
    """Test RetryPolicy class import."""
    from oq import RetryPolicy  # noqa: PLC0415

    assert RetryPolicy is not None


def test_import_exceptions() -> None:
    """Test exception classes import."""
    from oq import PermanentError, RetryableError  # noqa: PLC0415

    assert issubclass(RetryableError, Exception)
    assert issubclass(PermanentError, Exception)


def test_import_reschedule_error() -> None:
    """Test RescheduleError class import."""
    from oq import RescheduleError  # noqa: PLC0415

    assert issubclass(RescheduleError, Exception)
    # Verify it stores delay_seconds
    error = RescheduleError(60)
    assert error.delay_seconds == 60
    assert "60" in str(error)


def test_import_metrics() -> None:
    """Test metrics module import."""
    from oq import metrics  # noqa: PLC0415

    assert metrics is not None
    # Verify functions exist
    assert callable(metrics.enable_prometheus)
    assert callable(metrics.enable_statsd)
    assert callable(metrics.enable_opentelemetry)
    assert callable(metrics.auto_configure)
    assert callable(metrics.current_exporter)


def test_all_exports() -> None:
    """Test that __all__ contains expected exports."""
    import oq  # noqa: PLC0415

    expected = {
        "connect",
        "metrics",
        "PollingStrategy",
        "Queue",
        "RescheduleError",
        "Schedule",
        "ScheduleLastRun",
        "Task",
        "TaskContext",
        "TaskStatus",
        "Worker",
        "WorkerRunOptions",
        "RetryPolicy",
        "RetryableError",
        "PermanentError",
        "Config",
        "WorkerConfig",
        "MonitorConfig",
        "load_config",
        "SchemaValidationError",
        "ShardLeaseConfig",
        "JsonSchemaObject",
        "TaskSchema",
    }
    assert set(oq.__all__) == expected
