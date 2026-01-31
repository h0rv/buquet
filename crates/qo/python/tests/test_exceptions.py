"""Test qo exception classes."""

from __future__ import annotations

import pytest

from qo import PermanentError, RetryableError


def test_retryable_error_can_be_raised() -> None:
    """Test RetryableError can be raised and caught."""
    msg = "transient failure"
    with pytest.raises(RetryableError, match=msg):
        raise RetryableError(msg)


def test_permanent_error_can_be_raised() -> None:
    """Test PermanentError can be raised and caught."""
    msg = "fatal error"
    with pytest.raises(PermanentError, match=msg):
        raise PermanentError(msg)


def test_retryable_error_is_exception() -> None:
    """Test RetryableError inherits from Exception."""
    err = RetryableError("test")
    assert isinstance(err, Exception)
    assert str(err) == "test"


def test_permanent_error_is_exception() -> None:
    """Test PermanentError inherits from Exception."""
    err = PermanentError("test")
    assert isinstance(err, Exception)
    assert str(err) == "test"
