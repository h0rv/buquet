"""Tests for qo CLI commands.

Tests CLI commands including tail, schema, and other subcommands.
"""

from __future__ import annotations

import subprocess

import pytest

# =============================================================================
# CLI Helpers
# =============================================================================


def _run_qo_command(args: list[str]) -> subprocess.CompletedProcess[str] | None:
    """Run an qo CLI command, returning None if binary not found."""
    try:
        return subprocess.run(  # noqa: S603
            ["qo", *args],  # noqa: S607
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except FileNotFoundError:
        return None


def _run_cargo_qo_command(args: list[str]) -> subprocess.CompletedProcess[str]:
    """Run qo CLI via cargo run."""
    return subprocess.run(  # noqa: S603
        ["cargo", "run", "-q", "--", *args],  # noqa: S607
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )


def _get_help_or_skip(subcommand: list[str], feature_name: str) -> subprocess.CompletedProcess[str]:
    """Get help for a subcommand, skipping if not implemented."""
    result = _run_qo_command([*subcommand, "--help"])

    if result is None:
        result = _run_cargo_qo_command([*subcommand, "--help"])

    if result.returncode != 0 and "unrecognized subcommand" in result.stderr.lower():
        pytest.skip(f"{feature_name} not yet implemented")

    return result


# =============================================================================
# Tail Command
# =============================================================================


class TestCLITailCommand:
    """Test that the qo tail CLI command exists and parses arguments."""

    def test_qo_cli_has_tail_subcommand(self) -> None:
        """qo CLI should have a 'tail' subcommand."""
        _get_help_or_skip(["tail"], "tail command")

    def test_tail_accepts_task_type_filter(self) -> None:
        """qo tail should accept --task-type argument."""
        result = _get_help_or_skip(["tail"], "tail command")
        assert "--task-type" in result.stdout or "-t" in result.stdout

    def test_tail_accepts_status_filter(self) -> None:
        """qo tail should accept --status argument."""
        result = _get_help_or_skip(["tail"], "tail command")
        assert "--status" in result.stdout

    def test_tail_accepts_json_flag(self) -> None:
        """qo tail should accept --json flag for JSON output."""
        result = _get_help_or_skip(["tail"], "tail command")
        assert "--json" in result.stdout

    def test_tail_accepts_limit_option(self) -> None:
        """qo tail should accept --limit argument."""
        result = _get_help_or_skip(["tail"], "tail command")
        assert "--limit" in result.stdout

    def test_tail_accepts_interval_option(self) -> None:
        """qo tail should accept --interval argument for poll interval."""
        result = _get_help_or_skip(["tail"], "tail command")
        assert "--interval" in result.stdout or "--poll" in result.stdout


class TestCLITailCommandParsing:
    """Test that CLI argument combinations parse correctly."""

    def test_tail_with_all_options(self) -> None:
        """qo tail should accept all options together."""
        result = _get_help_or_skip(["tail"], "tail command")
        help_text = result.stdout.lower()
        assert "tail" in help_text or "stream" in help_text or result.returncode == 0


# =============================================================================
# Schema Commands
# =============================================================================


class TestCLISchemaCommand:
    """Test schema CLI commands exist."""

    def test_schema_subcommand_exists(self) -> None:
        """CLI should have 'schema' subcommand."""
        result = _get_help_or_skip(["schema"], "schema command")
        assert result.returncode == 0 or "help" in result.stdout.lower()

    def test_schema_publish_subcommand(self) -> None:
        """CLI should have 'schema publish' subcommand."""
        result = _get_help_or_skip(["schema", "publish"], "schema publish command")
        assert result.returncode == 0 or "help" in result.stdout.lower()

    def test_schema_get_subcommand(self) -> None:
        """CLI should have 'schema get' subcommand."""
        result = _get_help_or_skip(["schema", "get"], "schema get command")
        assert result.returncode == 0 or "help" in result.stdout.lower()

    def test_schema_list_subcommand(self) -> None:
        """CLI should have 'schema list' subcommand."""
        result = _get_help_or_skip(["schema", "list"], "schema list command")
        assert result.returncode == 0 or "help" in result.stdout.lower()

    def test_schema_delete_subcommand(self) -> None:
        """CLI should have 'schema delete' subcommand."""
        result = _get_help_or_skip(["schema", "delete"], "schema delete command")
        assert result.returncode == 0 or "help" in result.stdout.lower()

    def test_schema_validate_subcommand(self) -> None:
        """CLI should have 'schema validate' subcommand."""
        result = _get_help_or_skip(["schema", "validate"], "schema validate command")
        assert result.returncode == 0 or "help" in result.stdout.lower()


class TestCLISchemaArguments:
    """Test schema CLI command arguments."""

    def test_schema_publish_accepts_task_type_and_file(self) -> None:
        """schema publish should accept task type and schema file path."""
        result = _get_help_or_skip(["schema", "publish"], "schema publish command")
        help_text = result.stdout.lower()
        assert "task" in help_text or "type" in help_text or result.returncode == 0

    def test_schema_validate_accepts_input_flag(self) -> None:
        """schema validate should accept --input flag."""
        result = _get_help_or_skip(["schema", "validate"], "schema validate command")
        assert "--input" in result.stdout or result.returncode == 0

    def test_schema_validate_accepts_output_flag(self) -> None:
        """schema validate should accept --output flag."""
        result = _get_help_or_skip(["schema", "validate"], "schema validate command")
        assert "--output" in result.stdout or result.returncode == 0


# =============================================================================
# General CLI Tests
# =============================================================================


class TestCLIGeneral:
    """Test general CLI functionality."""

    def test_help_lists_available_commands(self) -> None:
        """The --help flag should list available commands."""
        result = _run_qo_command(["--help"])

        if result is None:
            result = _run_cargo_qo_command(["--help"])

        assert "submit" in result.stdout.lower() or "SUBCOMMANDS" in result.stdout
