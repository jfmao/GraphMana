"""Tests for the graphmana status command."""

from click.testing import CliRunner

from graphmana.cli import cli


def test_status_help():
    """Verify status command exists and shows help."""
    runner = CliRunner()
    result = runner.invoke(cli, ["status", "--help"])
    assert result.exit_code == 0
    assert "Show database status" in result.output


def test_version():
    """Verify --version flag works."""
    runner = CliRunner()
    result = runner.invoke(cli, ["--version"])
    assert result.exit_code == 0
    assert "1.1.0" in result.output
