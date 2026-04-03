"""Tests for graphmana db validate command."""

from click.testing import CliRunner

from graphmana.cli import cli


class TestValidateCommandHelp:
    """Test that the validate command is registered and shows help."""

    def test_validate_in_db_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["db", "--help"])
        assert "validate" in result.output

    def test_validate_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["db", "validate", "--help"])
        assert result.exit_code == 0
        assert "packed array" in result.output.lower() or "integrity" in result.output.lower()

    def test_validate_has_fix_option(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["db", "validate", "--help"])
        assert "--fix" in result.output

    def test_validate_has_neo4j_options(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["db", "validate", "--help"])
        assert "--neo4j-uri" in result.output
        assert "--neo4j-user" in result.output
        assert "--neo4j-password" in result.output
        assert "--database" in result.output
