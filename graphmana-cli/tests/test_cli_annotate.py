"""Tests for the 'graphmana annotate' CLI group."""

from click.testing import CliRunner

from graphmana.cli import cli


class TestAnnotateGroup:
    """Verify the annotate group and subcommands exist."""

    def test_annotate_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["annotate", "--help"])
        assert result.exit_code == 0
        assert "Manage annotation versions" in result.output
        assert "load" in result.output
        assert "list" in result.output
        assert "remove" in result.output

    def test_annotate_load_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["annotate", "load", "--help"])
        assert result.exit_code == 0
        assert "--input" in result.output
        assert "--version" in result.output
        assert "--mode" in result.output
        assert "add" in result.output
        assert "update" in result.output
        assert "replace" in result.output
        assert "--type" in result.output
        assert "auto" in result.output
        assert "vep" in result.output
        assert "snpeff" in result.output
        assert "--description" in result.output
        assert "--batch-size" in result.output

    def test_annotate_list_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["annotate", "list", "--help"])
        assert result.exit_code == 0
        assert "--neo4j-uri" in result.output

    def test_annotate_remove_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["annotate", "remove", "--help"])
        assert result.exit_code == 0
        assert "--version" in result.output
        assert "--neo4j-uri" in result.output

    def test_annotate_load_requires_input(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["annotate", "load", "--version", "v1"])
        assert result.exit_code != 0
        assert "Missing" in result.output or "required" in result.output.lower()

    def test_annotate_load_requires_version(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["annotate", "load", "--input", "/dev/null"])
        assert result.exit_code != 0

    def test_annotate_remove_requires_version(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["annotate", "remove"])
        assert result.exit_code != 0
        assert "Missing" in result.output or "required" in result.output.lower()
