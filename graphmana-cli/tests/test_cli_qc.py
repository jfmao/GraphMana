"""Tests for the 'graphmana qc' CLI command."""

from click.testing import CliRunner

from graphmana.cli import cli


class TestQCCommand:
    """Verify the qc command exists and has correct options."""

    def test_qc_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["qc", "--help"])
        assert result.exit_code == 0
        assert "quality control" in result.output.lower() or "QC" in result.output
        assert "--type" in result.output
        assert "--output" in result.output
        assert "--format" in result.output

    def test_qc_type_choices(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["qc", "--help"])
        assert "sample" in result.output
        assert "variant" in result.output
        assert "batch" in result.output
        assert "all" in result.output

    def test_qc_format_choices(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["qc", "--help"])
        assert "tsv" in result.output
        assert "json" in result.output
        assert "html" in result.output

    def test_qc_requires_output(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["qc"])
        assert result.exit_code != 0
        assert "Missing" in result.output or "required" in result.output.lower()

    def test_qc_has_neo4j_options(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["qc", "--help"])
        assert "--neo4j-uri" in result.output
        assert "--neo4j-user" in result.output
        assert "--neo4j-password" in result.output

    def test_qc_no_threads_option(self):
        """--threads was removed because QCManager.run() doesn't support parallelism."""
        runner = CliRunner()
        result = runner.invoke(cli, ["qc", "--help"])
        assert "--threads" not in result.output
