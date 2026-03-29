"""Tests for the cohort CLI command group."""

from click.testing import CliRunner

from graphmana.cli import cli

runner = CliRunner()


class TestCohortDefineHelp:
    """cohort define subcommand structure."""

    def test_help(self):
        result = runner.invoke(cli, ["cohort", "define", "--help"])
        assert result.exit_code == 0
        assert "--name" in result.output
        assert "--query" in result.output

    def test_requires_name(self):
        result = runner.invoke(cli, ["cohort", "define", "--query", "RETURN 1"])
        assert result.exit_code != 0

    def test_requires_query(self):
        result = runner.invoke(cli, ["cohort", "define", "--name", "test"])
        assert result.exit_code != 0


class TestCohortListHelp:
    """cohort list subcommand structure."""

    def test_help(self):
        result = runner.invoke(cli, ["cohort", "list", "--help"])
        assert result.exit_code == 0
        assert "--neo4j-uri" in result.output


class TestCohortShowHelp:
    """cohort show subcommand structure."""

    def test_help(self):
        result = runner.invoke(cli, ["cohort", "show", "--help"])
        assert result.exit_code == 0
        assert "--name" in result.output

    def test_requires_name(self):
        result = runner.invoke(cli, ["cohort", "show"])
        assert result.exit_code != 0


class TestCohortDeleteHelp:
    """cohort delete subcommand structure."""

    def test_help(self):
        result = runner.invoke(cli, ["cohort", "delete", "--help"])
        assert result.exit_code == 0
        assert "--name" in result.output

    def test_requires_name(self):
        result = runner.invoke(cli, ["cohort", "delete"])
        assert result.exit_code != 0


class TestCohortCountHelp:
    """cohort count subcommand structure."""

    def test_help(self):
        result = runner.invoke(cli, ["cohort", "count", "--help"])
        assert result.exit_code == 0
        assert "--name" in result.output

    def test_requires_name(self):
        result = runner.invoke(cli, ["cohort", "count"])
        assert result.exit_code != 0


class TestCohortValidateHelp:
    """cohort validate subcommand structure."""

    def test_help(self):
        result = runner.invoke(cli, ["cohort", "validate", "--help"])
        assert result.exit_code == 0
        assert "--query" in result.output

    def test_requires_query(self):
        result = runner.invoke(cli, ["cohort", "validate"])
        assert result.exit_code != 0


class TestCohortGroupHelp:
    """cohort group help shows all subcommands."""

    def test_group_help(self):
        result = runner.invoke(cli, ["cohort", "--help"])
        assert result.exit_code == 0
        for sub in ["define", "list", "show", "delete", "count", "validate"]:
            assert sub in result.output


class TestExportFilterCohortOption:
    """--filter-cohort option on export command."""

    def test_filter_cohort_in_help(self):
        result = runner.invoke(cli, ["export", "--help"])
        assert result.exit_code == 0
        assert "--filter-cohort" in result.output
