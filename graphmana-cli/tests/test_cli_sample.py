"""Tests for the 'graphmana sample' CLI group."""

from click.testing import CliRunner

from graphmana.cli import cli


class TestSampleGroup:
    """Verify the sample group and subcommands exist."""

    def test_sample_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["sample", "--help"])
        assert result.exit_code == 0
        assert "Manage samples" in result.output
        assert "remove" in result.output
        assert "restore" in result.output
        assert "list" in result.output

    def test_sample_remove_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["sample", "remove", "--help"])
        assert result.exit_code == 0
        assert "--sample-ids" in result.output
        assert "--sample-list" in result.output
        assert "--reason" in result.output
        assert "--neo4j-uri" in result.output

    def test_sample_restore_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["sample", "restore", "--help"])
        assert result.exit_code == 0
        assert "--sample-ids" in result.output
        assert "--sample-list" in result.output
        assert "--neo4j-uri" in result.output

    def test_sample_list_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["sample", "list", "--help"])
        assert result.exit_code == 0
        assert "--population" in result.output
        assert "--show-excluded" in result.output
        assert "--neo4j-uri" in result.output

    def test_sample_remove_requires_ids(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["sample", "remove"])
        assert result.exit_code != 0
        assert "Error" in result.output or "provide" in result.output

    def test_sample_restore_requires_ids(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["sample", "restore"])
        assert result.exit_code != 0
        assert "Error" in result.output or "provide" in result.output

    def test_sample_reassign_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["sample", "reassign", "--help"])
        assert result.exit_code == 0
        assert "--sample-ids" in result.output
        assert "--sample-list" in result.output
        assert "--new-population" in result.output
        assert "--batch-size" in result.output

    def test_sample_reassign_requires_ids(self):
        runner = CliRunner()
        result = runner.invoke(
            cli, ["sample", "reassign", "--new-population", "POP_X"]
        )
        assert result.exit_code != 0
        assert "Error" in result.output or "provide" in result.output

    def test_sample_hard_remove_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["sample", "hard-remove", "--help"])
        assert result.exit_code == 0
        assert "--sample-ids" in result.output
        assert "--sample-list" in result.output
        assert "--require-soft-deleted" in result.output
        assert "--batch-size" in result.output
        assert "--yes" in result.output

    def test_sample_hard_remove_requires_ids(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["sample", "hard-remove", "--yes"])
        assert result.exit_code != 0
        assert "Error" in result.output or "provide" in result.output

    def test_sample_group_shows_new_commands(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["sample", "--help"])
        assert "reassign" in result.output
        assert "hard-remove" in result.output
