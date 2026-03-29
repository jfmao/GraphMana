"""Tests for the 'graphmana snapshot' CLI group."""

from click.testing import CliRunner

from graphmana.cli import cli


class TestSnapshotGroup:
    """Verify the snapshot group and subcommands exist."""

    def test_snapshot_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["snapshot", "--help"])
        assert result.exit_code == 0
        assert "snapshot" in result.output.lower()
        assert "create" in result.output
        assert "list" in result.output
        assert "restore" in result.output
        assert "delete" in result.output

    def test_snapshot_create_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["snapshot", "create", "--help"])
        assert result.exit_code == 0
        assert "--name" in result.output
        assert "--neo4j-home" in result.output
        assert "--database" in result.output
        assert "--snapshot-dir" in result.output

    def test_snapshot_list_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["snapshot", "list", "--help"])
        assert result.exit_code == 0
        assert "--snapshot-dir" in result.output

    def test_snapshot_restore_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["snapshot", "restore", "--help"])
        assert result.exit_code == 0
        assert "--name" in result.output
        assert "--neo4j-home" in result.output
        assert "--database" in result.output

    def test_snapshot_delete_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["snapshot", "delete", "--help"])
        assert result.exit_code == 0
        assert "--name" in result.output

    def test_snapshot_create_requires_name(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["snapshot", "create", "--neo4j-home", "/tmp"])
        assert result.exit_code != 0

    def test_snapshot_create_requires_neo4j_home(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["snapshot", "create", "--name", "test"])
        assert result.exit_code != 0

    def test_snapshot_restore_requires_name(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["snapshot", "restore", "--neo4j-home", "/tmp"])
        assert result.exit_code != 0

    def test_snapshot_delete_requires_name(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["snapshot", "delete"])
        assert result.exit_code != 0

    def test_snapshot_list_empty(self, tmp_path):
        runner = CliRunner()
        result = runner.invoke(cli, ["snapshot", "list", "--snapshot-dir", str(tmp_path)])
        assert result.exit_code == 0
        assert "No snapshots found" in result.output

    def test_snapshot_list_with_dumps(self, tmp_path):
        (tmp_path / "backup1.dump").write_bytes(b"x" * 1024)
        runner = CliRunner()
        result = runner.invoke(cli, ["snapshot", "list", "--snapshot-dir", str(tmp_path)])
        assert result.exit_code == 0
        assert "backup1" in result.output

    def test_snapshot_delete_missing(self, tmp_path):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["snapshot", "delete", "--name", "nonexistent", "--snapshot-dir", str(tmp_path)],
        )
        assert result.exit_code != 0
        assert "not found" in result.output.lower()
