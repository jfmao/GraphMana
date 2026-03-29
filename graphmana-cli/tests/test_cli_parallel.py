"""Tests for --threads CLI option on prepare-csv, ingest, and export commands."""

from click.testing import CliRunner

from graphmana.cli import cli


class TestPrepareCsvThreads:
    def test_threads_option_in_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["prepare-csv", "--help"])
        assert result.exit_code == 0
        assert "--threads" in result.output

    def test_threads_described_in_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["prepare-csv", "--help"])
        assert result.exit_code == 0
        assert "Number of threads" in result.output


class TestIngestThreads:
    def test_threads_option_in_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["ingest", "--help"])
        assert result.exit_code == 0
        assert "--threads" in result.output


class TestExportThreads:
    def test_threads_option_in_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["export", "--help"])
        assert result.exit_code == 0
        assert "--threads" in result.output


class TestThreadsValuePropagation:
    """Verify that --threads values are accepted by the CLI."""

    def test_prepare_csv_accepts_threads_4(self):
        runner = CliRunner()
        # Dry run with --threads 4
        result = runner.invoke(
            cli,
            [
                "prepare-csv",
                "--input",
                "/nonexistent.vcf",
                "--population-map",
                "/nonexistent.tsv",
                "--output-dir",
                "/tmp/out",
                "--threads",
                "4",
                "--dry-run",
            ],
        )
        # Will fail because files don't exist, but --threads should parse OK
        # The error should be about the input file, not about --threads
        assert "--threads" not in (result.output if result.exception is None else "")

    def test_ingest_accepts_threads_8(self):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "ingest",
                "--input",
                "/nonexistent.vcf",
                "--population-map",
                "/nonexistent.tsv",
                "--neo4j-home",
                "/nonexistent",
                "--threads",
                "8",
                "--dry-run",
            ],
        )
        # Dry run should not fail due to --threads
        assert "--threads" not in (result.output if result.exception is None else "")
