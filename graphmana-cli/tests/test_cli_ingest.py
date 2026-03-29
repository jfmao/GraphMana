"""Tests for CLI ingest commands — help text and option registration."""

import pytest
from click.testing import CliRunner

from graphmana.cli import cli


@pytest.fixture
def runner():
    return CliRunner()


class TestPrepareCsvHelp:
    def test_shows_help(self, runner):
        result = runner.invoke(cli, ["prepare-csv", "--help"])
        assert result.exit_code == 0
        assert "Generate CSV files from VCF" in result.output

    def test_required_options_enforced(self, runner):
        result = runner.invoke(cli, ["prepare-csv"])
        assert result.exit_code != 0
        assert "Missing" in result.output or "Error" in result.output

    def test_filter_options_registered(self, runner):
        result = runner.invoke(cli, ["prepare-csv", "--help"])
        assert "--filter-min-qual" in result.output
        assert "--filter-min-call-rate" in result.output
        assert "--filter-maf-min" in result.output
        assert "--filter-maf-max" in result.output
        assert "--filter-variant-type" in result.output

    def test_vep_options_registered(self, runner):
        result = runner.invoke(cli, ["prepare-csv", "--help"])
        assert "--vep-vcf" in result.output
        assert "--annotation-version" in result.output


class TestLoadCsvHelp:
    def test_shows_help(self, runner):
        result = runner.invoke(cli, ["load-csv", "--help"])
        assert result.exit_code == 0
        assert "Load pre-generated CSVs" in result.output

    def test_required_options(self, runner):
        result = runner.invoke(cli, ["load-csv", "--help"])
        assert "--csv-dir" in result.output
        assert "--neo4j-home" in result.output


class TestIngestHelp:
    def test_shows_help(self, runner):
        result = runner.invoke(cli, ["ingest", "--help"])
        assert result.exit_code == 0
        assert "Import VCF data" in result.output

    def test_has_mode_option(self, runner):
        result = runner.invoke(cli, ["ingest", "--help"])
        assert "--mode" in result.output

    def test_has_filter_options(self, runner):
        result = runner.invoke(cli, ["ingest", "--help"])
        assert "--filter-min-qual" in result.output
        assert "--filter-variant-type" in result.output

    def test_has_neo4j_options(self, runner):
        result = runner.invoke(cli, ["ingest", "--help"])
        assert "--neo4j-home" in result.output
        assert "--neo4j-uri" in result.output
        assert "--database" in result.output


class TestMainHelp:
    def test_main_help_shows_all_commands(self, runner):
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "prepare-csv" in result.output
        assert "load-csv" in result.output
        assert "ingest" in result.output
        assert "status" in result.output
