"""Tests for the graphmana export CLI command."""

from click.testing import CliRunner

from graphmana.cli import cli


class TestExportHelp:
    """Test that export command is registered and shows help."""

    def test_export_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["export", "--help"])
        assert result.exit_code == 0
        assert "Export data from Neo4j" in result.output

    def test_export_formats_listed(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["export", "--help"])
        assert "vcf" in result.output
        assert "plink" in result.output
        assert "tsv" in result.output
        assert "eigenstrat" in result.output
        assert "treemix" in result.output
        assert "sfs-dadi" in result.output
        assert "sfs-fsc" in result.output
        assert "bed" in result.output
        assert "beagle" in result.output
        assert "structure" in result.output
        assert "genepop" in result.output
        assert "hap" in result.output


class TestExportRequiredOptions:
    """Test that required options are enforced."""

    def test_missing_output(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["export", "--format", "vcf"])
        assert result.exit_code != 0
        assert "output" in result.output.lower() or "required" in result.output.lower()

    def test_missing_format(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["export", "--output", "test.vcf"])
        assert result.exit_code != 0
        assert "format" in result.output.lower() or "required" in result.output.lower()


class TestExportFilterOptions:
    """Test that filter options are registered."""

    def test_population_option(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["export", "--help"])
        assert "--populations" in result.output

    def test_chromosome_option(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["export", "--help"])
        assert "--chromosomes" in result.output

    def test_region_option(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["export", "--help"])
        assert "--region" in result.output

    def test_maf_options(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["export", "--help"])
        assert "--filter-maf-min" in result.output
        assert "--filter-maf-max" in result.output

    def test_call_rate_option(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["export", "--help"])
        assert "--filter-min-call-rate" in result.output

    def test_variant_type_option(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["export", "--help"])
        assert "--filter-variant-type" in result.output


class TestExportFormatSpecificOptions:
    """Test format-specific options are registered."""

    def test_phased_option(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["export", "--help"])
        assert "--phased" in result.output

    def test_tsv_columns_option(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["export", "--help"])
        assert "--tsv-columns" in result.output

    def test_threads_option(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["export", "--help"])
        assert "--threads" in result.output

    def test_sfs_populations_option(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["export", "--help"])
        assert "--sfs-populations" in result.output

    def test_sfs_projection_option(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["export", "--help"])
        assert "--sfs-projection" in result.output

    def test_sfs_polarized_option(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["export", "--help"])
        assert "--sfs-polarized" in result.output

    def test_bed_extra_columns_option(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["export", "--help"])
        assert "--bed-extra-columns" in result.output

    def test_filter_sample_list_option(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["export", "--help"])
        assert "--filter-sample-list" in result.output

    def test_structure_format_option(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["export", "--help"])
        assert "--structure-format" in result.output

    def test_has_auto_start_option(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["export", "--help"])
        assert "--auto-start-neo4j" in result.output

    def test_has_neo4j_home_option(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["export", "--help"])
        assert "--neo4j-home" in result.output
