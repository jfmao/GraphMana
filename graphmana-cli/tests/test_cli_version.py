"""Tests for version, list-formats, and config-show CLI commands."""

from click.testing import CliRunner

from graphmana.cli import cli


class TestVersionCommand:
    """Test graphmana version command."""

    def test_version_shows_graphmana_version(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["version"])
        assert result.exit_code == 0
        assert "GraphMana:" in result.output

    def test_version_shows_schema_version(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["version"])
        assert "Schema version:" in result.output

    def test_version_shows_python(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["version"])
        assert "Python:" in result.output

    def test_version_shows_cyvcf2(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["version"])
        assert "cyvcf2:" in result.output

    def test_version_shows_numpy(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["version"])
        assert "NumPy:" in result.output

    def test_version_shows_java_status(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["version"])
        assert "Java:" in result.output

    def test_version_flag_still_works(self):
        """The --version flag on the main group should still work."""
        runner = CliRunner()
        result = runner.invoke(cli, ["--version"])
        assert result.exit_code == 0
        assert "graphmana" in result.output


class TestListFormatsCommand:
    """Test graphmana list-formats command."""

    def test_list_formats_exits_zero(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["list-formats"])
        assert result.exit_code == 0

    def test_list_formats_shows_all_17(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["list-formats"])
        assert "17 formats" in result.output

    def test_list_formats_shows_fast_path_formats(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["list-formats"])
        for fmt in ["treemix", "sfs-dadi", "sfs-fsc", "bed", "tsv", "json"]:
            assert fmt in result.output

    def test_list_formats_shows_full_path_formats(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["list-formats"])
        for fmt in ["vcf", "plink", "plink2", "eigenstrat", "beagle",
                     "structure", "genepop", "hap", "bgen", "gds", "zarr"]:
            assert fmt in result.output

    def test_list_formats_shows_path_labels(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["list-formats"])
        assert "FAST" in result.output
        assert "FULL" in result.output

    def test_list_formats_shows_path_explanations(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["list-formats"])
        assert "constant time" in result.output
        assert "linear time" in result.output


class TestConfigShowCommand:
    """Test graphmana config-show command."""

    def test_config_show_exits_zero(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["config-show"])
        assert result.exit_code == 0

    def test_config_show_displays_neo4j_uri(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["config-show"])
        assert "Neo4j URI:" in result.output
        assert "bolt://" in result.output

    def test_config_show_displays_database(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["config-show"])
        assert "Database:" in result.output

    def test_config_show_displays_batch_size(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["config-show"])
        assert "Batch size:" in result.output

    def test_config_show_displays_threads(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["config-show"])
        assert "Threads:" in result.output

    def test_config_show_displays_env_vars(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["config-show"])
        assert "GRAPHMANA_NEO4J_PASSWORD" in result.output

    def test_config_show_displays_version_info(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["config-show"])
        assert "GraphMana:" in result.output
        assert "Schema:" in result.output
