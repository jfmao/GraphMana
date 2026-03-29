"""Tests for CSV loader — validate, find binary, build command."""

import pytest

from graphmana.ingest.loader import (
    REQUIRED_CSV_FILES,
    _build_import_command,
    find_neo4j_admin,
    validate_csv_dir,
)

# ---------------------------------------------------------------------------
# validate_csv_dir
# ---------------------------------------------------------------------------


class TestValidateCsvDir:
    def test_all_present(self, tmp_path):
        for name in REQUIRED_CSV_FILES:
            (tmp_path / name).write_text("header\n")
        missing = validate_csv_dir(tmp_path)
        assert missing == []

    def test_missing_files_listed(self, tmp_path):
        # Only create 3 of 7
        for name in REQUIRED_CSV_FILES[:3]:
            (tmp_path / name).write_text("header\n")
        missing = validate_csv_dir(tmp_path)
        assert len(missing) == 4
        for name in REQUIRED_CSV_FILES[3:]:
            assert name in missing

    def test_empty_directory(self, tmp_path):
        missing = validate_csv_dir(tmp_path)
        assert len(missing) == 7


# ---------------------------------------------------------------------------
# find_neo4j_admin
# ---------------------------------------------------------------------------


class TestFindNeo4jAdmin:
    def test_found_at_expected_path(self, tmp_path):
        bin_dir = tmp_path / "bin"
        bin_dir.mkdir()
        admin = bin_dir / "neo4j-admin"
        admin.write_text("#!/bin/bash\n")

        result = find_neo4j_admin(tmp_path)
        assert result == admin

    def test_not_found_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="neo4j-admin"):
            find_neo4j_admin(tmp_path)


# ---------------------------------------------------------------------------
# _build_import_command
# ---------------------------------------------------------------------------


class TestBuildImportCommand:
    def test_correct_flags(self, tmp_path):
        neo4j_admin = tmp_path / "bin" / "neo4j-admin"
        neo4j_admin.parent.mkdir()
        neo4j_admin.touch()

        csv_dir = tmp_path / "csv"
        csv_dir.mkdir()

        cmd = _build_import_command(neo4j_admin, csv_dir, database="testdb")
        assert str(neo4j_admin) in cmd
        assert "database" in cmd
        assert "import" in cmd
        assert "full" in cmd
        assert "testdb" in cmd
        assert "--array-delimiter=;" in cmd
        assert "--id-type=string" in cmd

    def test_overwrite_flag(self, tmp_path):
        neo4j_admin = tmp_path / "neo4j-admin"
        neo4j_admin.touch()
        csv_dir = tmp_path / "csv"
        csv_dir.mkdir()

        cmd = _build_import_command(neo4j_admin, csv_dir, overwrite=True)
        assert "--overwrite-destination=true" in cmd

        cmd = _build_import_command(neo4j_admin, csv_dir, overwrite=False)
        assert "--overwrite-destination=true" not in cmd

    def test_optional_gene_csvs_included(self, tmp_path):
        neo4j_admin = tmp_path / "neo4j-admin"
        neo4j_admin.touch()
        csv_dir = tmp_path / "csv"
        csv_dir.mkdir()

        # Create optional gene files
        (csv_dir / "gene_nodes.csv").write_text("header\n")
        (csv_dir / "has_consequence_edges.csv").write_text("header\n")

        cmd = _build_import_command(neo4j_admin, csv_dir)
        cmd_str = " ".join(cmd)
        assert "gene_nodes.csv" in cmd_str
        assert "has_consequence_edges.csv" in cmd_str

    def test_optional_gene_csvs_absent(self, tmp_path):
        neo4j_admin = tmp_path / "neo4j-admin"
        neo4j_admin.touch()
        csv_dir = tmp_path / "csv"
        csv_dir.mkdir()

        cmd = _build_import_command(neo4j_admin, csv_dir)
        cmd_str = " ".join(cmd)
        assert "gene_nodes.csv" not in cmd_str
        assert "has_consequence_edges.csv" not in cmd_str

    def test_node_and_relationship_flags(self, tmp_path):
        neo4j_admin = tmp_path / "neo4j-admin"
        neo4j_admin.touch()
        csv_dir = tmp_path / "csv"
        csv_dir.mkdir()

        cmd = _build_import_command(neo4j_admin, csv_dir)
        cmd_str = " ".join(cmd)
        assert "--nodes=Variant=" in cmd_str
        assert "--nodes=Sample=" in cmd_str
        assert "--nodes=Population=" in cmd_str
        assert "--nodes=Chromosome=" in cmd_str
        assert "--relationships=NEXT=" in cmd_str
        assert "--relationships=ON_CHROMOSOME=" in cmd_str
        assert "--relationships=IN_POPULATION=" in cmd_str
