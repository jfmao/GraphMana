"""Tests for GO term and pathway annotation parsers."""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from graphmana.annotation.parsers.go_pathway import (
    GOParser,
    PathwayParser,
    _parse_obo,
)

FIXTURE_DIR = Path(__file__).parent / "fixtures"


class TestGOParseFile:
    """Test GAF file parsing."""

    def test_parse_fixture(self):
        parser = GOParser(MagicMock())
        records = list(parser._parse_file(FIXTURE_DIR / "go_sample.gaf"))
        assert len(records) == 4

    def test_fields(self):
        parser = GOParser(MagicMock())
        records = list(parser._parse_file(FIXTURE_DIR / "go_sample.gaf"))
        brca1_f = records[0]
        assert brca1_f["gene_symbol"] == "BRCA1"
        assert brca1_f["go_id"] == "GO:0003674"
        assert brca1_f["namespace"] == "molecular_function"

    def test_aspect_mapping(self):
        parser = GOParser(MagicMock())
        records = list(parser._parse_file(FIXTURE_DIR / "go_sample.gaf"))
        # First record: F -> molecular_function
        assert records[0]["namespace"] == "molecular_function"
        # Second record: P -> biological_process
        assert records[1]["namespace"] == "biological_process"

    def test_deduplication(self):
        """Duplicate gene-GO pairs should be skipped."""
        with tempfile.NamedTemporaryFile(suffix=".gaf", mode="w", delete=False) as tmp:
            tmp.write("!gaf-version: 2.2\n")
            # Same gene-GO pair twice
            tmp.write("UniProtKB\tA\tBRCA1\t\tGO:0001\tPMID:1\tIDA\t\tF\t\tgene\ttaxon:9606\t2020\tUniProt\n")
            tmp.write("UniProtKB\tA\tBRCA1\t\tGO:0001\tPMID:2\tIDA\t\tF\t\tgene\ttaxon:9606\t2020\tUniProt\n")
            tmp.write("UniProtKB\tA\tBRCA1\t\tGO:0002\tPMID:3\tIDA\t\tP\t\tgene\ttaxon:9606\t2020\tUniProt\n")
            tmp_path = Path(tmp.name)

        parser = GOParser(MagicMock())
        records = list(parser._parse_file(tmp_path))
        assert len(records) == 2  # Only unique gene-GO pairs
        tmp_path.unlink()

    def test_skip_comments(self):
        parser = GOParser(MagicMock())
        records = list(parser._parse_file(FIXTURE_DIR / "go_sample.gaf"))
        # Comments start with "!", should be skipped
        assert all(r["gene_symbol"] != "" for r in records)


class TestGOLoadBatch:
    """Test _load_batch with mocked connection."""

    def test_load_batch(self):
        mock_conn = MagicMock()
        mock_session = MagicMock()
        mock_session.run.return_value = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)
        mock_conn.driver.session.return_value = mock_session

        parser = GOParser(mock_conn)
        batch = [
            {"gene_symbol": "BRCA1", "go_id": "GO:0001", "go_name": "", "namespace": "molecular_function"},
            {"gene_symbol": "TP53", "go_id": "GO:0002", "go_name": "", "namespace": "biological_process"},
        ]
        result = parser._load_batch(batch)
        assert result == 2  # 2 edges
        assert mock_session.run.call_count == 2  # MERGE terms + CREATE edges


class TestParseObo:
    """Test OBO ontology file parsing."""

    def test_parse_obo(self):
        with tempfile.NamedTemporaryFile(suffix=".obo", mode="w", delete=False) as tmp:
            tmp.write("format-version: 1.2\n\n")
            tmp.write("[Term]\n")
            tmp.write("id: GO:0000001\n")
            tmp.write("name: mitochondrion inheritance\n")
            tmp.write("namespace: biological_process\n\n")
            tmp.write("[Term]\n")
            tmp.write("id: GO:0000002\n")
            tmp.write("name: mitochondrial genome maintenance\n")
            tmp.write("namespace: biological_process\n")
            tmp.write("is_a: GO:0000001 ! mitochondrion inheritance\n\n")
            tmp_path = Path(tmp.name)

        terms, hierarchy = _parse_obo(tmp_path)
        assert len(terms) == 2
        assert terms["GO:0000001"]["name"] == "mitochondrion inheritance"
        assert terms["GO:0000002"]["namespace"] == "biological_process"
        assert len(hierarchy) == 1
        assert hierarchy[0] == {"child_id": "GO:0000002", "parent_id": "GO:0000001"}
        tmp_path.unlink()


class TestPathwayParseFile:
    """Test pathway TSV parsing."""

    def test_parse_fixture(self):
        parser = PathwayParser(MagicMock())
        records = list(parser._parse_file(FIXTURE_DIR / "pathway_sample.tsv"))
        assert len(records) == 4

    def test_fields(self):
        parser = PathwayParser(MagicMock())
        records = list(parser._parse_file(FIXTURE_DIR / "pathway_sample.tsv"))
        assert records[0]["gene_symbol"] == "BRCA1"
        assert records[0]["pathway_id"] == "hsa03440"
        assert records[0]["pathway_name"] == "Homologous recombination"
        assert records[0]["source"] == "KEGG"

    def test_pathway_source_default(self):
        parser = PathwayParser(MagicMock(), pathway_source="Reactome")
        assert parser.source_name == "Pathway_Reactome"


class TestPathwayLoadBatch:
    """Test _load_batch with mocked connection."""

    def test_load_batch(self):
        mock_conn = MagicMock()
        mock_session = MagicMock()
        mock_session.run.return_value = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)
        mock_conn.driver.session.return_value = mock_session

        parser = PathwayParser(mock_conn)
        batch = [
            {"gene_symbol": "BRCA1", "pathway_id": "hsa03440", "pathway_name": "HR", "source": "KEGG"},
            {"gene_symbol": "TP53", "pathway_id": "hsa04115", "pathway_name": "p53", "source": "KEGG"},
        ]
        result = parser._load_batch(batch)
        assert result == 2
