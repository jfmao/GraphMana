"""Tests for gene constraint annotation parser."""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from graphmana.annotation.parsers.constraint import (
    GeneConstraintParser,
    _safe_float,
)

FIXTURE_DIR = Path(__file__).parent / "fixtures"


class TestConstraintParseFile:
    """Test gnomAD constraint TSV parsing."""

    def test_parse_fixture(self):
        parser = GeneConstraintParser(MagicMock())
        records = list(parser._parse_file(FIXTURE_DIR / "gnomad_constraint_sample.tsv"))
        assert len(records) == 4

    def test_first_record(self):
        parser = GeneConstraintParser(MagicMock())
        records = list(parser._parse_file(FIXTURE_DIR / "gnomad_constraint_sample.tsv"))
        brca1 = records[0]
        assert brca1["symbol"] == "BRCA1"
        assert brca1["pli"] == 0.99
        assert brca1["loeuf"] == 0.35
        assert brca1["mis_z"] == 3.45
        assert brca1["syn_z"] == 1.23

    def test_na_handling(self):
        parser = GeneConstraintParser(MagicMock())
        records = list(parser._parse_file(FIXTURE_DIR / "gnomad_constraint_sample.tsv"))
        unknown = records[2]  # UNKNOWN_GENE with all NA values
        assert unknown["symbol"] == "UNKNOWN_GENE"
        assert unknown["pli"] is None
        assert unknown["loeuf"] is None
        assert unknown["mis_z"] is None
        assert unknown["syn_z"] is None

    def test_empty_gene_skipped(self):
        with tempfile.NamedTemporaryFile(suffix=".tsv", mode="w", delete=False) as tmp:
            tmp.write("gene\ttranscript\tpLI\toe_lof_upper\tmis_z\tsyn_z\n")
            tmp.write("\tENST0001\t0.5\t0.3\t1.0\t1.0\n")  # empty gene
            tmp.write("BRCA2\tENST0002\t0.8\t0.4\t2.0\t1.5\n")
            tmp_path = Path(tmp.name)

        parser = GeneConstraintParser(MagicMock())
        records = list(parser._parse_file(tmp_path))
        assert len(records) == 1
        assert records[0]["symbol"] == "BRCA2"
        tmp_path.unlink()


class TestConstraintLoadBatch:
    """Test _load_batch with mocked connection."""

    def test_load_batch(self):
        mock_conn = MagicMock()
        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_record = {"matched": 2}
        mock_result.single.return_value = mock_record
        mock_session.run.return_value = mock_result
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)
        mock_conn.driver.session.return_value = mock_session

        parser = GeneConstraintParser(mock_conn)
        batch = [
            {"symbol": "BRCA1", "pli": 0.99, "loeuf": 0.35, "mis_z": 3.45, "syn_z": 1.23},
            {"symbol": "TP53", "pli": 0.95, "loeuf": 0.28, "mis_z": 2.89, "syn_z": 0.98},
        ]
        matched = parser._load_batch(batch)
        assert matched == 2


class TestSafeFloat:
    """Test _safe_float helper."""

    def test_valid_float(self):
        assert _safe_float("3.14") == 3.14

    def test_integer_string(self):
        assert _safe_float("42") == 42.0

    def test_empty_string(self):
        assert _safe_float("") is None

    def test_none(self):
        assert _safe_float(None) is None

    def test_na(self):
        assert _safe_float("NA") is None

    def test_nan(self):
        assert _safe_float("nan") is None

    def test_dot(self):
        assert _safe_float(".") is None

    def test_invalid(self):
        assert _safe_float("abc") is None

    def test_negative(self):
        assert _safe_float("-1.5") == -1.5
