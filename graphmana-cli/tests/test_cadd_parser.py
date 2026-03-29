"""Tests for CADD annotation parser."""

import gzip
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from graphmana.annotation.parsers.cadd import CADDParser, _is_gzipped

FIXTURE_DIR = Path(__file__).parent / "fixtures"


class TestCADDParseFile:
    """Test CADD TSV parsing."""

    def test_parse_fixture(self):
        parser = CADDParser(MagicMock())
        records = list(parser._parse_file(FIXTURE_DIR / "cadd_sample.tsv"))
        assert len(records) == 6
        assert records[0] == {
            "variantId": "1:10177:A:AC",
            "cadd_raw": 0.234,
            "cadd_phred": 3.456,
        }

    def test_chr_prefix(self):
        parser = CADDParser(MagicMock())
        records = list(
            parser._parse_file(FIXTURE_DIR / "cadd_sample.tsv", chr_prefix="chr")
        )
        assert records[0]["variantId"] == "chr1:10177:A:AC"
        assert records[3]["variantId"] == "chr22:16050075:A:G"

    def test_skip_comments(self):
        parser = CADDParser(MagicMock())
        records = list(parser._parse_file(FIXTURE_DIR / "cadd_sample.tsv"))
        # File has 3 comment lines (## and #header), 6 data lines
        assert len(records) == 6

    def test_gzipped_input(self):
        with tempfile.NamedTemporaryFile(suffix=".tsv.gz", delete=False) as tmp:
            with gzip.open(tmp.name, "wt") as f:
                f.write("## CADD\n")
                f.write("#Chrom\tPos\tRef\tAlt\tRawScore\tPHRED\n")
                f.write("1\t100\tA\tG\t0.5\t10.0\n")
            tmp_path = Path(tmp.name)

        parser = CADDParser(MagicMock())
        records = list(parser._parse_file(tmp_path))
        assert len(records) == 1
        assert records[0]["variantId"] == "1:100:A:G"
        assert records[0]["cadd_phred"] == 10.0
        tmp_path.unlink()

    def test_malformed_rows_skipped(self):
        with tempfile.NamedTemporaryFile(suffix=".tsv", mode="w", delete=False) as tmp:
            tmp.write("#Chrom\tPos\tRef\tAlt\tRawScore\tPHRED\n")
            tmp.write("1\t100\tA\n")  # Too few columns
            tmp.write("1\t200\tA\tG\tnotanumber\t10.0\n")  # Invalid raw score
            tmp.write("1\t300\tA\tG\t0.5\t15.0\n")  # Valid
            tmp_path = Path(tmp.name)

        parser = CADDParser(MagicMock())
        records = list(parser._parse_file(tmp_path))
        assert len(records) == 1
        assert records[0]["variantId"] == "1:300:A:G"
        tmp_path.unlink()


class TestCADDLoadBatch:
    """Test _load_batch with mocked connection."""

    def test_load_batch(self):
        mock_conn = MagicMock()
        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_record = {"matched": 3}
        mock_result.single.return_value = mock_record
        mock_session.run.return_value = mock_result
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)
        mock_conn.driver.session.return_value = mock_session

        parser = CADDParser(mock_conn)
        batch = [
            {"variantId": "1:100:A:G", "cadd_raw": 0.5, "cadd_phred": 10.0},
            {"variantId": "1:200:T:C", "cadd_raw": 1.0, "cadd_phred": 20.0},
            {"variantId": "1:300:G:A", "cadd_raw": 0.1, "cadd_phred": 5.0},
        ]
        matched = parser._load_batch(batch)
        assert matched == 3
        mock_session.run.assert_called_once()


class TestIsGzipped:
    """Test gzip detection."""

    def test_plain_file(self):
        assert not _is_gzipped(FIXTURE_DIR / "cadd_sample.tsv")

    def test_gzipped_file(self):
        with tempfile.NamedTemporaryFile(suffix=".gz", delete=False) as tmp:
            with gzip.open(tmp.name, "wb") as f:
                f.write(b"test data")
            assert _is_gzipped(Path(tmp.name))
            Path(tmp.name).unlink()
