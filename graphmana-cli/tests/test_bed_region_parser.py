"""Tests for BED region annotation parser."""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from graphmana.annotation.parsers.bed_region import BEDRegionParser

FIXTURE_DIR = Path(__file__).parent / "fixtures"


class TestBEDParseFile:
    """Test BED file parsing."""

    def test_parse_fixture(self):
        parser = BEDRegionParser(MagicMock())
        records = list(parser._parse_file(FIXTURE_DIR / "regions_sample.bed"))
        assert len(records) == 3

    def test_coordinate_conversion(self):
        """BED 0-based half-open → 1-based closed."""
        parser = BEDRegionParser(MagicMock())
        records = list(parser._parse_file(FIXTURE_DIR / "regions_sample.bed"))
        # chr1  999  2000 → start=1000, end=2000
        assert records[0]["chr"] == "chr1"
        assert records[0]["start"] == 1000
        assert records[0]["end"] == 2000

    def test_name_used_as_id(self):
        parser = BEDRegionParser(MagicMock())
        records = list(parser._parse_file(FIXTURE_DIR / "regions_sample.bed"))
        assert records[0]["id"] == "enhancer_1"
        assert records[1]["id"] == "promoter_1"

    def test_region_type(self):
        parser = BEDRegionParser(MagicMock(), region_type="enhancer")
        records = list(parser._parse_file(FIXTURE_DIR / "regions_sample.bed"))
        assert all(r["type"] == "enhancer" for r in records)

    def test_source_is_filename(self):
        parser = BEDRegionParser(MagicMock())
        records = list(parser._parse_file(FIXTURE_DIR / "regions_sample.bed"))
        assert all(r["source"] == "regions_sample.bed" for r in records)

    def test_skip_comments_and_track(self):
        with tempfile.NamedTemporaryFile(suffix=".bed", mode="w", delete=False) as tmp:
            tmp.write("# comment line\n")
            tmp.write("track name=test\n")
            tmp.write("chr1\t100\t200\tregion1\n")
            tmp.write("\n")  # empty line
            tmp.write("chr1\t300\t400\tregion2\n")
            tmp_path = Path(tmp.name)

        parser = BEDRegionParser(MagicMock())
        records = list(parser._parse_file(tmp_path))
        assert len(records) == 2
        tmp_path.unlink()

    def test_synthetic_id_when_no_name(self):
        with tempfile.NamedTemporaryFile(suffix=".bed", mode="w", delete=False) as tmp:
            tmp.write("chr1\t100\t200\n")  # Only 3 columns, no name
            tmp_path = Path(tmp.name)

        parser = BEDRegionParser(MagicMock())
        records = list(parser._parse_file(tmp_path))
        assert len(records) == 1
        # Synthetic ID format: chr:start-end_counter
        assert records[0]["id"].startswith("chr1:")
        tmp_path.unlink()

    def test_malformed_coordinates_skipped(self):
        with tempfile.NamedTemporaryFile(suffix=".bed", mode="w", delete=False) as tmp:
            tmp.write("chr1\tabc\t200\tregion1\n")  # bad start
            tmp.write("chr1\t100\t200\tregion2\n")  # valid
            tmp_path = Path(tmp.name)

        parser = BEDRegionParser(MagicMock())
        records = list(parser._parse_file(tmp_path))
        assert len(records) == 1
        assert records[0]["id"] == "region2"
        tmp_path.unlink()


class TestBEDLoadBatch:
    """Test _load_batch with mocked connection."""

    def test_load_creates_elements_and_edges(self):
        mock_conn = MagicMock()
        mock_session = MagicMock()
        # MERGE elements — no return value needed
        # FIND_VARIANTS returns some variant IDs
        mock_result_find = MagicMock()
        mock_result_find.__iter__ = MagicMock(
            return_value=iter([{"variantId": "chr1:1050:A:G"}])
        )
        # CREATE edges — no specific return
        mock_result_merge = MagicMock()
        mock_session.run.side_effect = [
            mock_result_merge,  # MERGE_REGULATORY_ELEMENT_BATCH
            mock_result_find,   # FIND_VARIANTS_IN_INTERVAL
            mock_result_merge,  # CREATE_IN_REGION_BATCH
        ]
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)
        mock_conn.driver.session.return_value = mock_session

        parser = BEDRegionParser(mock_conn)
        batch = [
            {"id": "enhancer_1", "type": "enhancer", "chr": "chr1", "start": 1000, "end": 2000, "source": "test.bed"},
        ]
        result = parser._load_batch(batch)
        assert result == 1  # One variant edge created
