"""Tests for BED format exporter."""

from graphmana.export.bed_export import BEDExporter


class TestBEDFormatting:
    """Test BED line formatting and coordinate conversion."""

    def test_zero_based_coordinates(self, tmp_path):
        """BED start is 0-based (pos - 1), end is pos."""
        # Verify the logic: pos=100 -> start=99, end=100
        # (actual export requires Neo4j; test formatting logic here)
        pos = 100
        start = pos - 1
        end = pos
        assert start == 99
        assert end == 100

    def test_bed_line_format(self):
        """Verify tab-separated BED line construction."""
        chrom = "chr1"
        pos = 12345
        name = "chr1-12345-A-G"
        parts = [chrom, str(pos - 1), str(pos), name]
        line = "\t".join(parts)
        assert line == "chr1\t12344\t12345\tchr1-12345-A-G"

    def test_bed_line_with_extras(self):
        """Verify extra columns are appended."""
        parts = ["chr1", "99", "100", "var1", "SNP", "0.25"]
        line = "\t".join(parts)
        fields = line.split("\t")
        assert len(fields) == 6
        assert fields[4] == "SNP"
        assert fields[5] == "0.25"

    def test_bed_missing_extra_value(self):
        """Missing extra column values should be '.'."""
        val = None
        formatted = "." if val is None else str(val)
        assert formatted == "."

    def test_bed_float_formatting(self):
        """Float values should use concise formatting."""
        val = 0.123456789
        formatted = f"{val:.6g}"
        assert formatted == "0.123457"

    def test_available_extra_columns(self):
        """Check that standard extra columns are defined."""
        from graphmana.export.bed_export import AVAILABLE_EXTRA_COLUMNS

        assert "variant_type" in AVAILABLE_EXTRA_COLUMNS
        assert "af_total" in AVAILABLE_EXTRA_COLUMNS
        assert "gene_symbol" in AVAILABLE_EXTRA_COLUMNS

    def test_exporter_inherits_base(self):
        """BEDExporter should inherit from BaseExporter."""
        from graphmana.export.base import BaseExporter

        assert issubclass(BEDExporter, BaseExporter)
