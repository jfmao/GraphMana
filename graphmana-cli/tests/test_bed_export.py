"""Tests for BED format exporter."""

from unittest.mock import MagicMock, patch

from graphmana.export.bed_export import BEDExporter


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

MOCK_VARIANTS_CHR1 = [
    {"variantId": "chr1:100:A:T", "chr": "chr1", "pos": 100, "ref": "A", "alt": "T",
     "variant_type": "SNP", "af_total": 0.25, "ac_total": 10, "an_total": 40},
    {"variantId": "chr1:200:G:C", "chr": "chr1", "pos": 200, "ref": "G", "alt": "C",
     "variant_type": "SNP", "af_total": 0.5, "ac_total": 20, "an_total": 40},
]

MOCK_VARIANTS_CHR2 = [
    {"variantId": "chr2:50:C:G", "chr": "chr2", "pos": 50, "ref": "C", "alt": "G",
     "variant_type": "SNP", "af_total": 0.1, "ac_total": 4, "an_total": 40},
]


def _make_bed_exporter(variants_by_chr=None, n_samples=20):
    """Create a BEDExporter with mocked Neo4j methods."""
    if variants_by_chr is None:
        variants_by_chr = {"chr1": MOCK_VARIANTS_CHR1, "chr2": MOCK_VARIANTS_CHR2}

    conn = MagicMock()
    exporter = BEDExporter(conn)
    exporter._get_target_chromosomes = MagicMock(
        return_value=list(variants_by_chr.keys())
    )
    exporter._iter_variants_fast = MagicMock(
        side_effect=lambda chrom, **kw: iter(variants_by_chr.get(chrom, []))
    )
    exporter._get_sample_count = MagicMock(return_value=n_samples)
    return exporter


# ---------------------------------------------------------------------------
# Formatting tests (existing)
# ---------------------------------------------------------------------------


class TestBEDFormatting:
    """Test BED line formatting and coordinate conversion."""

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


# ---------------------------------------------------------------------------
# Integration tests — export() with mocked Neo4j
# ---------------------------------------------------------------------------


class TestBEDExportIntegration:
    """Test BEDExporter.export() end-to-end with mocked data."""

    def test_export_produces_valid_bed(self, tmp_path):
        """export() writes correct BED format and returns valid summary."""
        exporter = _make_bed_exporter()
        out = tmp_path / "test.bed"
        summary = exporter.export(out)

        # Validate return dict contract
        assert summary["n_variants"] == 3
        assert summary["n_samples"] == 20
        assert summary["format"] == "bed"
        assert summary["chromosomes"] == ["chr1", "chr2"]

        # Validate file content
        lines = out.read_text().strip().split("\n")
        assert len(lines) == 3

        # First line: chr1:100 → BED [99, 100)
        fields = lines[0].split("\t")
        assert fields[0] == "chr1"
        assert fields[1] == "99"   # 0-based start
        assert fields[2] == "100"  # end
        assert fields[3] == "chr1:100:A:T"

    def test_export_with_extra_columns(self, tmp_path):
        """Extra columns are appended to BED output."""
        exporter = _make_bed_exporter()
        out = tmp_path / "test.bed"
        summary = exporter.export(out, extra_columns=["variant_type", "af_total"])

        lines = out.read_text().strip().split("\n")
        fields = lines[0].split("\t")
        assert len(fields) == 6  # chr, start, end, name, variant_type, af_total
        assert fields[4] == "SNP"
        assert fields[5] == "0.25"

    def test_export_missing_extra_column_shows_dot(self, tmp_path):
        """Missing extra column values render as '.'."""
        exporter = _make_bed_exporter()
        out = tmp_path / "test.bed"
        summary = exporter.export(out, extra_columns=["gene_symbol"])

        lines = out.read_text().strip().split("\n")
        fields = lines[0].split("\t")
        assert fields[4] == "."  # gene_symbol not in mock data

    def test_export_empty_variants(self, tmp_path):
        """Export with no variants produces empty file."""
        exporter = _make_bed_exporter(variants_by_chr={"chr1": []}, n_samples=10)
        out = tmp_path / "test.bed"
        summary = exporter.export(out)

        assert summary["n_variants"] == 0
        assert summary["n_samples"] == 10
        assert summary["format"] == "bed"
        assert out.read_text() == ""

    def test_export_uses_fast_path(self, tmp_path):
        """BED exporter must use _iter_variants_fast (FAST PATH)."""
        exporter = _make_bed_exporter()
        out = tmp_path / "test.bed"
        exporter.export(out)

        exporter._iter_variants_fast.assert_called()
        exporter._get_sample_count.assert_called_once()
