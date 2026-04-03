"""Tests for TSV export (pure function tests + integration with mocked Neo4j)."""

from unittest.mock import MagicMock

from graphmana.export.tsv_export import AVAILABLE_COLUMNS, DEFAULT_COLUMNS, TSVExporter


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

MOCK_VARIANTS = [
    {"variantId": "chr1:100:A:T", "chr": "chr1", "pos": 100, "ref": "A",
     "alt": "T", "variant_type": "SNP", "af_total": 0.25,
     "ac_total": 10, "an_total": 40, "call_rate": 0.99},
    {"variantId": "chr1:200:G:C", "chr": "chr1", "pos": 200, "ref": "G",
     "alt": "C", "variant_type": "SNP", "af_total": 0.5,
     "ac_total": 20, "an_total": 40, "call_rate": 1.0},
]


def _make_tsv_exporter(variants=None, n_samples=20):
    """Create a TSVExporter with mocked Neo4j methods."""
    if variants is None:
        variants = MOCK_VARIANTS
    conn = MagicMock()
    exporter = TSVExporter(conn)
    exporter._get_target_chromosomes = MagicMock(return_value=["chr1"])
    exporter._iter_variants_fast = MagicMock(
        side_effect=lambda chrom, **kw: iter(variants)
    )
    exporter._get_sample_count = MagicMock(return_value=n_samples)
    return exporter


# ---------------------------------------------------------------------------
# Column configuration tests (existing)
# ---------------------------------------------------------------------------


class TestDefaultColumns:
    """Verify TSV default column configuration."""

    def test_default_columns(self):
        assert "variantId" in DEFAULT_COLUMNS
        assert "chr" in DEFAULT_COLUMNS
        assert "pos" in DEFAULT_COLUMNS
        assert "ref" in DEFAULT_COLUMNS
        assert "alt" in DEFAULT_COLUMNS
        assert "variant_type" in DEFAULT_COLUMNS
        assert "af_total" in DEFAULT_COLUMNS

    def test_default_count(self):
        assert len(DEFAULT_COLUMNS) == 7

    def test_defaults_subset_of_available(self):
        for col in DEFAULT_COLUMNS:
            assert col in AVAILABLE_COLUMNS


class TestAvailableColumns:
    """Verify all available columns are listed."""

    def test_includes_core_fields(self):
        for col in ["variantId", "chr", "pos", "ref", "alt"]:
            assert col in AVAILABLE_COLUMNS

    def test_includes_stats_fields(self):
        for col in ["ac_total", "an_total", "af_total", "call_rate"]:
            assert col in AVAILABLE_COLUMNS

    def test_includes_annotation_fields(self):
        for col in ["ancestral_allele", "is_polarized"]:
            assert col in AVAILABLE_COLUMNS

    def test_includes_quality_fields(self):
        for col in ["qual", "filter"]:
            assert col in AVAILABLE_COLUMNS


# ---------------------------------------------------------------------------
# Integration tests — export() with mocked Neo4j
# ---------------------------------------------------------------------------


class TestTSVExportIntegration:
    """Test TSVExporter.export() end-to-end with mocked data."""

    def test_export_produces_valid_tsv(self, tmp_path):
        """export() writes TSV with header and returns valid summary."""
        exporter = _make_tsv_exporter()
        out = tmp_path / "test.tsv"
        summary = exporter.export(out)

        # Validate return dict contract
        assert summary["n_variants"] == 2
        assert summary["n_samples"] == 20
        assert summary["format"] == "tsv"
        assert summary["chromosomes"] == ["chr1"]
        assert summary["columns"] == list(DEFAULT_COLUMNS)

        # Validate file content
        lines = out.read_text().strip().split("\n")
        assert len(lines) == 3  # header + 2 data rows

        # Header line
        header_fields = lines[0].split("\t")
        assert header_fields == list(DEFAULT_COLUMNS)

        # First data line
        fields = lines[1].split("\t")
        assert fields[0] == "chr1:100:A:T"
        assert fields[2] == "100"
        assert fields[5] == "SNP"
        assert fields[6] == "0.25"

    def test_export_custom_columns(self, tmp_path):
        """Custom column list controls output."""
        exporter = _make_tsv_exporter()
        out = tmp_path / "test.tsv"
        cols = ["variantId", "pos", "ac_total"]
        summary = exporter.export(out, columns=cols)

        assert summary["columns"] == cols
        lines = out.read_text().strip().split("\n")
        header = lines[0].split("\t")
        assert header == cols
        fields = lines[1].split("\t")
        assert len(fields) == 3
        assert fields[2] == "10"

    def test_export_missing_column_shows_dot(self, tmp_path):
        """Column not present in variant props renders as '.'."""
        exporter = _make_tsv_exporter()
        out = tmp_path / "test.tsv"
        summary = exporter.export(out, columns=["variantId", "ancestral_allele"])

        lines = out.read_text().strip().split("\n")
        fields = lines[1].split("\t")
        assert fields[1] == "."  # ancestral_allele not in mock data

    def test_export_float_formatting(self, tmp_path):
        """Float values use concise g-format."""
        variants = [
            {"variantId": "v1", "chr": "chr1", "pos": 1, "ref": "A", "alt": "T",
             "variant_type": "SNP", "af_total": 0.000001234},
        ]
        exporter = _make_tsv_exporter(variants=variants)
        out = tmp_path / "test.tsv"
        exporter.export(out)

        lines = out.read_text().strip().split("\n")
        fields = lines[1].split("\t")
        af_val = fields[6]
        assert "e" in af_val or af_val == "1.234e-06"  # scientific notation for tiny values

    def test_export_empty_variants(self, tmp_path):
        """Export with no variants produces header-only file."""
        exporter = _make_tsv_exporter(variants=[])
        out = tmp_path / "test.tsv"
        summary = exporter.export(out)

        assert summary["n_variants"] == 0
        assert summary["n_samples"] == 20
        assert summary["format"] == "tsv"
        lines = out.read_text().strip().split("\n")
        assert len(lines) == 1  # header only

    def test_export_uses_fast_path(self, tmp_path):
        """TSV must use _iter_variants_fast (FAST PATH)."""
        exporter = _make_tsv_exporter()
        exporter.export(tmp_path / "test.tsv")

        exporter._iter_variants_fast.assert_called()
        exporter._get_sample_count.assert_called_once()
