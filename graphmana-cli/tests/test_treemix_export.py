"""Tests for TreeMix allele count matrix exporter."""

import gzip
from unittest.mock import MagicMock

from graphmana.export.treemix_export import TreeMixExporter
from graphmana.filtering.export_filters import ExportFilterConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

MOCK_VARIANTS = [
    {"variantId": "v1", "pop_ids": ["AFR", "EUR", "EAS"],
     "ac": [10, 5, 8], "an": [200, 100, 150], "an_total": 450},
    {"variantId": "v2", "pop_ids": ["AFR", "EUR", "EAS"],
     "ac": [20, 15, 12], "an": [200, 100, 150], "an_total": 450},
    {"variantId": "v3", "pop_ids": ["AFR", "EUR", "EAS"],
     "ac": [0, 0, 0], "an": [0, 0, 0], "an_total": 0},  # should be skipped
]


def _make_treemix_exporter(variants=None, n_samples=225, filter_config=None):
    """Create a TreeMixExporter with mocked Neo4j methods."""
    if variants is None:
        variants = MOCK_VARIANTS
    conn = MagicMock()
    fc = filter_config or ExportFilterConfig()
    exporter = TreeMixExporter(conn, filter_config=fc)
    exporter._get_target_chromosomes = MagicMock(return_value=["chr1"])
    exporter._iter_variants_fast = MagicMock(
        side_effect=lambda chrom, **kw: iter(variants)
    )
    exporter._get_sample_count = MagicMock(return_value=n_samples)
    return exporter


# ---------------------------------------------------------------------------
# Formatting tests (existing)
# ---------------------------------------------------------------------------


class TestTreeMixFormatting:
    """Test TreeMix output formatting."""

    def test_header_format(self):
        """Header should be space-separated population names."""
        pop_names = ["AFR", "EUR", "EAS"]
        header = " ".join(pop_names)
        assert header == "AFR EUR EAS"

    def test_ac_an_pair_format(self):
        """Each cell is ac,an with no spaces around comma."""
        ac, an = 10, 200
        pair = f"{ac},{an}"
        assert pair == "10,200"

    def test_row_format(self):
        """Row should be space-separated ac,an pairs."""
        ac = [10, 5, 8]
        an = [200, 100, 150]
        pairs = [f"{ac[i]},{an[i]}" for i in range(len(ac))]
        row = " ".join(pairs)
        assert row == "10,200 5,100 8,150"

    def test_population_subsetting(self):
        """Population filter should select correct indices."""
        pop_ids = ["AFR", "EAS", "EUR"]
        pop_filter = {"AFR", "EUR"}
        pop_indices = [i for i, p in enumerate(pop_ids) if p in pop_filter]
        pop_names = [pop_ids[i] for i in pop_indices]
        assert pop_indices == [0, 2]
        assert pop_names == ["AFR", "EUR"]

    def test_exporter_inherits_base(self):
        """TreeMixExporter should inherit from BaseExporter."""
        from graphmana.export.base import BaseExporter

        assert issubclass(TreeMixExporter, BaseExporter)


# ---------------------------------------------------------------------------
# Integration tests — export() with mocked Neo4j
# ---------------------------------------------------------------------------


class TestTreeMixExportIntegration:
    """Test TreeMixExporter.export() end-to-end with mocked data."""

    def test_export_produces_valid_treemix(self, tmp_path):
        """export() writes gzipped TreeMix format and returns valid summary."""
        exporter = _make_treemix_exporter()
        out = tmp_path / "test.treemix.gz"
        summary = exporter.export(out)

        # Validate return dict contract
        assert summary["n_variants"] == 2  # v3 skipped (an_total=0)
        assert summary["n_samples"] == 225
        assert summary["format"] == "treemix"
        assert summary["chromosomes"] == ["chr1"]
        assert summary["populations"] == ["AFR", "EUR", "EAS"]

        # Validate file content
        with gzip.open(out, "rt") as f:
            lines = f.read().strip().split("\n")
        assert len(lines) == 3  # header + 2 data rows
        assert lines[0] == "AFR EUR EAS"
        assert lines[1] == "10,200 5,100 8,150"
        assert lines[2] == "20,200 15,100 12,150"

    def test_export_auto_appends_gz(self, tmp_path):
        """Output path without .gz gets .gz appended."""
        exporter = _make_treemix_exporter()
        out = tmp_path / "test.treemix"
        exporter.export(out)

        gz_path = tmp_path / "test.treemix.gz"
        assert gz_path.exists()

    def test_export_with_population_filter(self, tmp_path):
        """Population filter subsets columns."""
        fc = ExportFilterConfig(populations=["AFR", "EAS"])
        exporter = _make_treemix_exporter(filter_config=fc)
        out = tmp_path / "test.treemix.gz"
        summary = exporter.export(out)

        assert summary["populations"] == ["AFR", "EAS"]
        with gzip.open(out, "rt") as f:
            lines = f.read().strip().split("\n")
        assert lines[0] == "AFR EAS"
        # Verify only 2 columns per data row
        pairs = lines[1].split(" ")
        assert len(pairs) == 2

    def test_export_empty_variants(self, tmp_path):
        """Export with no variants returns zero counts."""
        exporter = _make_treemix_exporter(variants=[])
        out = tmp_path / "test.treemix.gz"
        summary = exporter.export(out)

        assert summary["n_variants"] == 0
        assert summary["format"] == "treemix"

    def test_export_skips_zero_an_total(self, tmp_path):
        """Variants with an_total=0 are excluded."""
        zero_only = [
            {"variantId": "v1", "pop_ids": ["AFR"], "ac": [0], "an": [0], "an_total": 0},
        ]
        exporter = _make_treemix_exporter(variants=zero_only)
        out = tmp_path / "test.treemix.gz"
        summary = exporter.export(out)

        assert summary["n_variants"] == 0

    def test_export_uses_fast_path(self, tmp_path):
        """TreeMix must use _iter_variants_fast (FAST PATH)."""
        exporter = _make_treemix_exporter()
        exporter.export(tmp_path / "test.treemix.gz")

        exporter._iter_variants_fast.assert_called()
        exporter._get_sample_count.assert_called_once()
