"""Tests for dadi SFS format exporter."""

from unittest.mock import MagicMock

import numpy as np
import pytest

from graphmana.export.sfs_dadi_export import SFSDadiExporter, _resolve_pop_indices


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

MOCK_VARIANTS_POLARIZED = [
    {"variantId": "v1", "pop_ids": ["AFR", "EUR"], "ac": [5, 3], "an": [20, 20],
     "ac_total": 8, "an_total": 40, "is_polarized": True},
    {"variantId": "v2", "pop_ids": ["AFR", "EUR"], "ac": [10, 7], "an": [20, 20],
     "ac_total": 17, "an_total": 40, "is_polarized": True},
    # Monomorphic — should be skipped by default
    {"variantId": "v3", "pop_ids": ["AFR", "EUR"], "ac": [0, 0], "an": [20, 20],
     "ac_total": 0, "an_total": 40, "is_polarized": True},
]


def _make_dadi_exporter(variants=None, n_samples=20):
    """Create SFSDadiExporter with mocked Neo4j methods."""
    if variants is None:
        variants = MOCK_VARIANTS_POLARIZED
    conn = MagicMock()
    exporter = SFSDadiExporter(conn)
    exporter._get_target_chromosomes = MagicMock(return_value=["chr1"])
    exporter._iter_variants_fast = MagicMock(
        side_effect=lambda chrom, **kw: iter(variants)
    )
    exporter._get_sample_count = MagicMock(return_value=n_samples)
    return exporter


# ---------------------------------------------------------------------------
# Pop index resolution tests (existing)
# ---------------------------------------------------------------------------


class TestPopIndexResolution:
    """Test population index resolution."""

    def test_basic_resolution(self):
        """Find correct indices for target populations."""
        pop_ids = ["AFR", "EAS", "EUR"]
        indices = _resolve_pop_indices(pop_ids, ["AFR", "EUR"])
        assert indices == [0, 2]

    def test_single_pop(self):
        """Single population resolution."""
        pop_ids = ["AFR", "EAS", "EUR"]
        indices = _resolve_pop_indices(pop_ids, ["EAS"])
        assert indices == [1]

    def test_missing_pop_raises(self):
        """Missing population should raise ValueError."""
        pop_ids = ["AFR", "EAS", "EUR"]
        with pytest.raises(ValueError, match="SAS"):
            _resolve_pop_indices(pop_ids, ["SAS"])

    def test_order_preserved(self):
        """Target population order should be preserved."""
        pop_ids = ["AFR", "EAS", "EUR"]
        indices = _resolve_pop_indices(pop_ids, ["EUR", "AFR"])
        assert indices == [2, 0]


class TestDadiFormatOutput:
    """Test dadi .fs format structure."""

    def test_dimensions_1d(self):
        """1D SFS dimensions: proj+1."""
        proj = 20
        dims = [str(proj + 1)]
        assert dims == ["21"]

    def test_dimensions_2d(self):
        """2D SFS dimensions: proj1+1 proj2+1."""
        proj = [20, 30]
        dims = [str(p + 1) for p in proj]
        assert dims == ["21", "31"]

    def test_exporter_inherits_base(self):
        """SFSDadiExporter should inherit from BaseExporter."""
        from graphmana.export.base import BaseExporter

        assert issubclass(SFSDadiExporter, BaseExporter)


# ---------------------------------------------------------------------------
# Integration tests — export() with mocked Neo4j
# ---------------------------------------------------------------------------


class TestDadiExportIntegration:
    """Test SFSDadiExporter.export() end-to-end with mocked data."""

    def test_export_1d_returns_valid_summary(self, tmp_path):
        """1-pop export returns correct summary dict."""
        exporter = _make_dadi_exporter()
        out = tmp_path / "test.fs"
        summary = exporter.export(
            out, populations=["AFR"], projection=[10], polarized=True
        )

        assert summary["n_variants"] == 2  # v3 monomorphic skipped
        assert summary["n_samples"] == 20
        assert summary["format"] == "sfs-dadi"
        assert summary["chromosomes"] == ["chr1"]
        assert summary["populations"] == ["AFR"]
        assert summary["projection"] == [10]
        assert summary["polarized"] is True

    def test_export_1d_produces_valid_file(self, tmp_path):
        """1-pop export writes correct .fs format."""
        exporter = _make_dadi_exporter()
        out = tmp_path / "test.fs"
        exporter.export(out, populations=["AFR"], projection=[10], polarized=True)

        lines = out.read_text().strip().split("\n")
        assert len(lines) == 4  # comment, dims, values, mask
        assert lines[0].startswith("# ")
        assert "2 SNPs" in lines[0]
        assert lines[1] == "11"  # proj+1 = 11

        # Values line: 11 floats
        values = lines[2].split()
        assert len(values) == 11

        # Mask line: all zeros (polarized)
        mask = lines[3].split()
        assert all(m == "0" for m in mask)

    def test_export_2d_returns_valid_summary(self, tmp_path):
        """2-pop export returns correct summary dict."""
        exporter = _make_dadi_exporter()
        out = tmp_path / "test.fs"
        summary = exporter.export(
            out, populations=["AFR", "EUR"], projection=[10, 10], polarized=True
        )

        assert summary["n_variants"] == 2
        assert summary["populations"] == ["AFR", "EUR"]
        assert summary["projection"] == [10, 10]

    def test_export_folded_masks_edges(self, tmp_path):
        """Folded (non-polarized) SFS masks bin 0 and last bin."""
        # Use non-polarized variants
        variants = [
            {"variantId": "v1", "pop_ids": ["AFR"], "ac": [5], "an": [20],
             "ac_total": 5, "an_total": 20, "is_polarized": False},
        ]
        exporter = _make_dadi_exporter(variants=variants)
        out = tmp_path / "test.fs"
        exporter.export(
            out, populations=["AFR"], projection=[10], polarized=False
        )

        lines = out.read_text().strip().split("\n")
        mask = lines[3].split()
        assert mask[0] == "1"   # first bin masked
        assert mask[-1] == "1"  # last bin masked

    def test_export_projection_mismatch_raises(self, tmp_path):
        """Mismatched populations and projections raises ValueError."""
        exporter = _make_dadi_exporter()
        with pytest.raises(ValueError, match="must match"):
            exporter.export(
                tmp_path / "test.fs",
                populations=["AFR", "EUR"],
                projection=[10],  # only 1 projection for 2 pops
                polarized=True,
            )

    def test_export_too_many_populations_raises(self, tmp_path):
        """More than 3 populations raises ValueError."""
        exporter = _make_dadi_exporter()
        with pytest.raises(ValueError, match="1-3 populations"):
            exporter.export(
                tmp_path / "test.fs",
                populations=["A", "B", "C", "D"],
                projection=[10, 10, 10, 10],
                polarized=True,
            )

    def test_export_skips_monomorphic_by_default(self, tmp_path):
        """Monomorphic variants (ac=0 or ac=an) are excluded by default."""
        exporter = _make_dadi_exporter()
        out = tmp_path / "test.fs"
        summary = exporter.export(
            out, populations=["AFR"], projection=[10], polarized=True
        )
        assert summary["n_variants"] == 2  # v3 (ac=0) skipped

    def test_export_includes_monomorphic_when_requested(self, tmp_path):
        """include_monomorphic=True includes sites with ac=0."""
        exporter = _make_dadi_exporter()
        out = tmp_path / "test.fs"
        summary = exporter.export(
            out,
            populations=["AFR"],
            projection=[10],
            polarized=True,
            include_monomorphic=True,
        )
        assert summary["n_variants"] == 3  # all 3 variants

    def test_export_empty_variants_raises(self, tmp_path):
        """No variants with population data raises ValueError."""
        exporter = _make_dadi_exporter(variants=[])
        with pytest.raises(ValueError, match="No variants"):
            exporter.export(
                tmp_path / "test.fs",
                populations=["AFR"],
                projection=[10],
                polarized=True,
            )

    def test_export_uses_fast_path(self, tmp_path):
        """dadi SFS must use _iter_variants_fast (FAST PATH)."""
        exporter = _make_dadi_exporter()
        exporter.export(
            tmp_path / "test.fs",
            populations=["AFR"],
            projection=[10],
            polarized=True,
        )
        exporter._iter_variants_fast.assert_called()
        exporter._get_sample_count.assert_called_once()
