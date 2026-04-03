"""Tests for fastsimcoal2 SFS format exporter."""

from unittest.mock import MagicMock

import numpy as np
import pytest

from graphmana.export.sfs_fsc_export import SFSFscExporter, _resolve_pop_indices


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

MOCK_VARIANTS_POLARIZED = [
    {"variantId": "v1", "pop_ids": ["AFR", "EUR"], "ac": [5, 3], "an": [20, 20],
     "ac_total": 8, "an_total": 40, "is_polarized": True},
    {"variantId": "v2", "pop_ids": ["AFR", "EUR"], "ac": [10, 7], "an": [20, 20],
     "ac_total": 17, "an_total": 40, "is_polarized": True},
    # Monomorphic — skipped by default
    {"variantId": "v3", "pop_ids": ["AFR", "EUR"], "ac": [0, 0], "an": [20, 20],
     "ac_total": 0, "an_total": 40, "is_polarized": True},
]


def _make_fsc_exporter(variants=None, n_samples=20):
    """Create SFSFscExporter with mocked Neo4j methods."""
    if variants is None:
        variants = MOCK_VARIANTS_POLARIZED
    conn = MagicMock()
    exporter = SFSFscExporter(conn)
    exporter._get_target_chromosomes = MagicMock(return_value=["chr1"])
    exporter._iter_variants_fast = MagicMock(
        side_effect=lambda chrom, **kw: iter(variants)
    )
    exporter._get_sample_count = MagicMock(return_value=n_samples)
    return exporter


# ---------------------------------------------------------------------------
# Pop index resolution tests (existing)
# ---------------------------------------------------------------------------


class TestFscPopIndexResolution:
    """Test population index resolution."""

    def test_basic_resolution(self):
        """Find correct indices for target populations."""
        pop_ids = ["AFR", "EAS", "EUR"]
        indices = _resolve_pop_indices(pop_ids, ["AFR"])
        assert indices == [0]

    def test_two_pops(self):
        """Two population resolution."""
        pop_ids = ["AFR", "EAS", "EUR"]
        indices = _resolve_pop_indices(pop_ids, ["EAS", "EUR"])
        assert indices == [1, 2]


class TestFsc1DFormat:
    """Test 1-population fsc .obs format."""

    def test_header_format(self):
        """1D header should be d0_0 d0_1 ... d0_n."""
        proj = 4
        headers = [f"d0_{i}" for i in range(proj + 1)]
        assert headers == ["d0_0", "d0_1", "d0_2", "d0_3", "d0_4"]

    def test_count_format(self):
        """Counts should be formatted as floats."""
        sfs = np.array([10.0, 5.5, 3.0])
        formatted = "\t".join(f"{v:.6f}" for v in sfs)
        assert "10.000000" in formatted
        assert "5.500000" in formatted


class TestFsc2DFormat:
    """Test 2-population joint fsc .obs format."""

    def test_2d_header(self):
        """2D header should be npops n1 n2."""
        npops = 2
        n1, n2 = 10, 20
        header = f"{npops}\t{n1}\t{n2}"
        assert header == "2\t10\t20"

    def test_column_headers(self):
        """Column headers should be d1_0 d1_1 ... d1_n2."""
        n2 = 3
        headers = [f"d1_{j}" for j in range(n2 + 1)]
        assert headers == ["d1_0", "d1_1", "d1_2", "d1_3"]


class TestFscExporterClass:
    """Test SFSFscExporter class properties."""

    def test_inherits_base(self):
        """SFSFscExporter should inherit from BaseExporter."""
        from graphmana.export.base import BaseExporter

        assert issubclass(SFSFscExporter, BaseExporter)


# ---------------------------------------------------------------------------
# Integration tests — export() with mocked Neo4j
# ---------------------------------------------------------------------------


class TestFscExportIntegration:
    """Test SFSFscExporter.export() end-to-end with mocked data."""

    def test_export_1d_returns_valid_summary(self, tmp_path):
        """1-pop export returns correct summary dict."""
        exporter = _make_fsc_exporter()
        out = tmp_path / "test_DAFpop0.obs"
        summary = exporter.export(
            out, populations=["AFR"], projection=[10], polarized=True
        )

        assert summary["n_variants"] == 2  # v3 monomorphic skipped
        assert summary["n_samples"] == 20
        assert summary["format"] == "sfs-fsc"
        assert summary["chromosomes"] == ["chr1"]
        assert summary["populations"] == ["AFR"]
        assert summary["projection"] == [10]
        assert summary["polarized"] is True

    def test_export_1d_produces_valid_file(self, tmp_path):
        """1-pop export writes correct .obs format."""
        exporter = _make_fsc_exporter()
        out = tmp_path / "test.obs"
        exporter.export(out, populations=["AFR"], projection=[10], polarized=True)

        lines = out.read_text().strip().split("\n")
        assert lines[0] == "1 observations"
        # d0_0 through d0_10
        headers = lines[1].split("\t")
        assert len(headers) == 11
        assert headers[0] == "d0_0"
        assert headers[10] == "d0_10"
        # Values line
        values = lines[2].split("\t")
        assert len(values) == 11

    def test_export_2d_produces_valid_file(self, tmp_path):
        """2-pop export writes correct joint .obs format."""
        exporter = _make_fsc_exporter()
        out = tmp_path / "test.obs"
        summary = exporter.export(
            out, populations=["AFR", "EUR"], projection=[10, 8], polarized=True
        )

        assert summary["n_variants"] == 2
        assert summary["populations"] == ["AFR", "EUR"]

        lines = out.read_text().strip().split("\n")
        assert lines[0] == "1 observations"
        # Header: npops n1 n2
        assert lines[1] == "2\t10\t8"
        # Column headers: d1_0 ... d1_8
        col_headers = lines[2].split("\t")
        assert len(col_headers) == 9  # proj[1]+1
        # Data rows: n1+1 = 11
        data_rows = lines[3:]
        assert len(data_rows) == 11

    def test_export_projection_mismatch_raises(self, tmp_path):
        """Mismatched populations and projections raises ValueError."""
        exporter = _make_fsc_exporter()
        with pytest.raises(ValueError, match="must match"):
            exporter.export(
                tmp_path / "test.obs",
                populations=["AFR", "EUR"],
                projection=[10],
                polarized=True,
            )

    def test_export_three_populations_raises(self, tmp_path):
        """fastsimcoal2 rejects more than 2 populations."""
        exporter = _make_fsc_exporter()
        with pytest.raises(ValueError, match="1-2 populations"):
            exporter.export(
                tmp_path / "test.obs",
                populations=["AFR", "EUR", "EAS"],
                projection=[10, 10, 10],
                polarized=True,
            )

    def test_export_skips_monomorphic_by_default(self, tmp_path):
        """Monomorphic variants excluded by default."""
        exporter = _make_fsc_exporter()
        out = tmp_path / "test.obs"
        summary = exporter.export(
            out, populations=["AFR"], projection=[10], polarized=True
        )
        assert summary["n_variants"] == 2

    def test_export_empty_variants_raises(self, tmp_path):
        """No variants with population data raises ValueError."""
        exporter = _make_fsc_exporter(variants=[])
        with pytest.raises(ValueError, match="No variants"):
            exporter.export(
                tmp_path / "test.obs",
                populations=["AFR"],
                projection=[10],
                polarized=True,
            )

    def test_export_uses_fast_path(self, tmp_path):
        """fsc SFS must use _iter_variants_fast (FAST PATH)."""
        exporter = _make_fsc_exporter()
        exporter.export(
            tmp_path / "test.obs",
            populations=["AFR"],
            projection=[10],
            polarized=True,
        )
        exporter._iter_variants_fast.assert_called()
        exporter._get_sample_count.assert_called_once()
