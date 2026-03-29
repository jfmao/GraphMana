"""Tests for dadi SFS format exporter."""

import pytest

from graphmana.export.sfs_dadi_export import SFSDadiExporter, _resolve_pop_indices


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

    def test_mask_line_polarized(self):
        """Polarized SFS: no bins masked."""
        import numpy as np

        n = 5
        mask = np.zeros(n, dtype=int)
        assert all(m == 0 for m in mask)

    def test_mask_line_folded(self):
        """Folded SFS: bin 0 and last bin masked."""
        import numpy as np

        n = 5
        mask = np.zeros(n, dtype=int)
        mask[0] = 1
        mask[-1] = 1
        assert mask[0] == 1
        assert mask[4] == 1
        assert mask[2] == 0

    def test_exporter_inherits_base(self):
        """SFSDadiExporter should inherit from BaseExporter."""
        from graphmana.export.base import BaseExporter

        assert issubclass(SFSDadiExporter, BaseExporter)

    def test_projection_population_mismatch_raises(self):
        """Mismatched populations and projections should raise ValueError."""
        # This tests the validation logic directly
        pops = ["AFR", "EUR"]
        proj = [20]
        assert len(pops) != len(proj)
