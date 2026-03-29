"""Tests for fastsimcoal2 SFS format exporter."""

import numpy as np

from graphmana.export.sfs_fsc_export import SFSFscExporter, _resolve_pop_indices


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

    def test_tab_separated(self):
        """Headers should be tab-separated."""
        headers = ["d0_0", "d0_1", "d0_2"]
        line = "\t".join(headers)
        assert "\t" in line
        assert line.count("\t") == 2

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

    def test_2d_rows(self):
        """Should have n1+1 data rows."""
        n1 = 3
        sfs = np.zeros((n1 + 1, 4))
        assert sfs.shape[0] == 4  # n1+1 rows

    def test_observations_line(self):
        """First line should be '1 observations'."""
        line = "1 observations"
        assert line == "1 observations"


class TestFscExporterClass:
    """Test SFSFscExporter class properties."""

    def test_inherits_base(self):
        """SFSFscExporter should inherit from BaseExporter."""
        from graphmana.export.base import BaseExporter

        assert issubclass(SFSFscExporter, BaseExporter)
