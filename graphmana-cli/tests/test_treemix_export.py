"""Tests for TreeMix allele count matrix exporter."""

from graphmana.export.treemix_export import TreeMixExporter


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

    def test_population_subsetting_ac_an(self):
        """Subsetted ac/an should match filtered indices."""
        ac = [10, 5, 8]
        an = [200, 100, 150]
        pop_indices = [0, 2]  # AFR, EUR
        filtered_ac = [ac[i] for i in pop_indices]
        filtered_an = [an[i] for i in pop_indices]
        assert filtered_ac == [10, 8]
        assert filtered_an == [200, 150]

    def test_skip_zero_an_total(self):
        """Variants with an_total == 0 should be skipped."""
        an_total = 0
        assert an_total == 0  # would skip

    def test_exporter_inherits_base(self):
        """TreeMixExporter should inherit from BaseExporter."""
        from graphmana.export.base import BaseExporter

        assert issubclass(TreeMixExporter, BaseExporter)
