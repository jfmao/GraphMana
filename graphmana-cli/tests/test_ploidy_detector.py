"""Tests for ploidy detection."""

from unittest.mock import MagicMock

import numpy as np

from graphmana.ingest.ploidy_detector import detect_ploidy


def _mock_variant(genotypes_list):
    """Create a mock cyvcf2 Variant with given genotypes list."""
    v = MagicMock()
    v.genotypes = genotypes_list
    return v


class TestDetectPloidy:
    def test_all_diploid(self):
        """All samples have 3-element genotypes → all_diploid."""
        v = _mock_variant(
            [
                [0, 0, True],
                [0, 1, True],
                [1, 1, True],
            ]
        )
        mode, flags = detect_ploidy(v)
        assert mode == "all_diploid"
        assert flags.sum() == 0
        assert len(flags) == 3

    def test_all_haploid(self):
        """All samples have 2-element genotypes → all_haploid."""
        v = _mock_variant(
            [
                [0, True],
                [1, True],
                [0, False],
                [1, True],
            ]
        )
        mode, flags = detect_ploidy(v)
        assert mode == "all_haploid"
        assert flags.sum() == 4
        assert all(flags)

    def test_mixed(self):
        """Mix of diploid and haploid → mixed with correct flags."""
        v = _mock_variant(
            [
                [0, 0, True],  # diploid
                [0, 1, True],  # diploid
                [0, True],  # haploid
                [1, True],  # haploid
                [0, True],  # haploid
            ]
        )
        mode, flags = detect_ploidy(v)
        assert mode == "mixed"
        expected = [False, False, True, True, True]
        np.testing.assert_array_equal(flags, expected)

    def test_single_sample_diploid(self):
        """Single diploid sample."""
        v = _mock_variant([[0, 0, True]])
        mode, flags = detect_ploidy(v)
        assert mode == "all_diploid"
        assert len(flags) == 1
        assert not flags[0]

    def test_single_sample_haploid(self):
        """Single haploid sample."""
        v = _mock_variant([[1, True]])
        mode, flags = detect_ploidy(v)
        assert mode == "all_haploid"
        assert len(flags) == 1
        assert flags[0]
