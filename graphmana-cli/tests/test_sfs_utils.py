"""Tests for SFS utility functions."""

import numpy as np
import pytest

from graphmana.export.sfs_utils import (
    build_sfs_1d,
    build_sfs_2d,
    fold_sfs,
    fold_sfs_2d,
    hypergeometric_projection,
)


class TestHypergeometricProjection:
    """Test hypergeometric down-sampling."""

    def test_no_projection_needed(self):
        """When an == proj, result is deterministic."""
        # ac=3, an=10, proj=10 -> sfs[3] = 1.0
        result = hypergeometric_projection(3, 10, 10)
        assert len(result) == 11
        assert result[3] == pytest.approx(1.0)
        assert sum(result) == pytest.approx(1.0)

    def test_projection_sums_to_one(self):
        """Projected probabilities should sum to 1.0."""
        result = hypergeometric_projection(5, 20, 10)
        assert len(result) == 11
        assert sum(result) == pytest.approx(1.0, abs=1e-10)

    def test_zero_ac(self):
        """All reference alleles -> sfs[0] = 1.0."""
        result = hypergeometric_projection(0, 20, 10)
        assert result[0] == pytest.approx(1.0)
        assert sum(result[1:]) == pytest.approx(0.0)

    def test_all_alt(self):
        """All alt alleles -> sfs[proj] = 1.0."""
        result = hypergeometric_projection(20, 20, 10)
        assert result[10] == pytest.approx(1.0)
        assert sum(result[:10]) == pytest.approx(0.0)

    def test_insufficient_data(self):
        """When an < proj, return zeros."""
        result = hypergeometric_projection(3, 5, 10)
        assert np.all(result == 0.0)

    def test_zero_an(self):
        """an=0 returns zeros."""
        result = hypergeometric_projection(0, 0, 10)
        assert np.all(result == 0.0)

    def test_known_values(self):
        """Verify specific projection values.

        ac=1, an=4, proj=2:
        P(k=0) = C(1,0)*C(3,2)/C(4,2) = 1*3/6 = 0.5
        P(k=1) = C(1,1)*C(3,1)/C(4,2) = 1*3/6 = 0.5
        P(k=2) = C(1,2)*C(3,0)/C(4,2) = 0
        """
        result = hypergeometric_projection(1, 4, 2)
        assert result[0] == pytest.approx(0.5)
        assert result[1] == pytest.approx(0.5)
        assert result[2] == pytest.approx(0.0)

    def test_half_alt(self):
        """ac=an/2 should give symmetric distribution.

        ac=5, an=10, proj=4 -> symmetric around 2.
        """
        result = hypergeometric_projection(5, 10, 4)
        assert result[0] == pytest.approx(result[4])
        assert result[1] == pytest.approx(result[3])


class TestFoldSFS:
    """Test SFS folding."""

    def test_fold_simple(self):
        """Fold combines bins k and n-k."""
        # SFS: [10, 5, 3, 2, 8] (n=4)
        sfs = np.array([10.0, 5.0, 3.0, 2.0, 8.0])
        folded = fold_sfs(sfs)
        # k=0: 10+8=18, k=1: 5+2=7, k=2: 3 (middle, unchanged)
        assert folded[0] == pytest.approx(18.0)
        assert folded[1] == pytest.approx(7.0)
        assert folded[2] == pytest.approx(3.0)
        assert folded[3] == pytest.approx(0.0)
        assert folded[4] == pytest.approx(0.0)

    def test_fold_even(self):
        """Even-length SFS: middle bin stays."""
        sfs = np.array([10.0, 5.0, 3.0])  # n=2
        folded = fold_sfs(sfs)
        assert folded[0] == pytest.approx(13.0)  # 10 + 3
        assert folded[1] == pytest.approx(5.0)  # middle

    def test_fold_preserves_total(self):
        """Folding preserves total count."""
        sfs = np.array([10.0, 5.0, 3.0, 2.0, 8.0])
        folded = fold_sfs(sfs)
        assert sum(folded) == pytest.approx(sum(sfs))


class TestBuildSFS1D:
    """Test 1D SFS construction."""

    def test_single_variant_polarized(self):
        """Single polarized variant contributes to SFS."""
        variants = [
            {"ac": [3], "an": [10], "is_polarized": True},
        ]
        sfs = build_sfs_1d(variants, pop_idx=0, projection=10, polarized=True)
        assert len(sfs) == 11
        assert sfs[3] == pytest.approx(1.0)

    def test_unpolarized_skipped_when_polarized(self):
        """Unpolarized variants should be skipped in polarized mode."""
        variants = [
            {"ac": [3], "an": [10], "is_polarized": False},
        ]
        sfs = build_sfs_1d(variants, pop_idx=0, projection=10, polarized=True)
        assert np.all(sfs == 0.0)

    def test_folded_mode(self):
        """Folded SFS should be computed when polarized=False."""
        variants = [
            {"ac": [8], "an": [10], "is_polarized": False},
        ]
        sfs = build_sfs_1d(variants, pop_idx=0, projection=10, polarized=False)
        # Folded: ac=8 -> bin 8 maps to bin 2 (minor allele)
        assert sfs[2] > 0.0

    def test_insufficient_data_skipped(self):
        """Variants with an < projection should be skipped."""
        variants = [
            {"ac": [3], "an": [4], "is_polarized": True},
        ]
        sfs = build_sfs_1d(variants, pop_idx=0, projection=10, polarized=True)
        assert np.all(sfs == 0.0)

    def test_multiple_variants(self):
        """Multiple variants accumulate in SFS."""
        variants = [
            {"ac": [3], "an": [10], "is_polarized": True},
            {"ac": [3], "an": [10], "is_polarized": True},
        ]
        sfs = build_sfs_1d(variants, pop_idx=0, projection=10, polarized=True)
        assert sfs[3] == pytest.approx(2.0)

    def test_missing_arrays_skipped(self):
        """Variants without ac/an should be skipped."""
        variants = [{"pos": 100}]
        sfs = build_sfs_1d(variants, pop_idx=0, projection=10, polarized=True)
        assert np.all(sfs == 0.0)


class TestBuildSFS2D:
    """Test 2D joint SFS construction."""

    def test_two_pop_shape(self):
        """2D SFS should have shape (proj1+1, proj2+1)."""
        variants = [
            {"ac": [3, 5], "an": [10, 10], "is_polarized": True},
        ]
        sfs = build_sfs_2d(variants, 0, 1, 10, 10, polarized=True)
        assert sfs.shape == (11, 11)

    def test_two_pop_single_variant(self):
        """Single variant contributes via outer product."""
        variants = [
            {"ac": [3, 5], "an": [10, 10], "is_polarized": True},
        ]
        sfs = build_sfs_2d(variants, 0, 1, 10, 10, polarized=True)
        assert sfs[3, 5] == pytest.approx(1.0)
        assert sfs.sum() == pytest.approx(1.0)


class TestFoldSFS2D:
    """Test 2D SFS folding."""

    def test_preserves_total(self):
        """Folding 2D SFS should preserve total count."""
        sfs = np.random.rand(5, 5)
        folded = fold_sfs_2d(sfs)
        assert folded.sum() == pytest.approx(sfs.sum())

    def test_same_shape(self):
        """Folded 2D SFS should have same shape."""
        sfs = np.ones((5, 5))
        folded = fold_sfs_2d(sfs)
        assert folded.shape == sfs.shape
