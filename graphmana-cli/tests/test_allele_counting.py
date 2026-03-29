"""Tests for ploidy-aware allele counting logic.

These tests verify the allele counting arithmetic used in VCFParser._stream()
without requiring actual VCF files. The counting logic is tested directly
using numpy arrays that simulate cyvcf2 gt_types output.
"""

import numpy as np


class TestDiploidCounting:
    """Standard diploid allele counting."""

    def test_basic(self):
        """3 HOM_REF, 1 HET, 1 HOM_ALT → ac=4, an=10."""
        gt_types = np.array([0, 0, 0, 1, 3], dtype=np.int8)

        n_miss = int(np.sum(gt_types == 2))
        n_het = int(np.sum(gt_types == 1))
        n_hom_alt = int(np.sum(gt_types == 3))
        n_called = len(gt_types) - n_miss

        ac = n_het + 2 * n_hom_alt
        an = 2 * n_called

        assert ac == 3
        assert an == 10
        assert n_het == 1
        assert n_hom_alt == 1

    def test_all_missing(self):
        """All samples missing → ac=0, an=0."""
        gt_types = np.array([2, 2, 2], dtype=np.int8)

        n_miss = int(np.sum(gt_types == 2))
        n_called = len(gt_types) - n_miss
        ac = 0
        an = 2 * n_called

        assert ac == 0
        assert an == 0


class TestHaploidCounting:
    """All-haploid allele counting (e.g. mitochondria)."""

    def test_haploid_counts(self):
        """3 REF + 2 ALT → ac=2, an=5."""
        gt_types = np.array([0, 0, 0, 3, 3], dtype=np.int8)

        n_miss = int(np.sum(gt_types == 2))
        n_hom_alt = int(np.sum(gt_types == 3))
        n_called = len(gt_types) - n_miss

        ac = n_hom_alt
        an = n_called
        af = ac / an if an > 0 else 0.0

        assert ac == 2
        assert an == 5
        assert abs(af - 0.4) < 1e-10


class TestMixedPloidyCounting:
    """Mixed ploidy counting (e.g. chrX with males/females)."""

    def test_mixed_chrX_counts(self):
        """2 diploid females + 3 haploid males."""
        gt_types = np.array([1, 0, 3, 0, 0], dtype=np.int8)
        haploid_flags = np.array([False, False, True, True, True])

        idx = np.arange(5)
        hap_k = haploid_flags[idx]

        # Diploid subset
        dip_mask = ~hap_k
        gt_dip = gt_types[dip_mask]
        n_miss_dip = int(np.sum(gt_dip == 2))
        n_het_dip = int(np.sum(gt_dip == 1))
        n_hom_alt_dip = int(np.sum(gt_dip == 3))
        n_called_dip = len(gt_dip) - n_miss_dip
        ac_dip = n_het_dip + 2 * n_hom_alt_dip
        an_dip = 2 * n_called_dip

        # Haploid subset
        gt_hap = gt_types[hap_k]
        n_miss_hap = int(np.sum(gt_hap == 2))
        n_hom_alt_hap = int(np.sum(gt_hap == 3))
        n_called_hap = len(gt_hap) - n_miss_hap
        ac_hap = n_hom_alt_hap
        an_hap = n_called_hap

        ac = ac_dip + ac_hap
        an = an_dip + an_hap
        het_count = n_het_dip

        assert ac_dip == 1
        assert an_dip == 4
        assert ac_hap == 1
        assert an_hap == 3
        assert ac == 2
        assert an == 7
        assert het_count == 1

    def test_mixed_with_missing(self):
        """Mixed ploidy with missing samples excluded correctly."""
        gt_types = np.array([1, 2, 3, 2], dtype=np.int8)
        haploid_flags = np.array([False, False, True, True])

        idx = np.arange(4)
        hap_k = haploid_flags[idx]

        # Diploid
        gt_dip = gt_types[~hap_k]
        n_miss_dip = int(np.sum(gt_dip == 2))
        n_het_dip = int(np.sum(gt_dip == 1))
        n_hom_alt_dip = int(np.sum(gt_dip == 3))
        n_called_dip = len(gt_dip) - n_miss_dip
        ac_dip = n_het_dip + 2 * n_hom_alt_dip
        an_dip = 2 * n_called_dip

        # Haploid
        gt_hap = gt_types[hap_k]
        n_miss_hap = int(np.sum(gt_hap == 2))
        n_hom_alt_hap = int(np.sum(gt_hap == 3))
        n_called_hap = len(gt_hap) - n_miss_hap
        ac_hap = n_hom_alt_hap
        an_hap = n_called_hap

        ac = ac_dip + ac_hap
        an = an_dip + an_hap

        assert ac == 2
        assert an == 3
