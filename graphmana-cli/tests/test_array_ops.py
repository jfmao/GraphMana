"""Tests for array extension utilities used in incremental sample addition."""

import numpy as np
import pytest

from graphmana.ingest.array_ops import (
    extend_gt_packed,
    extend_phase_packed,
    extend_ploidy_packed,
    merge_pop_stats,
    pad_gt_for_new_variant,
    pad_phase_for_new_variant,
)
from graphmana.ingest.genotype_packer import (
    GT_REMAP,
    build_ploidy_packed,
    unpack_genotypes,
    unpack_phase,
    unpack_ploidy,
    vectorized_gt_pack,
)


class TestExtendGtPacked:
    """Test extending gt_packed with new samples."""

    def test_basic_extension(self):
        """Extend 4 existing samples with 2 new ones."""
        # Existing: 4 samples (HomRef, Het, HomAlt, Missing) in cyvcf2 codes
        existing_cyvcf2 = np.array([0, 1, 3, 2], dtype=np.int8)
        existing_packed = vectorized_gt_pack(existing_cyvcf2)

        # New: 2 samples (Het, HomRef) in cyvcf2 codes
        new_gt = np.array([1, 0], dtype=np.int8)

        result = extend_gt_packed(existing_packed, 4, new_gt)
        unpacked = unpack_genotypes(result, 6)

        # Expected packed codes: HomRef=0, Het=1, HomAlt=2, Missing=3, Het=1, HomRef=0
        expected = np.array([0, 1, 2, 3, 1, 0], dtype=np.int8)
        np.testing.assert_array_equal(unpacked, expected)

    def test_non_multiple_of_4_existing(self):
        """Extend when existing count is not a multiple of 4."""
        # 3 existing samples
        existing_cyvcf2 = np.array([0, 1, 3], dtype=np.int8)
        existing_packed = vectorized_gt_pack(existing_cyvcf2)

        # 2 new samples
        new_gt = np.array([3, 2], dtype=np.int8)

        result = extend_gt_packed(existing_packed, 3, new_gt)
        unpacked = unpack_genotypes(result, 5)

        expected = np.array([0, 1, 2, 2, 3], dtype=np.int8)
        np.testing.assert_array_equal(unpacked, expected)

    def test_empty_existing(self):
        """Extend from zero existing samples (fresh start)."""
        new_gt = np.array([0, 1, 3, 2], dtype=np.int8)
        result = extend_gt_packed(b"", 0, new_gt)
        unpacked = unpack_genotypes(result, 4)

        expected = GT_REMAP[new_gt].astype(np.int8)
        np.testing.assert_array_equal(unpacked, expected)

    def test_single_existing_single_new(self):
        """Extend 1 existing sample with 1 new."""
        existing = vectorized_gt_pack(np.array([3], dtype=np.int8))  # HomAlt
        new_gt = np.array([1], dtype=np.int8)  # Het
        result = extend_gt_packed(existing, 1, new_gt)
        unpacked = unpack_genotypes(result, 2)
        expected = np.array([2, 1], dtype=np.int8)  # HomAlt=2, Het=1
        np.testing.assert_array_equal(unpacked, expected)

    def test_all_homref_extension(self):
        """Extending with all-HomRef new samples."""
        existing = vectorized_gt_pack(np.array([0, 1, 3, 2], dtype=np.int8))
        new_gt = np.zeros(4, dtype=np.int8)  # All HomRef
        result = extend_gt_packed(existing, 4, new_gt)
        unpacked = unpack_genotypes(result, 8)

        expected = np.array([0, 1, 2, 3, 0, 0, 0, 0], dtype=np.int8)
        np.testing.assert_array_equal(unpacked, expected)

    def test_large_roundtrip(self):
        """100 existing + 50 new samples roundtrip correctly."""
        rng = np.random.default_rng(42)
        existing_cyvcf2 = rng.integers(0, 4, size=100, dtype=np.int8)
        new_cyvcf2 = rng.integers(0, 4, size=50, dtype=np.int8)

        existing_packed = vectorized_gt_pack(existing_cyvcf2)
        result = extend_gt_packed(existing_packed, 100, new_cyvcf2)
        unpacked = unpack_genotypes(result, 150)

        # Build expected: remap existing + remap new
        expected_old = GT_REMAP[existing_cyvcf2].astype(np.int8)
        expected_new = GT_REMAP[new_cyvcf2].astype(np.int8)
        expected = np.concatenate([expected_old, expected_new])
        np.testing.assert_array_equal(unpacked, expected)


class TestExtendPhasePacked:
    """Test extending phase_packed with new samples."""

    def test_basic_extension(self):
        """Extend 8 existing phase bits with 4 new."""
        existing_bits = np.array([1, 0, 1, 0, 0, 1, 0, 0], dtype=np.uint8)
        existing_packed = np.packbits(existing_bits, bitorder="little").tobytes()

        new_bits = np.array([1, 1, 0, 1], dtype=np.uint8)
        result = extend_phase_packed(existing_packed, 8, new_bits)
        unpacked = unpack_phase(result, 12)

        expected = np.concatenate([existing_bits, new_bits])
        np.testing.assert_array_equal(unpacked, expected)

    def test_non_multiple_of_8_existing(self):
        """Extend when existing count is not a multiple of 8."""
        existing_bits = np.array([1, 0, 1], dtype=np.uint8)
        existing_packed = np.packbits(existing_bits, bitorder="little").tobytes()

        new_bits = np.array([0, 1], dtype=np.uint8)
        result = extend_phase_packed(existing_packed, 3, new_bits)
        unpacked = unpack_phase(result, 5)

        expected = np.array([1, 0, 1, 0, 1], dtype=np.uint8)
        np.testing.assert_array_equal(unpacked, expected)

    def test_empty_existing(self):
        """Extend from zero existing."""
        new_bits = np.array([1, 0, 1, 0], dtype=np.uint8)
        result = extend_phase_packed(b"", 0, new_bits)
        unpacked = unpack_phase(result, 4)
        np.testing.assert_array_equal(unpacked, new_bits)

    def test_all_zero_extension(self):
        """Extending with all-zero phase bits."""
        existing_packed = np.packbits(
            np.array([1, 1, 1, 1, 1, 1, 1, 1], dtype=np.uint8), bitorder="little"
        ).tobytes()
        new_bits = np.zeros(4, dtype=np.uint8)
        result = extend_phase_packed(existing_packed, 8, new_bits)
        unpacked = unpack_phase(result, 12)

        expected = np.array([1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0], dtype=np.uint8)
        np.testing.assert_array_equal(unpacked, expected)


class TestExtendPloidyPacked:
    """Test extending ploidy_packed with new samples."""

    def test_none_to_none(self):
        """All diploid existing + all diploid new = None."""
        new_haploid = np.zeros(4, dtype=np.uint8)
        result = extend_ploidy_packed(None, 4, new_haploid)
        assert result is None

    def test_none_to_some_haploid(self):
        """All diploid existing + some haploid new."""
        new_haploid = np.array([0, 1, 0, 1], dtype=np.uint8)
        result = extend_ploidy_packed(None, 4, new_haploid)
        assert result is not None
        unpacked = unpack_ploidy(result, 8)
        expected = np.array([0, 0, 0, 0, 0, 1, 0, 1], dtype=np.uint8)
        np.testing.assert_array_equal(unpacked, expected)

    def test_some_haploid_to_diploid(self):
        """Some haploid existing + all diploid new."""
        existing_flags = np.array([1, 0, 1, 0], dtype=bool)
        existing_packed = build_ploidy_packed(existing_flags)
        new_haploid = np.zeros(4, dtype=np.uint8)
        result = extend_ploidy_packed(existing_packed, 4, new_haploid)
        assert result is not None
        unpacked = unpack_ploidy(result, 8)
        expected = np.array([1, 0, 1, 0, 0, 0, 0, 0], dtype=np.uint8)
        np.testing.assert_array_equal(unpacked, expected)

    def test_roundtrip(self):
        """Mixed haploid flags roundtrip correctly."""
        existing_flags = np.array([True, False, True], dtype=bool)
        existing_packed = build_ploidy_packed(existing_flags)
        new_haploid = np.array([0, 1], dtype=np.uint8)
        result = extend_ploidy_packed(existing_packed, 3, new_haploid)
        unpacked = unpack_ploidy(result, 5)
        expected = np.array([1, 0, 1, 0, 1], dtype=np.uint8)
        np.testing.assert_array_equal(unpacked, expected)


class TestPadGtForNewVariant:
    """Test creating gt_packed for new variants.

    Default (v1.1, pop-gen correct): existing samples are padded with Missing
    (code 3), because the current input batch carries no information about
    their genotype at the new site. Allele frequency denominators honor this
    via the called_packed mask. Legacy "assume HomRef" semantics remain
    available via ``assume_homref=True`` for fixed-site-list workflows.
    """

    def test_basic_padding_missing_default(self):
        """4 existing Missing + 2 new samples (v1.1 default)."""
        new_gt = np.array([1, 3], dtype=np.int8)  # Het, HomAlt
        result = pad_gt_for_new_variant(4, new_gt)
        unpacked = unpack_genotypes(result, 6)

        # 4 Missing (3) + Het (1) + HomAlt (2)
        expected = np.array([3, 3, 3, 3, 1, 2], dtype=np.int8)
        np.testing.assert_array_equal(unpacked, expected)

    def test_basic_padding_legacy_homref(self):
        """Legacy mode preserves the v1.0 "absent = HomRef" assumption."""
        new_gt = np.array([1, 3], dtype=np.int8)  # Het, HomAlt
        result = pad_gt_for_new_variant(4, new_gt, assume_homref=True)
        unpacked = unpack_genotypes(result, 6)
        expected = np.array([0, 0, 0, 0, 1, 2], dtype=np.int8)
        np.testing.assert_array_equal(unpacked, expected)

    def test_zero_existing(self):
        """No existing samples — just new samples, padding mode is irrelevant."""
        new_gt = np.array([0, 1, 3, 2], dtype=np.int8)
        result = pad_gt_for_new_variant(0, new_gt)
        unpacked = unpack_genotypes(result, 4)
        expected = GT_REMAP[new_gt].astype(np.int8)
        np.testing.assert_array_equal(unpacked, expected)

    def test_non_multiple_of_4(self):
        """3 existing + 2 new = 5 total (non-multiple of 4)."""
        new_gt = np.array([3, 2], dtype=np.int8)
        result = pad_gt_for_new_variant(3, new_gt)
        unpacked = unpack_genotypes(result, 5)
        # Default: existing → Missing (3)
        expected = np.array([3, 3, 3, 2, 3], dtype=np.int8)
        np.testing.assert_array_equal(unpacked, expected)


class TestPadPhaseForNewVariant:
    """Test creating phase_packed for new variants with zero padding."""

    def test_basic_padding(self):
        """4 existing zeros + 2 new phase bits."""
        new_phase = np.array([1, 0], dtype=np.uint8)
        result = pad_phase_for_new_variant(4, new_phase)
        unpacked = unpack_phase(result, 6)
        expected = np.array([0, 0, 0, 0, 1, 0], dtype=np.uint8)
        np.testing.assert_array_equal(unpacked, expected)

    def test_zero_existing(self):
        """No existing — just new."""
        new_phase = np.array([1, 0, 1], dtype=np.uint8)
        result = pad_phase_for_new_variant(0, new_phase)
        unpacked = unpack_phase(result, 3)
        np.testing.assert_array_equal(unpacked, new_phase)


class TestMergePopStats:
    """Test population statistics merging."""

    def test_identical_populations(self):
        """Both sides have the same populations."""
        result = merge_pop_stats(
            existing_pop_ids=["EUR", "AFR"],
            existing_ac=[10, 20],
            existing_an=[100, 200],
            existing_het_count=[8, 15],
            existing_hom_alt_count=[1, 2],
            new_pop_ids=["AFR", "EUR"],
            new_ac=[5, 3],
            new_an=[50, 30],
            new_het_count=[4, 2],
            new_hom_alt_count=[0, 1],
        )
        # Sorted: AFR, EUR
        assert result["pop_ids"] == ["AFR", "EUR"]
        assert result["ac"] == [25, 13]  # AFR: 20+5, EUR: 10+3
        assert result["an"] == [250, 130]  # AFR: 200+50, EUR: 100+30
        assert result["het_count"] == [19, 10]
        assert result["hom_alt_count"] == [2, 2]
        assert result["ac_total"] == 38
        assert result["an_total"] == 380
        assert pytest.approx(result["af_total"]) == 38 / 380

    def test_new_population_added(self):
        """New VCF introduces a population not in the existing DB."""
        result = merge_pop_stats(
            existing_pop_ids=["EUR"],
            existing_ac=[10],
            existing_an=[100],
            existing_het_count=[8],
            existing_hom_alt_count=[1],
            new_pop_ids=["AFR"],
            new_ac=[5],
            new_an=[50],
            new_het_count=[4],
            new_hom_alt_count=[0],
        )
        assert result["pop_ids"] == ["AFR", "EUR"]
        assert result["ac"] == [5, 10]
        assert result["an"] == [50, 100]

    def test_existing_only_population_preserved(self):
        """Population in existing DB but not in new VCF is preserved."""
        result = merge_pop_stats(
            existing_pop_ids=["AFR", "EUR"],
            existing_ac=[20, 10],
            existing_an=[200, 100],
            existing_het_count=[15, 8],
            existing_hom_alt_count=[2, 1],
            new_pop_ids=["EUR"],
            new_ac=[3],
            new_an=[30],
            new_het_count=[2],
            new_hom_alt_count=[1],
        )
        assert result["pop_ids"] == ["AFR", "EUR"]
        assert result["ac"] == [20, 13]  # AFR unchanged, EUR merged
        assert result["an"] == [200, 130]

    def test_af_computation(self):
        """Allele frequencies are correctly computed."""
        result = merge_pop_stats(
            existing_pop_ids=["POP"],
            existing_ac=[10],
            existing_an=[100],
            existing_het_count=[5],
            existing_hom_alt_count=[2],
            new_pop_ids=["POP"],
            new_ac=[0],
            new_an=[50],
            new_het_count=[0],
            new_hom_alt_count=[0],
        )
        assert pytest.approx(result["af"][0]) == 10 / 150

    def test_zero_an_produces_zero_af(self):
        """Zero allele number produces zero allele frequency."""
        result = merge_pop_stats(
            existing_pop_ids=["POP"],
            existing_ac=[0],
            existing_an=[0],
            existing_het_count=[0],
            existing_hom_alt_count=[0],
            new_pop_ids=["POP"],
            new_ac=[0],
            new_an=[0],
            new_het_count=[0],
            new_hom_alt_count=[0],
        )
        assert result["af"][0] == 0.0
        assert result["af_total"] == 0.0

    def test_het_exp_computation(self):
        """Expected heterozygosity is 2*p*(1-p)."""
        result = merge_pop_stats(
            existing_pop_ids=["POP"],
            existing_ac=[50],
            existing_an=[100],
            existing_het_count=[30],
            existing_hom_alt_count=[10],
            new_pop_ids=[],
            new_ac=[],
            new_an=[],
            new_het_count=[],
            new_hom_alt_count=[],
        )
        af = 50 / 100
        expected_het_exp = 2.0 * af * (1.0 - af)
        assert pytest.approx(result["het_exp"][0]) == expected_het_exp

    def test_empty_new(self):
        """Empty new populations — existing stats unchanged."""
        result = merge_pop_stats(
            existing_pop_ids=["EUR", "AFR"],
            existing_ac=[10, 20],
            existing_an=[100, 200],
            existing_het_count=[8, 15],
            existing_hom_alt_count=[1, 2],
            new_pop_ids=[],
            new_ac=[],
            new_an=[],
            new_het_count=[],
            new_hom_alt_count=[],
        )
        assert result["pop_ids"] == ["AFR", "EUR"]
        assert result["ac"] == [20, 10]
        assert result["an"] == [200, 100]

    def test_empty_existing(self):
        """Empty existing — new stats are the result."""
        result = merge_pop_stats(
            existing_pop_ids=[],
            existing_ac=[],
            existing_an=[],
            existing_het_count=[],
            existing_hom_alt_count=[],
            new_pop_ids=["EUR"],
            new_ac=[10],
            new_an=[100],
            new_het_count=[8],
            new_hom_alt_count=[1],
        )
        assert result["pop_ids"] == ["EUR"]
        assert result["ac"] == [10]
        assert result["an"] == [100]
