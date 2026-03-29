"""Tests for hard delete array operations."""

import numpy as np
import pytest

from graphmana.ingest.array_ops import (
    subtract_sample_from_pop_stats,
    zero_out_gt_packed,
    zero_out_phase_packed,
)
from graphmana.ingest.genotype_packer import unpack_genotypes, unpack_phase


class TestZeroOutGtPacked:
    """Test zeroing out genotype slots in packed arrays."""

    def test_single_sample_zeroed(self):
        """Zero out one sample in a 4-sample packed array."""
        # 4 samples: HomRef(0), Het(1), HomAlt(2), Missing(3)
        # Byte: 0b11_10_01_00 = 0xE4
        gt_packed = bytes([0xE4])
        result = zero_out_gt_packed(gt_packed, [1])  # Zero out Het at index 1
        gt = unpack_genotypes(result, 4)
        assert gt[0] == 0  # HomRef unchanged
        assert gt[1] == 3  # Now Missing
        assert gt[2] == 2  # HomAlt unchanged
        assert gt[3] == 3  # Missing unchanged

    def test_multiple_samples_zeroed(self):
        """Zero out multiple samples."""
        gt_packed = bytes([0xE4])  # [0, 1, 2, 3]
        result = zero_out_gt_packed(gt_packed, [0, 2])
        gt = unpack_genotypes(result, 4)
        assert gt[0] == 3  # Was HomRef, now Missing
        assert gt[1] == 1  # Het unchanged
        assert gt[2] == 3  # Was HomAlt, now Missing
        assert gt[3] == 3  # Missing unchanged

    def test_already_missing_unchanged(self):
        """Zeroing an already-missing sample is a no-op."""
        gt_packed = bytes([0xFF])  # All Missing
        result = zero_out_gt_packed(gt_packed, [0, 1, 2, 3])
        assert result == gt_packed

    def test_multi_byte_array(self):
        """Zero out sample in second byte."""
        # 8 samples across 2 bytes
        gt_packed = bytes([0x00, 0x00])  # All HomRef
        result = zero_out_gt_packed(gt_packed, [5])  # Index 5 is in byte 1
        gt = unpack_genotypes(result, 8)
        assert gt[4] == 0  # Unchanged
        assert gt[5] == 3  # Now Missing
        assert gt[6] == 0  # Unchanged

    def test_empty_indices(self):
        """No indices to zero — no change."""
        gt_packed = bytes([0xE4])
        result = zero_out_gt_packed(gt_packed, [])
        assert result == gt_packed


class TestZeroOutPhasePacked:
    """Test zeroing out phase bits in packed arrays."""

    def test_single_bit_cleared(self):
        """Clear one phase bit."""
        # 8 samples, all phased: 0xFF
        phase_packed = bytes([0xFF])
        result = zero_out_phase_packed(phase_packed, [3])
        phase = unpack_phase(result, 8)
        assert phase[2] == 1  # Unchanged
        assert phase[3] == 0  # Cleared
        assert phase[4] == 1  # Unchanged

    def test_multiple_bits_cleared(self):
        """Clear multiple phase bits."""
        phase_packed = bytes([0xFF])
        result = zero_out_phase_packed(phase_packed, [0, 4, 7])
        phase = unpack_phase(result, 8)
        assert phase[0] == 0
        assert phase[1] == 1
        assert phase[4] == 0
        assert phase[7] == 0

    def test_already_zero_unchanged(self):
        """Clearing already-zero bits is a no-op."""
        phase_packed = bytes([0x00])
        result = zero_out_phase_packed(phase_packed, [0, 1, 2])
        assert result == phase_packed

    def test_empty_indices(self):
        """No indices — no change."""
        phase_packed = bytes([0xFF])
        result = zero_out_phase_packed(phase_packed, [])
        assert result == phase_packed


class TestSubtractSampleFromPopStats:
    """Test subtracting sample contributions from population stats."""

    def test_homref_removal(self):
        """Removing HomRef sample: only AN changes."""
        result = subtract_sample_from_pop_stats(
            pop_ids=["A", "B"],
            ac=[0, 1],
            an=[4, 4],
            het_count=[0, 1],
            hom_alt_count=[0, 0],
            gt_codes=np.array([0], dtype=np.uint8),  # HomRef
            pop_id="A",
        )
        assert result["ac"] == [0, 1]
        assert result["an"] == [2, 4]
        assert result["het_count"] == [0, 1]
        assert result["hom_alt_count"] == [0, 0]

    def test_het_removal(self):
        """Removing Het sample: AC, AN, het_count change."""
        result = subtract_sample_from_pop_stats(
            pop_ids=["A"],
            ac=[3],
            an=[6],
            het_count=[1],
            hom_alt_count=[1],
            gt_codes=np.array([1], dtype=np.uint8),
            pop_id="A",
        )
        assert result["ac"] == [2]
        assert result["an"] == [4]
        assert result["het_count"] == [0]
        assert result["hom_alt_count"] == [1]

    def test_homalt_removal(self):
        """Removing HomAlt sample: AC, AN, hom_alt_count change."""
        result = subtract_sample_from_pop_stats(
            pop_ids=["A"],
            ac=[4],
            an=[6],
            het_count=[0],
            hom_alt_count=[2],
            gt_codes=np.array([2], dtype=np.uint8),
            pop_id="A",
        )
        assert result["ac"] == [2]
        assert result["an"] == [4]
        assert result["hom_alt_count"] == [1]

    def test_missing_removal(self):
        """Removing Missing sample: no stat change."""
        result = subtract_sample_from_pop_stats(
            pop_ids=["A"],
            ac=[2],
            an=[4],
            het_count=[0],
            hom_alt_count=[1],
            gt_codes=np.array([3], dtype=np.uint8),
            pop_id="A",
        )
        assert result["ac"] == [2]
        assert result["an"] == [4]

    def test_multiple_samples_same_pop(self):
        """Remove multiple samples from same population."""
        result = subtract_sample_from_pop_stats(
            pop_ids=["A", "B"],
            ac=[3, 0],
            an=[8, 4],
            het_count=[1, 0],
            hom_alt_count=[1, 0],
            gt_codes=np.array([1, 2], dtype=np.uint8),  # Het + HomAlt
            pop_id="A",
        )
        assert result["ac"] == [0, 0]
        assert result["an"] == [4, 4]
        assert result["het_count"] == [0, 0]
        assert result["hom_alt_count"] == [0, 0]

    def test_population_becomes_empty(self):
        """All samples removed from population — counts go to zero."""
        result = subtract_sample_from_pop_stats(
            pop_ids=["A"],
            ac=[1],
            an=[2],
            het_count=[1],
            hom_alt_count=[0],
            gt_codes=np.array([1], dtype=np.uint8),
            pop_id="A",
        )
        assert result["ac"] == [0]
        assert result["an"] == [0]
        assert result["af"] == [0.0]

    def test_af_and_totals_recomputed(self):
        """Verify derived stats are correct after subtraction."""
        result = subtract_sample_from_pop_stats(
            pop_ids=["A", "B"],
            ac=[2, 1],
            an=[6, 4],
            het_count=[0, 1],
            hom_alt_count=[1, 0],
            gt_codes=np.array([2], dtype=np.uint8),
            pop_id="A",
        )
        # A: ac=0, an=4 → af=0.0
        assert result["af"][0] == 0.0
        assert result["af"][1] == 1 / 4
        assert result["ac_total"] == 1
        assert result["an_total"] == 8
        assert abs(result["af_total"] - 1 / 8) < 1e-9
