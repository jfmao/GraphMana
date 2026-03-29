"""Tests for phase and ploidy unpacking functions."""

import numpy as np
import pytest

from graphmana_py._unpack import unpack_genotypes, unpack_phase, unpack_ploidy


class TestUnpackPhase:
    """Test phase bit unpacking."""

    def test_all_zeros(self):
        """All unphased (bit=0)."""
        packed = bytes([0x00])  # 8 zeros
        result = unpack_phase(packed, 8)
        assert len(result) == 8
        np.testing.assert_array_equal(result, [0, 0, 0, 0, 0, 0, 0, 0])

    def test_all_ones(self):
        """All phased ALT-first (bit=1)."""
        packed = bytes([0xFF])  # 8 ones
        result = unpack_phase(packed, 8)
        assert len(result) == 8
        np.testing.assert_array_equal(result, [1, 1, 1, 1, 1, 1, 1, 1])

    def test_alternating(self):
        """Alternating bits: 0b10101010 = 0xAA."""
        packed = bytes([0xAA])  # 10101010 in binary
        result = unpack_phase(packed, 8)
        # LSB-first: bit0=0, bit1=1, bit2=0, bit3=1, ...
        np.testing.assert_array_equal(result, [0, 1, 0, 1, 0, 1, 0, 1])

    def test_subset_of_byte(self):
        """Unpack fewer samples than a full byte."""
        packed = bytes([0x05])  # 00000101 → LSB-first: 1, 0, 1, 0, 0, 0, 0, 0
        result = unpack_phase(packed, 3)
        assert len(result) == 3
        np.testing.assert_array_equal(result, [1, 0, 1])

    def test_multi_byte(self):
        """Two bytes → 16 samples."""
        packed = bytes([0x00, 0xFF])
        result = unpack_phase(packed, 16)
        assert len(result) == 16
        # First 8 = 0, next 8 = 1
        np.testing.assert_array_equal(result[:8], [0] * 8)
        np.testing.assert_array_equal(result[8:], [1] * 8)


class TestUnpackPloidy:
    """Test ploidy unpacking."""

    def test_none_means_all_diploid(self):
        """None ploidy_packed → all diploid (2)."""
        result = unpack_ploidy(None, 10)
        assert len(result) == 10
        np.testing.assert_array_equal(result, [2] * 10)

    def test_all_diploid(self):
        """All bits 0 → all diploid (2)."""
        packed = bytes([0x00])
        result = unpack_ploidy(packed, 8)
        np.testing.assert_array_equal(result, [2] * 8)

    def test_all_haploid(self):
        """All bits 1 → all haploid (1)."""
        packed = bytes([0xFF])
        result = unpack_ploidy(packed, 8)
        np.testing.assert_array_equal(result, [1] * 8)

    def test_mixed_ploidy(self):
        """Some haploid, some diploid."""
        # 0x03 = 00000011 → LSB-first: bit0=1, bit1=1, rest=0
        packed = bytes([0x03])
        result = unpack_ploidy(packed, 5)
        assert len(result) == 5
        # bit0=1 → haploid(1), bit1=1 → haploid(1), bits 2-4=0 → diploid(2)
        np.testing.assert_array_equal(result, [1, 1, 2, 2, 2])

    def test_dtype(self):
        """Result should be int8."""
        result = unpack_ploidy(None, 5)
        assert result.dtype == np.int8


class TestUnpackGenotypesCompat:
    """Ensure existing unpack_genotypes still works."""

    def test_basic_unpack(self):
        # 4 samples in 1 byte: 0b11100100 = 0xE4
        # LSB-first: 00=HomRef, 01=Het, 10=HomAlt, 11=Missing
        packed = bytes([0xE4])
        result = unpack_genotypes(packed, 4)
        np.testing.assert_array_equal(result, [0, 1, 2, 3])
