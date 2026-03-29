"""Tests for genotype packing, ploidy packing, and phase packing."""

import numpy as np

from graphmana.ingest.genotype_packer import (
    GT_REMAP,
    build_ploidy_packed,
    pack_phase,
    unpack_genotypes,
    unpack_phase,
    unpack_ploidy,
    vectorized_gt_pack,
)


class TestGtRemap:
    """Verify the GT_REMAP constant maps cyvcf2 codes correctly."""

    def test_remap_values(self):
        assert GT_REMAP[0] == 0  # HOM_REF → 00
        assert GT_REMAP[1] == 1  # HET → 01
        assert GT_REMAP[2] == 3  # MISSING → 11
        assert GT_REMAP[3] == 2  # HOM_ALT → 10


class TestVectorizedGtPack:
    """Test 2-bit genotype packing."""

    def test_roundtrip(self):
        """Packed bytes can be manually unpacked to recover remapped genotypes."""
        gt_types = np.array([0, 1, 3, 2, 0, 3, 1, 2, 0], dtype=np.int8)
        packed = vectorized_gt_pack(gt_types)

        remap = {0: 0, 1: 1, 2: 3, 3: 2}
        for i, gt in enumerate(gt_types):
            byte_idx = i >> 2
            bit_shift = (i & 3) << 1
            extracted = (packed[byte_idx] >> bit_shift) & 0x03
            assert extracted == remap[gt], f"sample {i}: got {extracted}, expected {remap[gt]}"

    def test_empty(self):
        """Empty input produces empty bytes."""
        packed = vectorized_gt_pack(np.array([], dtype=np.int8))
        assert packed == b""

    def test_single_sample(self):
        """Single sample packs into one byte."""
        for gt, expected_packed in [(0, 0), (1, 1), (2, 3), (3, 2)]:
            packed = vectorized_gt_pack(np.array([gt], dtype=np.int8))
            assert len(packed) == 1
            assert packed[0] == expected_packed

    def test_exact_multiple_of_4(self):
        """4 samples pack into exactly 1 byte with no padding."""
        gt = np.array([0, 1, 3, 2], dtype=np.int8)
        packed = vectorized_gt_pack(gt)
        assert len(packed) == 1
        # Remapped: 0, 1, 2, 3 → bits: 00, 01, 10, 11 → 0b11_10_01_00 = 0xE4
        assert packed[0] == 0b11_10_01_00

    def test_non_multiple_of_4(self):
        """5 samples should produce 2 bytes (padded to 8 slots, 5th=real, 6-8=0)."""
        gt = np.array([3, 3, 3, 3, 3], dtype=np.int8)
        packed = vectorized_gt_pack(gt)
        assert len(packed) == 2
        # First byte: 4 HOM_ALT → remapped 2 each → 0b10_10_10_10 = 0xAA
        assert packed[0] == 0xAA
        # Second byte: 1 HOM_ALT + 3 padding zeros → 0b00_00_00_10 = 0x02
        assert packed[1] == 0x02


class TestBuildPloidyPacked:
    """Test 1-bit ploidy packing."""

    def test_roundtrip(self):
        """Packed bits can be unpacked to recover original flags."""
        flags = np.array([True, False, True, False, False, True, False, False, True])
        packed = build_ploidy_packed(flags)

        for i, expected in enumerate(flags):
            byte_idx = i >> 3
            bit_idx = i & 7
            bit = (packed[byte_idx] >> bit_idx) & 1
            assert bit == int(expected), f"sample {i}: got {bit}, expected {int(expected)}"

    def test_all_diploid(self):
        """All-diploid (False) flags produce all-zero bytes."""
        flags = np.zeros(16, dtype=bool)
        packed = build_ploidy_packed(flags)
        assert all(b == 0 for b in packed)
        assert len(packed) == 2  # 16 samples → 2 bytes

    def test_all_haploid(self):
        """All-haploid (True) flags produce all-ones bits."""
        flags = np.ones(8, dtype=bool)
        packed = build_ploidy_packed(flags)
        assert len(packed) == 1
        assert packed[0] == 0xFF


class TestPackPhase:
    """Test phase bit packing for heterozygous sites."""

    def test_no_hets(self):
        """No het sites → all-zero phase bytes."""
        het_idx = np.array([], dtype=np.intp)
        phase = pack_phase(8, het_idx, [])
        assert phase == bytes(1)

    def test_all_hets_alt_on_second(self):
        """All samples het with ALT on second haplotype → all bits set."""
        n = 8
        het_idx = np.arange(n)
        genotypes = [[0, 1, True]] * n  # a1=1 → phase bit set
        phase = pack_phase(n, het_idx, genotypes)
        assert phase[0] == 0xFF

    def test_mixed(self):
        """Mix of het/non-het with varying phase."""
        n = 4
        het_idx = np.array([0, 2])
        genotypes = [
            [0, 1, True],  # het, ALT on second → bit 0 set
            [0, 0, True],  # hom_ref, not in het_idx
            [1, 0, True],  # het, ALT on first → bit 2 NOT set
            [1, 1, True],  # hom_alt, not in het_idx
        ]
        phase = pack_phase(n, het_idx, genotypes)
        # Only bit 0 should be set (sample 0: a1==1)
        # Sample 2: a1==0, so bit 2 not set
        assert phase[0] == 0b00000001


class TestUnpackGenotypes:
    """Test genotype unpacking (2-bit → per-sample codes)."""

    def test_roundtrip_all_genotypes(self):
        """Pack cyvcf2 codes → unpack → verify packed encoding."""
        # cyvcf2 codes: 0=HomRef, 1=Het, 3=HomAlt, 2=Missing
        gt_types = np.array([0, 1, 3, 2, 0], dtype=np.int8)
        packed = vectorized_gt_pack(gt_types)
        unpacked = unpack_genotypes(packed, 5)
        # Expected packed encoding: 0=HomRef, 1=Het, 2=HomAlt, 3=Missing
        expected = np.array([0, 1, 2, 3, 0], dtype=np.int8)
        np.testing.assert_array_equal(unpacked, expected)

    def test_single_sample(self):
        """Single sample roundtrips correctly."""
        for cyvcf2_gt, packed_gt in [(0, 0), (1, 1), (3, 2), (2, 3)]:
            packed = vectorized_gt_pack(np.array([cyvcf2_gt], dtype=np.int8))
            unpacked = unpack_genotypes(packed, 1)
            assert unpacked[0] == packed_gt

    def test_exact_multiple_of_4(self):
        """4 samples unpack exactly."""
        gt_types = np.array([0, 1, 3, 2], dtype=np.int8)
        packed = vectorized_gt_pack(gt_types)
        unpacked = unpack_genotypes(packed, 4)
        expected = np.array([0, 1, 2, 3], dtype=np.int8)
        np.testing.assert_array_equal(unpacked, expected)

    def test_boundary_values(self):
        """All-same genotype codes unpack correctly."""
        for cyvcf2_gt, packed_gt in [(0, 0), (1, 1), (3, 2), (2, 3)]:
            gt = np.full(7, cyvcf2_gt, dtype=np.int8)
            packed = vectorized_gt_pack(gt)
            unpacked = unpack_genotypes(packed, 7)
            expected = np.full(7, packed_gt, dtype=np.int8)
            np.testing.assert_array_equal(unpacked, expected)

    def test_large_roundtrip(self):
        """100 random samples roundtrip pack/unpack correctly."""
        rng = np.random.default_rng(42)
        cyvcf2_codes = rng.integers(0, 4, size=100, dtype=np.int8)
        packed = vectorized_gt_pack(cyvcf2_codes)
        unpacked = unpack_genotypes(packed, 100)
        expected = GT_REMAP[cyvcf2_codes].astype(np.int8)
        np.testing.assert_array_equal(unpacked, expected)


class TestUnpackPhase:
    """Test phase bit unpacking."""

    def test_roundtrip(self):
        """Pack phase → unpack → verify."""
        n = 8
        het_idx = np.arange(n)
        genotypes = [[0, 1, True]] * n
        packed = pack_phase(n, het_idx, genotypes)
        unpacked = unpack_phase(packed, n)
        np.testing.assert_array_equal(unpacked, np.ones(n, dtype=np.uint8))

    def test_mixed_phase(self):
        """Mixed phase bits roundtrip."""
        n = 4
        het_idx = np.array([0, 2])
        genotypes = [
            [0, 1, True],  # bit 0 set
            [0, 0, True],  # not het
            [1, 0, True],  # bit 2 NOT set (ALT on first)
            [1, 1, True],  # not het
        ]
        packed = pack_phase(n, het_idx, genotypes)
        unpacked = unpack_phase(packed, n)
        expected = np.array([1, 0, 0, 0], dtype=np.uint8)
        np.testing.assert_array_equal(unpacked, expected)

    def test_all_zero(self):
        """No phase bits set → all zeros."""
        packed = bytes(2)  # 16 zero bits
        unpacked = unpack_phase(packed, 10)
        np.testing.assert_array_equal(unpacked, np.zeros(10, dtype=np.uint8))


class TestUnpackPloidy:
    """Test ploidy flag unpacking."""

    def test_roundtrip(self):
        """Pack ploidy → unpack → verify."""
        flags = np.array([True, False, True, False, False, True, False, False, True])
        packed = build_ploidy_packed(flags)
        unpacked = unpack_ploidy(packed, 9)
        np.testing.assert_array_equal(unpacked, flags.astype(np.uint8))

    def test_none_input(self):
        """None ploidy_packed → all diploid (zeros)."""
        unpacked = unpack_ploidy(None, 10)
        np.testing.assert_array_equal(unpacked, np.zeros(10, dtype=np.uint8))

    def test_all_haploid(self):
        """All haploid flags roundtrip."""
        flags = np.ones(8, dtype=bool)
        packed = build_ploidy_packed(flags)
        unpacked = unpack_ploidy(packed, 8)
        np.testing.assert_array_equal(unpacked, np.ones(8, dtype=np.uint8))
