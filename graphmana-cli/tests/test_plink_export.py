"""Tests for PLINK 1.9 export (pure function tests, no Neo4j)."""

import numpy as np

from graphmana.export.plink_export import (
    BED_MAGIC,
    PLINK_REMAP,
    format_bim_line,
    format_fam_line,
    gt_to_plink_packed,
)


class TestPlinkRemap:
    """Verify PLINK_REMAP values."""

    def test_homref_to_plink(self):
        # GraphMana 0=HomRef → PLINK 11 (3) = Hom_A2(REF)
        assert PLINK_REMAP[0] == 3

    def test_het_to_plink(self):
        # GraphMana 1=Het → PLINK 10 (2) = Het
        assert PLINK_REMAP[1] == 2

    def test_homalt_to_plink(self):
        # GraphMana 2=HomAlt → PLINK 00 (0) = Hom_A1(ALT)
        assert PLINK_REMAP[2] == 0

    def test_missing_to_plink(self):
        # GraphMana 3=Missing → PLINK 01 (1) = Missing
        assert PLINK_REMAP[3] == 1


class TestGtToPlinkPacked:
    """Test genotype repacking to PLINK BED format."""

    def test_four_samples(self):
        """4 samples pack into 1 byte."""
        # HomRef, Het, HomAlt, Missing → PLINK: 3, 2, 0, 1
        gt = np.array([0, 1, 2, 3], dtype=np.int8)
        packed = gt_to_plink_packed(gt)
        assert len(packed) == 1
        # bits: 11, 10, 00, 01 → 0b01_00_10_11 = 0x4B
        expected = 3 | (2 << 2) | (0 << 4) | (1 << 6)
        assert packed[0] == expected

    def test_single_sample(self):
        """Single sample pads to 4."""
        gt = np.array([0], dtype=np.int8)
        packed = gt_to_plink_packed(gt)
        assert len(packed) == 1
        # HomRef → PLINK 3 = 0b11, rest padded with 0
        assert packed[0] == 0x03

    def test_five_samples(self):
        """5 samples → 2 bytes."""
        gt = np.array([0, 0, 0, 0, 2], dtype=np.int8)
        packed = gt_to_plink_packed(gt)
        assert len(packed) == 2
        # First byte: 4 HomRef → PLINK 3 each → 0b11_11_11_11 = 0xFF
        assert packed[0] == 0xFF
        # Second byte: HomAlt(→0) + 3 padding(→0) → 0x00
        assert packed[1] == 0x00

    def test_all_missing(self):
        """All missing → all PLINK 01 bits."""
        gt = np.array([3, 3, 3, 3], dtype=np.int8)
        packed = gt_to_plink_packed(gt)
        assert len(packed) == 1
        # PLINK missing = 01 → 0b01_01_01_01 = 0x55
        assert packed[0] == 0x55

    def test_roundtrip_consistency(self):
        """Verify known mapping for all 4 genotype codes."""
        for gm_code, plink_code in [(0, 3), (1, 2), (2, 0), (3, 1)]:
            gt = np.array([gm_code], dtype=np.int8)
            packed = gt_to_plink_packed(gt)
            assert (packed[0] & 0x03) == plink_code


class TestBedMagic:
    """Verify PLINK BED magic bytes."""

    def test_magic_bytes(self):
        assert BED_MAGIC == bytes([0x6C, 0x1B, 0x01])

    def test_magic_length(self):
        assert len(BED_MAGIC) == 3


class TestFormatFamLine:
    """Test .fam line formatting."""

    def test_basic(self):
        sample = {"sampleId": "NA12878", "population": "EUR", "sex": "female"}
        line = format_fam_line(sample)
        assert line == "EUR\tNA12878\t0\t0\t2\t-9"

    def test_male(self):
        sample = {"sampleId": "NA12877", "population": "EUR", "sex": "male"}
        line = format_fam_line(sample)
        assert line == "EUR\tNA12877\t0\t0\t1\t-9"

    def test_unknown_sex(self):
        sample = {"sampleId": "S1", "population": "AFR", "sex": None}
        line = format_fam_line(sample)
        assert line == "AFR\tS1\t0\t0\t0\t-9"

    def test_numeric_sex(self):
        sample = {"sampleId": "S1", "population": "AFR", "sex": 1}
        line = format_fam_line(sample)
        assert "1\t-9" in line

    def test_no_population(self):
        sample = {"sampleId": "S1", "sex": None}
        line = format_fam_line(sample)
        assert line.startswith("0\tS1")


class TestFormatBimLine:
    """Test .bim line formatting."""

    def test_basic(self):
        props = {"chr": "chr1", "variantId": "chr1_100_A_T", "pos": 100, "ref": "A", "alt": "T"}
        line = format_bim_line(props)
        fields = line.split("\t")
        assert fields[0] == "chr1"
        assert fields[1] == "chr1_100_A_T"
        assert fields[2] == "0"  # cM
        assert fields[3] == "100"
        assert fields[4] == "T"  # A1 = ALT
        assert fields[5] == "A"  # A2 = REF
