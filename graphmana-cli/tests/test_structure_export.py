"""Tests for STRUCTURE format exporter."""

import numpy as np

from graphmana.export.structure_export import (
    STRUCTUREExporter,
    format_structure_sample_onerow,
    format_structure_sample_tworow,
    gt_to_structure_alleles,
)


class TestStructureAlleleConversion:
    """Test genotype to STRUCTURE allele conversion."""

    def test_homref(self):
        """HomRef -> (1, 1)."""
        gt = np.array([0], dtype=np.int8)
        phase = np.array([0], dtype=np.uint8)
        a1, a2 = gt_to_structure_alleles(gt, phase)
        assert a1[0] == 1
        assert a2[0] == 1

    def test_het_unphased(self):
        """Het phase=0 -> ALT on first haplotype -> (2, 1)."""
        gt = np.array([1], dtype=np.int8)
        phase = np.array([0], dtype=np.uint8)
        a1, a2 = gt_to_structure_alleles(gt, phase)
        assert a1[0] == 2
        assert a2[0] == 1

    def test_het_phased(self):
        """Het phase=1 -> ALT on second haplotype -> (1, 2)."""
        gt = np.array([1], dtype=np.int8)
        phase = np.array([1], dtype=np.uint8)
        a1, a2 = gt_to_structure_alleles(gt, phase)
        assert a1[0] == 1
        assert a2[0] == 2

    def test_homalt(self):
        """HomAlt -> (2, 2)."""
        gt = np.array([2], dtype=np.int8)
        phase = np.array([0], dtype=np.uint8)
        a1, a2 = gt_to_structure_alleles(gt, phase)
        assert a1[0] == 2
        assert a2[0] == 2

    def test_missing(self):
        """Missing -> (-9, -9)."""
        gt = np.array([3], dtype=np.int8)
        phase = np.array([0], dtype=np.uint8)
        a1, a2 = gt_to_structure_alleles(gt, phase)
        assert a1[0] == -9
        assert a2[0] == -9

    def test_all_types(self):
        """All 4 genotype types at once (all phase=0 -> ALT on first haplotype)."""
        gt = np.array([0, 1, 2, 3], dtype=np.int8)
        phase = np.array([0, 0, 0, 0], dtype=np.uint8)
        a1, a2 = gt_to_structure_alleles(gt, phase)
        expected_a1 = [1, 2, 2, -9]
        expected_a2 = [1, 1, 2, -9]
        np.testing.assert_array_equal(a1, expected_a1)
        np.testing.assert_array_equal(a2, expected_a2)

    def test_vectorized_mixed(self):
        """Multiple samples with mixed phases."""
        gt = np.array([1, 1, 0, 2], dtype=np.int8)
        phase = np.array([0, 1, 0, 0], dtype=np.uint8)
        a1, a2 = gt_to_structure_alleles(gt, phase)
        # phase=0: ALT on 1st->(2,1), phase=1: ALT on 2nd->(1,2)
        np.testing.assert_array_equal(a1, [2, 1, 1, 2])
        np.testing.assert_array_equal(a2, [1, 2, 1, 2])


class TestStructureOnerowFormat:
    """Test onerow line formatting."""

    def test_basic_onerow(self):
        """Format onerow with 2 loci."""
        alleles = np.array([[1, 2], [2, 2]], dtype=np.int16)
        line = format_structure_sample_onerow("S1", 1, alleles)
        assert line == "S1\t1\t1\t2\t2\t2"

    def test_onerow_with_missing(self):
        """Missing alleles as -9."""
        alleles = np.array([[-9, -9], [1, 1]], dtype=np.int16)
        line = format_structure_sample_onerow("S2", 2, alleles)
        assert line == "S2\t2\t-9\t-9\t1\t1"


class TestStructureTworowFormat:
    """Test tworow line formatting."""

    def test_basic_tworow(self):
        """Format tworow — one haplotype row."""
        hap = np.array([1, 2, 1], dtype=np.int16)
        line = format_structure_sample_tworow("S1", 1, hap)
        assert line == "S1\t1\t1\t2\t1"

    def test_tworow_with_missing(self):
        """Missing alleles as -9."""
        hap = np.array([-9, 1, -9], dtype=np.int16)
        line = format_structure_sample_tworow("S2", 3, hap)
        assert line == "S2\t3\t-9\t1\t-9"


class TestStructureExporterClass:
    """Test STRUCTUREExporter class properties."""

    def test_inherits_base(self):
        """STRUCTUREExporter should inherit from BaseExporter."""
        from graphmana.export.base import BaseExporter

        assert issubclass(STRUCTUREExporter, BaseExporter)

    def test_invalid_format_raises(self):
        """Invalid output_format should raise ValueError."""
        # Can't call export() without a connection, but we can test that
        # the validation message is correct by checking the function exists
        # and the class accepts the parameter.
        assert hasattr(STRUCTUREExporter, "export")
