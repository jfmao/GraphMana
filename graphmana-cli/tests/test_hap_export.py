"""Tests for Haplotype (.hap/.map) format exporter."""

import numpy as np

from graphmana.export.hap_export import (
    HAPExporter,
    format_map_line,
    gt_phase_to_haplotypes,
)


class TestMapLine:
    """Test .map line formatting."""

    def test_basic_map_line(self):
        """Standard map line format."""
        props = {"chr": "chr1", "variantId": "rs123", "pos": 12345}
        line = format_map_line(props)
        assert line == "chr1 rs123 0.0 12345"

    def test_map_line_defaults(self):
        """Missing values get defaults."""
        props = {}
        line = format_map_line(props)
        assert line == "0 . 0.0 0"

    def test_map_line_numeric_chr(self):
        """Numeric chromosome."""
        props = {"chr": "22", "variantId": "v1", "pos": 100}
        line = format_map_line(props)
        assert line == "22 v1 0.0 100"


class TestHaplotypeConversion:
    """Test genotype/phase to haplotype conversion."""

    def test_homref(self):
        """HomRef -> (0, 0)."""
        gt = np.array([0], dtype=np.int8)
        phase = np.array([0], dtype=np.uint8)
        h1, h2 = gt_phase_to_haplotypes(gt, phase)
        assert h1[0] == 0
        assert h2[0] == 0

    def test_het_unphased(self):
        """Het phase=0 -> ALT on first haplotype -> (1, 0)."""
        gt = np.array([1], dtype=np.int8)
        phase = np.array([0], dtype=np.uint8)
        h1, h2 = gt_phase_to_haplotypes(gt, phase)
        assert h1[0] == 1
        assert h2[0] == 0

    def test_het_phased(self):
        """Het phase=1 -> ALT on second haplotype -> (0, 1)."""
        gt = np.array([1], dtype=np.int8)
        phase = np.array([1], dtype=np.uint8)
        h1, h2 = gt_phase_to_haplotypes(gt, phase)
        assert h1[0] == 0
        assert h2[0] == 1

    def test_homalt(self):
        """HomAlt -> (1, 1)."""
        gt = np.array([2], dtype=np.int8)
        phase = np.array([0], dtype=np.uint8)
        h1, h2 = gt_phase_to_haplotypes(gt, phase)
        assert h1[0] == 1
        assert h2[0] == 1

    def test_missing(self):
        """Missing -> (0, 0) — treated as ref."""
        gt = np.array([3], dtype=np.int8)
        phase = np.array([0], dtype=np.uint8)
        h1, h2 = gt_phase_to_haplotypes(gt, phase)
        assert h1[0] == 0
        assert h2[0] == 0

    def test_all_types(self):
        """All 4 genotype types at once."""
        gt = np.array([0, 1, 2, 3], dtype=np.int8)
        phase = np.array([0, 0, 0, 0], dtype=np.uint8)
        h1, h2 = gt_phase_to_haplotypes(gt, phase)
        np.testing.assert_array_equal(h1, [0, 1, 1, 0])
        np.testing.assert_array_equal(h2, [0, 0, 1, 0])

    def test_mixed_phase(self):
        """Multiple het samples with different phases."""
        gt = np.array([1, 1, 1], dtype=np.int8)
        phase = np.array([0, 1, 0], dtype=np.uint8)
        h1, h2 = gt_phase_to_haplotypes(gt, phase)
        np.testing.assert_array_equal(h1, [1, 0, 1])
        np.testing.assert_array_equal(h2, [0, 1, 0])

    def test_result_dtype(self):
        """Output arrays should be uint8."""
        gt = np.array([0, 2], dtype=np.int8)
        phase = np.array([0, 0], dtype=np.uint8)
        h1, h2 = gt_phase_to_haplotypes(gt, phase)
        assert h1.dtype == np.uint8
        assert h2.dtype == np.uint8


class TestHAPExporterClass:
    """Test HAPExporter class properties."""

    def test_inherits_base(self):
        """HAPExporter should inherit from BaseExporter."""
        from graphmana.export.base import BaseExporter

        assert issubclass(HAPExporter, BaseExporter)
