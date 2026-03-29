"""Tests for Beagle format exporter."""

import numpy as np

from graphmana.export.beagle_export import (
    BeagleExporter,
    format_beagle_header,
    format_beagle_variant_line,
)


class TestBeagleHeader:
    """Test Beagle header formatting."""

    def test_basic_header(self):
        """Header with two samples — each ID appears twice."""
        samples = [{"sampleId": "S1"}, {"sampleId": "S2"}]
        header = format_beagle_header(samples)
        assert header == "marker\talleleA\talleleB\tS1\tS1\tS2\tS2"

    def test_single_sample_header(self):
        """Single sample."""
        samples = [{"sampleId": "NA12878"}]
        header = format_beagle_header(samples)
        assert header == "marker\talleleA\talleleB\tNA12878\tNA12878"

    def test_empty_header(self):
        """No samples."""
        header = format_beagle_header([])
        assert header == "marker\talleleA\talleleB"


class TestBeagleVariantLine:
    """Test Beagle variant line formatting."""

    def test_all_homref(self):
        """All samples HomRef."""
        props = {"variantId": "rs1"}
        gt = np.array([0, 0], dtype=np.int8)
        phase = np.array([0, 0], dtype=np.uint8)
        line = format_beagle_variant_line(props, gt, phase, "A", "T")
        assert line == "rs1\tA\tT\tA\tA\tA\tA"

    def test_all_homalt(self):
        """All samples HomAlt."""
        props = {"variantId": "rs2"}
        gt = np.array([2, 2], dtype=np.int8)
        phase = np.array([0, 0], dtype=np.uint8)
        line = format_beagle_variant_line(props, gt, phase, "A", "T")
        assert line == "rs2\tA\tT\tT\tT\tT\tT"

    def test_het_unphased(self):
        """Het with phase=0 -> ALT on first haplotype -> alt, ref."""
        props = {"variantId": "rs3"}
        gt = np.array([1], dtype=np.int8)
        phase = np.array([0], dtype=np.uint8)
        line = format_beagle_variant_line(props, gt, phase, "G", "C")
        assert line == "rs3\tG\tC\tC\tG"

    def test_het_phased(self):
        """Het with phase=1 -> ALT on second haplotype -> ref, alt."""
        props = {"variantId": "rs4"}
        gt = np.array([1], dtype=np.int8)
        phase = np.array([1], dtype=np.uint8)
        line = format_beagle_variant_line(props, gt, phase, "G", "C")
        assert line == "rs4\tG\tC\tG\tC"

    def test_missing(self):
        """Missing genotype -> '.', '.'."""
        props = {"variantId": "rs5"}
        gt = np.array([3], dtype=np.int8)
        phase = np.array([0], dtype=np.uint8)
        line = format_beagle_variant_line(props, gt, phase, "A", "T")
        assert line == "rs5\tA\tT\t.\t."

    def test_mixed_genotypes(self):
        """All 4 genotype types."""
        props = {"variantId": "v1"}
        gt = np.array([0, 1, 2, 3], dtype=np.int8)
        phase = np.array([0, 0, 0, 0], dtype=np.uint8)
        line = format_beagle_variant_line(props, gt, phase, "A", "T")
        fields = line.split("\t")
        assert fields[0] == "v1"
        assert fields[1] == "A"
        assert fields[2] == "T"
        # HomRef
        assert fields[3] == "A"
        assert fields[4] == "A"
        # Het (phase=0: ALT on first haplotype -> alt, ref)
        assert fields[5] == "T"
        assert fields[6] == "A"
        # HomAlt
        assert fields[7] == "T"
        assert fields[8] == "T"
        # Missing
        assert fields[9] == "."
        assert fields[10] == "."

    def test_default_variant_id(self):
        """Missing variantId defaults to '.'."""
        props = {}
        gt = np.array([0], dtype=np.int8)
        phase = np.array([0], dtype=np.uint8)
        line = format_beagle_variant_line(props, gt, phase, "A", "T")
        assert line.startswith(".\t")


class TestBeagleExporterClass:
    """Test BeagleExporter class properties."""

    def test_inherits_base(self):
        """BeagleExporter should inherit from BaseExporter."""
        from graphmana.export.base import BaseExporter

        assert issubclass(BeagleExporter, BaseExporter)
