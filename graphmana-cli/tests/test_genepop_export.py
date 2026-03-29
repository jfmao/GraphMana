"""Tests for Genepop format exporter."""

import numpy as np

from graphmana.export.genepop_export import (
    GenepopExporter,
    format_genepop_locus_name,
    gt_to_genepop_code,
    gt_to_genepop_codes,
)


class TestGenepopCodes:
    """Test genotype to Genepop code conversion."""

    def test_homref(self):
        """HomRef -> 001001."""
        assert gt_to_genepop_code(0) == "001001"

    def test_het(self):
        """Het -> 001002."""
        assert gt_to_genepop_code(1) == "001002"

    def test_homalt(self):
        """HomAlt -> 002002."""
        assert gt_to_genepop_code(2) == "002002"

    def test_missing(self):
        """Missing -> 000000."""
        assert gt_to_genepop_code(3) == "000000"

    def test_vectorized_all_types(self):
        """All 4 types via vectorized function."""
        gt = np.array([0, 1, 2, 3], dtype=np.int8)
        codes = gt_to_genepop_codes(gt)
        assert codes == ["001001", "001002", "002002", "000000"]

    def test_vectorized_homref_only(self):
        """All HomRef."""
        gt = np.array([0, 0, 0], dtype=np.int8)
        codes = gt_to_genepop_codes(gt)
        assert codes == ["001001", "001001", "001001"]

    def test_vectorized_mixed(self):
        """Realistic mixed genotypes."""
        gt = np.array([0, 0, 1, 2, 3, 0], dtype=np.int8)
        codes = gt_to_genepop_codes(gt)
        assert codes == ["001001", "001001", "001002", "002002", "000000", "001001"]


class TestGenepopLocusName:
    """Test locus name formatting."""

    def test_with_variant_id(self):
        """Variant with ID."""
        props = {"variantId": "rs12345"}
        assert format_genepop_locus_name(props) == "rs12345"

    def test_missing_variant_id(self):
        """Missing variant ID defaults to '.'."""
        props = {}
        assert format_genepop_locus_name(props) == "."

    def test_complex_variant_id(self):
        """Complex variant ID with underscores."""
        props = {"variantId": "chr1_12345_A_T"}
        assert format_genepop_locus_name(props) == "chr1_12345_A_T"


class TestGenepopExporterClass:
    """Test GenepopExporter class properties."""

    def test_inherits_base(self):
        """GenepopExporter should inherit from BaseExporter."""
        from graphmana.export.base import BaseExporter

        assert issubclass(GenepopExporter, BaseExporter)
