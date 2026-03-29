"""Tests for EIGENSTRAT format exporter."""

import numpy as np

from graphmana.export.eigenstrat_export import (
    EIGENSTRAT_REMAP,
    EIGENSTRATExporter,
    format_ind_line,
    format_snp_line,
    gt_to_eigenstrat,
)


class TestEigenstratRemap:
    """Test genotype code remapping."""

    def test_remap_values(self):
        """Verify remap table: HomRef->2, Het->1, HomAlt->0, Missing->9."""
        assert EIGENSTRAT_REMAP[0] == 2  # HomRef -> 2
        assert EIGENSTRAT_REMAP[1] == 1  # Het -> 1
        assert EIGENSTRAT_REMAP[2] == 0  # HomAlt -> 0
        assert EIGENSTRAT_REMAP[3] == 9  # Missing -> 9

    def test_gt_to_eigenstrat_all_types(self):
        """Convert all 4 genotype types."""
        gt = np.array([0, 1, 2, 3], dtype=np.int8)
        result = gt_to_eigenstrat(gt)
        assert result == "2109"

    def test_gt_to_eigenstrat_homref_only(self):
        """All HomRef samples."""
        gt = np.array([0, 0, 0], dtype=np.int8)
        result = gt_to_eigenstrat(gt)
        assert result == "222"

    def test_gt_to_eigenstrat_mixed(self):
        """Realistic mixed genotypes."""
        gt = np.array([0, 0, 1, 2, 0, 3, 1], dtype=np.int8)
        result = gt_to_eigenstrat(gt)
        assert result == "2210291"

    def test_gt_to_eigenstrat_single_sample(self):
        """Single sample export."""
        gt = np.array([1], dtype=np.int8)
        result = gt_to_eigenstrat(gt)
        assert result == "1"


class TestSnpLine:
    """Test .snp line formatting."""

    def test_basic_snp_line(self):
        """Standard SNP line format."""
        props = {
            "variantId": "rs123",
            "chr": "chr1",
            "pos": 12345,
            "ref": "A",
            "alt": "G",
        }
        line = format_snp_line(props)
        assert line == "rs123\tchr1\t0.0\t12345\tA\tG"

    def test_snp_line_defaults(self):
        """Missing values get defaults."""
        props = {}
        line = format_snp_line(props)
        assert line == ".\t0\t0.0\t0\tN\tN"

    def test_snp_line_numeric_chr(self):
        """Numeric chromosome."""
        props = {"variantId": "v1", "chr": "22", "pos": 100, "ref": "C", "alt": "T"}
        line = format_snp_line(props)
        fields = line.split("\t")
        assert fields[1] == "22"
        assert fields[3] == "100"


class TestIndLine:
    """Test .ind line formatting."""

    def test_male_sample(self):
        """Male sample with population."""
        sample = {"sampleId": "NA12878", "sex": "male", "population": "EUR"}
        line = format_ind_line(sample)
        assert line == "NA12878\tM\tEUR"

    def test_female_sample(self):
        """Female sample."""
        sample = {"sampleId": "NA12877", "sex": "female", "population": "EUR"}
        line = format_ind_line(sample)
        assert line == "NA12877\tF\tEUR"

    def test_unknown_sex(self):
        """Unknown sex defaults to U."""
        sample = {"sampleId": "S1", "population": "AFR"}
        line = format_ind_line(sample)
        assert line == "S1\tU\tAFR"

    def test_numeric_sex(self):
        """Numeric sex codes (1=male, 2=female)."""
        assert format_ind_line({"sampleId": "S", "sex": "1", "population": "P"}) == "S\tM\tP"
        assert format_ind_line({"sampleId": "S", "sex": "2", "population": "P"}) == "S\tF\tP"

    def test_missing_population(self):
        """Missing population defaults to Unknown."""
        sample = {"sampleId": "S1"}
        line = format_ind_line(sample)
        assert line == "S1\tU\tUnknown"


class TestEigenstratExporterClass:
    """Test EIGENSTRATExporter class properties."""

    def test_inherits_base(self):
        """EIGENSTRATExporter should inherit from BaseExporter."""
        from graphmana.export.base import BaseExporter

        assert issubclass(EIGENSTRATExporter, BaseExporter)
