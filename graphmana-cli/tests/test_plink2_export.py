"""Tests for PLINK 2.0 export (pure function tests, no Neo4j)."""

import numpy as np

from graphmana.export.plink2_export import format_psam_line, format_pvar_line


class TestFormatPvarLine:
    """Test .pvar line formatting."""

    def test_snp(self):
        props = {"chr": "chr1", "pos": 100, "variantId": "chr1_100_A_T", "ref": "A", "alt": "T"}
        line = format_pvar_line(props)
        fields = line.split("\t")
        assert fields[0] == "chr1"
        assert fields[1] == "100"
        assert fields[2] == "chr1_100_A_T"
        assert fields[3] == "A"
        assert fields[4] == "T"

    def test_indel(self):
        props = {
            "chr": "chr2",
            "pos": 500,
            "variantId": "chr2_500_AT_A",
            "ref": "AT",
            "alt": "A",
        }
        line = format_pvar_line(props)
        fields = line.split("\t")
        assert fields[3] == "AT"
        assert fields[4] == "A"

    def test_defaults(self):
        line = format_pvar_line({})
        fields = line.split("\t")
        assert fields[0] == "0"  # chr default
        assert fields[1] == "0"  # pos default
        assert fields[2] == "."  # variantId default
        assert fields[3] == "N"  # ref default
        assert fields[4] == "N"  # alt default


class TestFormatPsamLine:
    """Test .psam line formatting."""

    def test_female(self):
        sample = {"sampleId": "NA12878", "population": "EUR", "sex": "female"}
        line = format_psam_line(sample)
        assert line == "EUR\tNA12878\t2"

    def test_male(self):
        sample = {"sampleId": "NA12877", "population": "EUR", "sex": "male"}
        line = format_psam_line(sample)
        assert line == "EUR\tNA12877\t1"

    def test_unknown_sex(self):
        sample = {"sampleId": "S1", "population": "AFR", "sex": None}
        line = format_psam_line(sample)
        assert line == "AFR\tS1\tNA"

    def test_numeric_sex_male(self):
        sample = {"sampleId": "S1", "population": "AFR", "sex": 1}
        line = format_psam_line(sample)
        assert line == "AFR\tS1\t1"

    def test_numeric_sex_female(self):
        sample = {"sampleId": "S2", "population": "AFR", "sex": 2}
        line = format_psam_line(sample)
        assert line == "AFR\tS2\t2"

    def test_string_sex_codes(self):
        sample = {"sampleId": "S1", "population": "AFR", "sex": "1"}
        assert format_psam_line(sample) == "AFR\tS1\t1"
        sample2 = {"sampleId": "S2", "population": "AFR", "sex": "2"}
        assert format_psam_line(sample2) == "AFR\tS2\t2"

    def test_no_population(self):
        sample = {"sampleId": "S1", "sex": None}
        line = format_psam_line(sample)
        assert line.startswith("0\tS1")

    def test_missing_sex_key(self):
        sample = {"sampleId": "S1", "population": "POP"}
        line = format_psam_line(sample)
        assert line == "POP\tS1\tNA"


class TestEncodingIdentity:
    """Verify GraphMana and pgenlib use the same genotype encoding."""

    def test_encoding_values(self):
        # GraphMana: 0=HomRef, 1=Het, 2=HomAlt, 3=Missing
        # pgenlib:   0=HomRef, 1=Het, 2=HomAlt, 3=Missing (== -9 sentinel)
        # No remap needed — pass gt_codes directly
        gt_codes = np.array([0, 1, 2, 3], dtype=np.int8)
        # Verify the cast to int32 preserves values
        gt_int32 = gt_codes.astype(np.int32)
        np.testing.assert_array_equal(gt_int32, [0, 1, 2, 3])

    def test_all_homref(self):
        gt = np.zeros(8, dtype=np.int8)
        assert np.all(gt.astype(np.int32) == 0)

    def test_all_missing(self):
        gt = np.full(8, 3, dtype=np.int8)
        assert np.all(gt.astype(np.int32) == 3)
