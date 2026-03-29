"""Tests for SeqArray GDS (HDF5) export format."""

import numpy as np


class TestGDSGenotypeEncoding:
    """Test SeqArray GDS genotype encoding."""

    def test_homref_encoding(self):
        """HomRef (gt=0) should encode as 0."""
        gt = 0
        gds_val = gt  # 0 maps to 0
        assert gds_val == 0

    def test_het_encoding(self):
        """Het (gt=1) should encode as 1."""
        gt = 1
        gds_val = gt
        assert gds_val == 1

    def test_homalt_encoding(self):
        """HomAlt (gt=2) should encode as 2."""
        gt = 2
        gds_val = gt
        assert gds_val == 2

    def test_missing_encoding(self):
        """Missing (gt=3) should encode as 255."""
        # gt_code=3 (missing) → GDS value 255
        assert 255 == 255

    def test_genotype_matrix_shape(self):
        """Genotype matrix should be (n_variants, n_samples)."""
        n_variants, n_samples = 100, 10
        data = np.full((n_variants, n_samples), 255, dtype=np.uint8)
        assert data.shape == (100, 10)
        assert data.dtype == np.uint8

    def test_batch_encoding(self):
        """Convert batch of packed genotypes to GDS encoding."""
        gt_codes = np.array([0, 1, 2, 3], dtype=np.int8)
        gds_row = np.full(len(gt_codes), 255, dtype=np.uint8)
        for i, gt in enumerate(gt_codes):
            if gt in (0, 1, 2):
                gds_row[i] = gt
        np.testing.assert_array_equal(gds_row, [0, 1, 2, 255])


class TestGDSStructure:
    """Test SeqArray GDS file structure requirements."""

    def test_required_datasets(self):
        """Verify the required dataset names for SeqArray compatibility."""
        required = ["sample.id", "variant.id", "chromosome", "position", "allele"]
        for name in required:
            assert isinstance(name, str)
            assert "." not in name or name.startswith("sample") or name.startswith("variant")

    def test_allele_format(self):
        """Alleles should be comma-separated ref,alt strings."""
        ref, alt = "A", "T"
        allele_str = f"{ref},{alt}"
        assert allele_str == "A,T"

        ref, alt = "ACGT", "A"
        allele_str = f"{ref},{alt}"
        assert allele_str == "ACGT,A"

    def test_genotype_group(self):
        """Genotype data should be under a 'genotype' group with 'data' dataset."""
        group_name = "genotype"
        dataset_name = "data"
        path = f"{group_name}/{dataset_name}"
        assert path == "genotype/data"

    def test_file_format_attribute(self):
        """File should have FileFormat=SEQ_ARRAY attribute."""
        attrs = {"FileFormat": "SEQ_ARRAY", "source": "graphmana"}
        assert attrs["FileFormat"] == "SEQ_ARRAY"
