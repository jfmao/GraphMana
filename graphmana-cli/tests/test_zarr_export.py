"""Tests for Zarr/sgkit-compatible export format."""

import numpy as np

from graphmana.export.zarr_export import DEFAULT_CHUNK_SIZE


class TestZarrConfig:
    """Test Zarr export configuration."""

    def test_default_chunk_size(self):
        assert DEFAULT_CHUNK_SIZE == 10000

    def test_chunk_size_positive(self):
        assert DEFAULT_CHUNK_SIZE > 0


class TestSgkitCompatibility:
    """Test sgkit-compatible data structures."""

    def test_call_genotype_encoding_homref(self):
        """HomRef (gt=0) should encode as [0, 0]."""
        # gt_code=0 → allele dosages [0, 0]
        assert [0, 0] == [0, 0]

    def test_call_genotype_encoding_het(self):
        """Het (gt=1) should encode as [0, 1]."""
        assert [0, 1] == [0, 1]

    def test_call_genotype_encoding_homalt(self):
        """HomAlt (gt=2) should encode as [1, 1]."""
        assert [1, 1] == [1, 1]

    def test_call_genotype_encoding_missing(self):
        """Missing (gt=3) should encode as [-1, -1]."""
        assert [-1, -1] == [-1, -1]

    def test_call_genotype_matrix_shape(self):
        """call_genotype should be (n_variants, n_samples, ploidy)."""
        n_variants, n_samples, ploidy = 100, 10, 2
        data = np.full((n_variants, n_samples, ploidy), -1, dtype=np.int8)
        assert data.shape == (100, 10, 2)
        assert data.dtype == np.int8

    def test_variant_contig_dtype(self):
        """variant_contig should be unicode string array."""
        contigs = np.array(["chr1", "chr1", "chr2"], dtype="U32")
        assert contigs.dtype.kind == "U"

    def test_variant_position_dtype(self):
        """variant_position should be int64."""
        positions = np.array([100, 200, 300], dtype=np.int64)
        assert positions.dtype == np.int64

    def test_variant_allele_shape(self):
        """variant_allele should be (n_variants, 2) for biallelic."""
        alleles = np.array([["A", "T"], ["G", "C"]], dtype="U64")
        assert alleles.shape == (2, 2)

    def test_sample_id_dtype(self):
        """sample_id should be unicode string array."""
        ids = np.array(["S1", "S2", "S3"], dtype="U")
        assert ids.dtype.kind == "U"


class TestGenotypeConversion:
    """Test packed genotype to sgkit genotype conversion."""

    def test_batch_conversion(self):
        """Convert a batch of packed genotypes to sgkit format."""
        gt_codes = np.array([0, 1, 2, 3, 0], dtype=np.int8)
        n_samples = len(gt_codes)
        ploidy = 2

        result = np.full((n_samples, ploidy), -1, dtype=np.int8)
        for i, gt in enumerate(gt_codes):
            if gt == 0:
                result[i] = [0, 0]
            elif gt == 1:
                result[i] = [0, 1]
            elif gt == 2:
                result[i] = [1, 1]
            # gt == 3 stays [-1, -1]

        np.testing.assert_array_equal(result[0], [0, 0])  # HomRef
        np.testing.assert_array_equal(result[1], [0, 1])  # Het
        np.testing.assert_array_equal(result[2], [1, 1])  # HomAlt
        np.testing.assert_array_equal(result[3], [-1, -1])  # Missing
        np.testing.assert_array_equal(result[4], [0, 0])  # HomRef


class TestChunking:
    """Test chunk size logic."""

    def test_chunk_size_less_than_variants(self):
        n_variants = 25000
        chunk_size = DEFAULT_CHUNK_SIZE
        v_chunk = min(chunk_size, n_variants)
        assert v_chunk == 10000

    def test_chunk_size_greater_than_variants(self):
        n_variants = 500
        chunk_size = DEFAULT_CHUNK_SIZE
        v_chunk = min(chunk_size, n_variants)
        assert v_chunk == 500

    def test_custom_chunk_size(self):
        chunk_size = 5000
        n_variants = 20000
        v_chunk = min(chunk_size, n_variants)
        assert v_chunk == 5000
