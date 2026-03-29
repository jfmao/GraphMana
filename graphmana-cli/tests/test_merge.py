"""Tests for database merging feature.

Tests the array concatenation functions (bit-level correctness),
merge validation logic, and DatabaseMerger interface.
"""

import numpy as np
import pytest

from graphmana.ingest.array_ops import (
    _pack_bits,
    _pack_codes_direct,
    concatenate_gt_packed,
    concatenate_phase_packed,
    concatenate_ploidy_packed,
)
from graphmana.ingest.genotype_packer import (
    build_ploidy_packed,
    unpack_genotypes,
    unpack_phase,
    unpack_ploidy,
    vectorized_gt_pack,
)
from graphmana.merge.merger import DatabaseMerger, MergeValidationError

# ---------------------------------------------------------------------------
# Test concatenate_gt_packed
# ---------------------------------------------------------------------------


class TestConcatenateGtPacked:
    """Test concatenating two already-packed genotype arrays."""

    def test_basic_concatenation(self):
        """Concatenate two 4-sample packed arrays."""
        # Target: HomRef, Het, HomAlt, Missing (packed codes: 0, 1, 2, 3)
        target_codes = np.array([0, 1, 2, 3], dtype=np.uint8)
        target_packed = _pack_codes_direct(target_codes)

        # Source: Het, HomRef (packed codes: 1, 0)
        source_codes = np.array([1, 0], dtype=np.uint8)
        source_packed = _pack_codes_direct(source_codes)

        result = concatenate_gt_packed(target_packed, 4, source_packed, 2)
        unpacked = unpack_genotypes(result, 6)

        expected = np.array([0, 1, 2, 3, 1, 0], dtype=np.int8)
        np.testing.assert_array_equal(unpacked, expected)

    def test_non_aligned_boundaries(self):
        """Concatenate when neither array is 4-sample aligned."""
        target_codes = np.array([0, 1, 2], dtype=np.uint8)
        target_packed = _pack_codes_direct(target_codes)

        source_codes = np.array([3, 2, 1, 0, 1], dtype=np.uint8)
        source_packed = _pack_codes_direct(source_codes)

        result = concatenate_gt_packed(target_packed, 3, source_packed, 5)
        unpacked = unpack_genotypes(result, 8)

        expected = np.array([0, 1, 2, 3, 2, 1, 0, 1], dtype=np.int8)
        np.testing.assert_array_equal(unpacked, expected)

    def test_empty_target(self):
        """Concatenating onto empty target returns source as-is."""
        source_codes = np.array([1, 2, 3], dtype=np.uint8)
        source_packed = _pack_codes_direct(source_codes)

        result = concatenate_gt_packed(b"", 0, source_packed, 3)
        assert result == source_packed

    def test_empty_source(self):
        """Concatenating empty source returns target as-is."""
        target_codes = np.array([0, 1], dtype=np.uint8)
        target_packed = _pack_codes_direct(target_codes)

        result = concatenate_gt_packed(target_packed, 2, b"", 0)
        assert result == target_packed

    def test_large_roundtrip(self):
        """100 target + 50 source samples roundtrip correctly."""
        rng = np.random.default_rng(42)
        target_codes = rng.integers(0, 4, size=100, dtype=np.uint8)
        source_codes = rng.integers(0, 4, size=50, dtype=np.uint8)

        target_packed = _pack_codes_direct(target_codes)
        source_packed = _pack_codes_direct(source_codes)

        result = concatenate_gt_packed(target_packed, 100, source_packed, 50)
        unpacked = unpack_genotypes(result, 150)

        expected = np.concatenate([target_codes, source_codes]).astype(np.int8)
        np.testing.assert_array_equal(unpacked, expected)

    def test_all_missing(self):
        """Both arrays are all-missing (code 3)."""
        target_codes = np.full(4, 3, dtype=np.uint8)
        source_codes = np.full(3, 3, dtype=np.uint8)
        target_packed = _pack_codes_direct(target_codes)
        source_packed = _pack_codes_direct(source_codes)

        result = concatenate_gt_packed(target_packed, 4, source_packed, 3)
        unpacked = unpack_genotypes(result, 7)
        expected = np.full(7, 3, dtype=np.int8)
        np.testing.assert_array_equal(unpacked, expected)

    def test_single_sample_each(self):
        """Single sample in each array."""
        target_packed = _pack_codes_direct(np.array([2], dtype=np.uint8))
        source_packed = _pack_codes_direct(np.array([1], dtype=np.uint8))

        result = concatenate_gt_packed(target_packed, 1, source_packed, 1)
        unpacked = unpack_genotypes(result, 2)
        expected = np.array([2, 1], dtype=np.int8)
        np.testing.assert_array_equal(unpacked, expected)

    def test_no_remap_applied(self):
        """Values are NOT remapped — packed codes go in, packed codes come out."""
        # Pack code 2 (HomAlt) and code 3 (Missing) — these would be
        # swapped if GT_REMAP were applied (cyvcf2 2=Missing, 3=HomAlt)
        target_codes = np.array([2, 3], dtype=np.uint8)
        target_packed = _pack_codes_direct(target_codes)

        source_codes = np.array([0, 1], dtype=np.uint8)
        source_packed = _pack_codes_direct(source_codes)

        result = concatenate_gt_packed(target_packed, 2, source_packed, 2)
        unpacked = unpack_genotypes(result, 4)
        # Should be exactly [2, 3, 0, 1] — no remapping
        expected = np.array([2, 3, 0, 1], dtype=np.int8)
        np.testing.assert_array_equal(unpacked, expected)


# ---------------------------------------------------------------------------
# Test concatenate_phase_packed
# ---------------------------------------------------------------------------


class TestConcatenatePhasePacked:
    """Test concatenating two packed phase arrays."""

    def test_basic_concatenation(self):
        """Concatenate 8 target + 4 source phase bits."""
        target_bits = np.array([1, 0, 1, 0, 0, 1, 0, 0], dtype=np.uint8)
        target_packed = _pack_bits(target_bits)

        source_bits = np.array([1, 1, 0, 1], dtype=np.uint8)
        source_packed = _pack_bits(source_bits)

        result = concatenate_phase_packed(target_packed, 8, source_packed, 4)
        unpacked = unpack_phase(result, 12)

        expected = np.concatenate([target_bits, source_bits])
        np.testing.assert_array_equal(unpacked, expected)

    def test_non_aligned(self):
        """Non-byte-aligned phase arrays."""
        target_bits = np.array([1, 0, 1], dtype=np.uint8)
        target_packed = _pack_bits(target_bits)

        source_bits = np.array([0, 1, 1, 0, 0], dtype=np.uint8)
        source_packed = _pack_bits(source_bits)

        result = concatenate_phase_packed(target_packed, 3, source_packed, 5)
        unpacked = unpack_phase(result, 8)

        expected = np.array([1, 0, 1, 0, 1, 1, 0, 0], dtype=np.uint8)
        np.testing.assert_array_equal(unpacked, expected)

    def test_empty_target(self):
        """Empty target returns source."""
        source_bits = np.array([1, 0, 1], dtype=np.uint8)
        source_packed = _pack_bits(source_bits)

        result = concatenate_phase_packed(b"", 0, source_packed, 3)
        assert result == source_packed

    def test_empty_source(self):
        """Empty source returns target."""
        target_bits = np.array([1, 0], dtype=np.uint8)
        target_packed = _pack_bits(target_bits)

        result = concatenate_phase_packed(target_packed, 2, b"", 0)
        assert result == target_packed

    def test_large_roundtrip(self):
        """100 target + 50 source roundtrip correctly."""
        rng = np.random.default_rng(123)
        target_bits = rng.integers(0, 2, size=100, dtype=np.uint8)
        source_bits = rng.integers(0, 2, size=50, dtype=np.uint8)

        target_packed = _pack_bits(target_bits)
        source_packed = _pack_bits(source_bits)

        result = concatenate_phase_packed(target_packed, 100, source_packed, 50)
        unpacked = unpack_phase(result, 150)

        expected = np.concatenate([target_bits, source_bits])
        np.testing.assert_array_equal(unpacked, expected)


# ---------------------------------------------------------------------------
# Test concatenate_ploidy_packed
# ---------------------------------------------------------------------------


class TestConcatenatePloidyPacked:
    """Test concatenating two packed ploidy arrays."""

    def test_none_plus_none(self):
        """Both all-diploid (None) returns None."""
        result = concatenate_ploidy_packed(None, 4, None, 3)
        assert result is None

    def test_none_plus_haploid(self):
        """All-diploid target + some haploid source."""
        source_flags = np.array([True, False, True], dtype=bool)
        source_packed = build_ploidy_packed(source_flags)

        result = concatenate_ploidy_packed(None, 4, source_packed, 3)
        assert result is not None
        unpacked = unpack_ploidy(result, 7)
        expected = np.array([0, 0, 0, 0, 1, 0, 1], dtype=np.uint8)
        np.testing.assert_array_equal(unpacked, expected)

    def test_haploid_plus_none(self):
        """Some haploid target + all-diploid source."""
        target_flags = np.array([True, False], dtype=bool)
        target_packed = build_ploidy_packed(target_flags)

        result = concatenate_ploidy_packed(target_packed, 2, None, 3)
        assert result is not None
        unpacked = unpack_ploidy(result, 5)
        expected = np.array([1, 0, 0, 0, 0], dtype=np.uint8)
        np.testing.assert_array_equal(unpacked, expected)

    def test_both_haploid(self):
        """Both have haploid samples."""
        target_flags = np.array([False, True], dtype=bool)
        target_packed = build_ploidy_packed(target_flags)

        source_flags = np.array([True, False, True], dtype=bool)
        source_packed = build_ploidy_packed(source_flags)

        result = concatenate_ploidy_packed(target_packed, 2, source_packed, 3)
        assert result is not None
        unpacked = unpack_ploidy(result, 5)
        expected = np.array([0, 1, 1, 0, 1], dtype=np.uint8)
        np.testing.assert_array_equal(unpacked, expected)


# ---------------------------------------------------------------------------
# Test _pack_codes_direct (no remap)
# ---------------------------------------------------------------------------


class TestPackCodesDirectNoRemap:
    """Verify _pack_codes_direct packs without GT_REMAP."""

    def test_identity_roundtrip(self):
        """Pack and unpack returns original codes."""
        codes = np.array([0, 1, 2, 3, 0, 1, 2, 3], dtype=np.uint8)
        packed = _pack_codes_direct(codes)
        unpacked = unpack_genotypes(packed, 8)
        np.testing.assert_array_equal(unpacked, codes.astype(np.int8))

    def test_does_not_remap(self):
        """Contrast with vectorized_gt_pack which does remap."""
        # vectorized_gt_pack remaps cyvcf2 code 3 -> packed code 2 (HomAlt)
        # _pack_codes_direct should keep code 3 as code 3 (Missing)
        codes = np.array([3], dtype=np.uint8)
        packed = _pack_codes_direct(codes)
        unpacked = unpack_genotypes(packed, 1)
        assert unpacked[0] == 3  # Missing, not HomAlt

        # Contrast: vectorized_gt_pack treats input as cyvcf2 code 3 = HomAlt
        cyvcf2_packed = vectorized_gt_pack(np.array([3], dtype=np.int8))
        cyvcf2_unpacked = unpack_genotypes(cyvcf2_packed, 1)
        assert cyvcf2_unpacked[0] == 2  # HomAlt in packed encoding


# ---------------------------------------------------------------------------
# Test MergeValidationError
# ---------------------------------------------------------------------------


class TestMergeValidation:
    """Test validation logic without live Neo4j connections."""

    def test_reference_genome_mismatch(self):
        """MergeValidationError raised on reference genome mismatch."""

        class MockConn:
            def execute_read(self, query, params=None):
                return MockResult([])

        class MockResult:
            def __init__(self, records):
                self._records = records

            def single(self):
                return self._records[0] if self._records else None

            def __iter__(self):
                return iter(self._records)

        class SourceConn(MockConn):
            def execute_read(self, query, params=None):
                if "SchemaMetadata" in query:
                    return MockResult(
                        [
                            {
                                "m": {
                                    "reference_genome": "GRCh37",
                                    "chr_naming_style": "ucsc",
                                    "schema_version": "1",
                                }
                            }
                        ]
                    )
                return MockResult([])

        class TargetConn(MockConn):
            def execute_read(self, query, params=None):
                if "SchemaMetadata" in query:
                    return MockResult(
                        [
                            {
                                "m": {
                                    "reference_genome": "GRCh38",
                                    "chr_naming_style": "ucsc",
                                    "schema_version": "1",
                                }
                            }
                        ]
                    )
                return MockResult([])

        merger = DatabaseMerger(
            source_conn=SourceConn(),
            target_conn=TargetConn(),
        )
        with pytest.raises(MergeValidationError, match="Reference genome mismatch"):
            merger._validate()

    def test_chr_style_mismatch(self):
        """MergeValidationError raised on chr naming style mismatch."""

        class MockResult:
            def __init__(self, records):
                self._records = records

            def single(self):
                return self._records[0] if self._records else None

            def __iter__(self):
                return iter(self._records)

        class SourceConn:
            def execute_read(self, query, params=None):
                if "SchemaMetadata" in query:
                    return MockResult(
                        [
                            {
                                "m": {
                                    "reference_genome": "GRCh38",
                                    "chr_naming_style": "ucsc",
                                    "schema_version": "1",
                                }
                            }
                        ]
                    )
                return MockResult([])

        class TargetConn:
            def execute_read(self, query, params=None):
                if "SchemaMetadata" in query:
                    return MockResult(
                        [
                            {
                                "m": {
                                    "reference_genome": "GRCh38",
                                    "chr_naming_style": "ensembl",
                                    "schema_version": "1",
                                }
                            }
                        ]
                    )
                return MockResult([])

        merger = DatabaseMerger(
            source_conn=SourceConn(),
            target_conn=TargetConn(),
        )
        with pytest.raises(MergeValidationError, match="naming style mismatch"):
            merger._validate()

    def test_schema_version_mismatch(self):
        """MergeValidationError raised on schema version mismatch."""

        class MockResult:
            def __init__(self, records):
                self._records = records

            def single(self):
                return self._records[0] if self._records else None

            def __iter__(self):
                return iter(self._records)

        class SourceConn:
            def execute_read(self, query, params=None):
                if "SchemaMetadata" in query:
                    return MockResult(
                        [
                            {
                                "m": {
                                    "reference_genome": "GRCh38",
                                    "chr_naming_style": "ucsc",
                                    "schema_version": "2",
                                }
                            }
                        ]
                    )
                return MockResult([])

        class TargetConn:
            def execute_read(self, query, params=None):
                if "SchemaMetadata" in query:
                    return MockResult(
                        [
                            {
                                "m": {
                                    "reference_genome": "GRCh38",
                                    "chr_naming_style": "ucsc",
                                    "schema_version": "1",
                                }
                            }
                        ]
                    )
                return MockResult([])

        merger = DatabaseMerger(
            source_conn=SourceConn(),
            target_conn=TargetConn(),
        )
        with pytest.raises(MergeValidationError, match="Schema version mismatch"):
            merger._validate()

    def test_no_schema_metadata(self):
        """MergeValidationError raised when no SchemaMetadata node."""

        class MockResult:
            def __init__(self, records):
                self._records = records

            def single(self):
                return None

            def __iter__(self):
                return iter(self._records)

        class EmptyConn:
            def execute_read(self, query, params=None):
                return MockResult([])

        merger = DatabaseMerger(
            source_conn=EmptyConn(),
            target_conn=EmptyConn(),
        )
        with pytest.raises(MergeValidationError, match="No SchemaMetadata"):
            merger._validate()


# ---------------------------------------------------------------------------
# Test duplicate sample handling
# ---------------------------------------------------------------------------


class TestDuplicateSamples:
    """Test duplicate sample detection and handling."""

    def _make_mock_connections(self, overlap_ids):
        """Create mock connections with overlapping sample IDs."""

        class MockResult:
            def __init__(self, records):
                self._records = records

            def single(self):
                return self._records[0] if self._records else None

            def __iter__(self):
                return iter(self._records)

            def __len__(self):
                return len(self._records)

        all_source_ids = ["S1", "S2", "S3"]
        all_target_ids = ["T1", "T2"] + list(overlap_ids)

        class SourceConn:
            def execute_read(self, query, params=None):
                if "SchemaMetadata" in query:
                    return MockResult(
                        [
                            {
                                "m": {
                                    "reference_genome": "GRCh38",
                                    "chr_naming_style": "ucsc",
                                    "schema_version": "1",
                                }
                            }
                        ]
                    )
                if "collect(s.sampleId)" in query:
                    return MockResult([{"ids": all_source_ids}])
                if "collect(p.populationId)" in query:
                    return MockResult([{"ids": ["POP1"]}])
                if "Sample" in query and "IN_POPULATION" in query:
                    return MockResult(
                        [
                            {
                                "sampleId": sid,
                                "population": "POP1",
                                "packed_index": i,
                                "sex": 0,
                                "source_dataset": "ds",
                                "source_file": "f.vcf",
                            }
                            for i, sid in enumerate(all_source_ids)
                        ]
                    )
                if "Population" in query and "n_samples" in query:
                    return MockResult(
                        [
                            {
                                "populationId": "POP1",
                                "name": "POP1",
                                "n_samples": 3,
                                "a_n": 1.0,
                                "a_n2": 1.0,
                            }
                        ]
                    )
                if "AnnotationVersion" in query:
                    return MockResult([{"c": 0}])
                return MockResult([])

        class TargetConn:
            def execute_read(self, query, params=None):
                if "SchemaMetadata" in query:
                    return MockResult(
                        [
                            {
                                "m": {
                                    "reference_genome": "GRCh38",
                                    "chr_naming_style": "ucsc",
                                    "schema_version": "1",
                                }
                            }
                        ]
                    )
                if "collect(s.sampleId)" in query:
                    return MockResult([{"ids": all_target_ids}])
                if "collect(p.populationId)" in query:
                    return MockResult([{"ids": ["POP1"]}])
                return MockResult([])

        return SourceConn(), TargetConn()

    def test_duplicate_error(self):
        """Raises MergeValidationError when duplicates exist and mode=error."""
        src, tgt = self._make_mock_connections(["S1"])
        merger = DatabaseMerger(
            source_conn=src,
            target_conn=tgt,
            on_duplicate_sample="error",
        )
        with pytest.raises(MergeValidationError, match="Duplicate sample IDs"):
            merger._validate()

    def test_duplicate_skip(self):
        """Skips duplicates without error when mode=skip."""
        src, tgt = self._make_mock_connections(["S1"])
        merger = DatabaseMerger(
            source_conn=src,
            target_conn=tgt,
            on_duplicate_sample="skip",
        )
        # Should not raise
        merger._validate()
        assert "S1" in merger._skipped_sample_ids
        assert len(merger._skipped_sample_ids) == 1


# ---------------------------------------------------------------------------
# Test DatabaseMerger interface
# ---------------------------------------------------------------------------


class TestMergerInterface:
    """Test that DatabaseMerger has the expected public interface."""

    def test_has_run_method(self):
        assert hasattr(DatabaseMerger, "run")

    def test_has_validate_method(self):
        assert hasattr(DatabaseMerger, "_validate")

    def test_has_merge_samples_method(self):
        assert hasattr(DatabaseMerger, "_merge_samples")

    def test_has_merge_variants_method(self):
        assert hasattr(DatabaseMerger, "_merge_variants")

    def test_has_update_metadata_method(self):
        assert hasattr(DatabaseMerger, "_update_metadata")

    def test_has_extend_batch_method(self):
        assert hasattr(DatabaseMerger, "_extend_batch")

    def test_has_homref_extend_batch_method(self):
        assert hasattr(DatabaseMerger, "_homref_extend_batch")

    def test_has_create_batch_method(self):
        assert hasattr(DatabaseMerger, "_create_batch")


# ---------------------------------------------------------------------------
# Test dry run
# ---------------------------------------------------------------------------


class TestDryRun:
    """Test dry run validation-only mode."""

    def test_dry_run_returns_zeros(self):
        """Dry run after successful validation returns zero-count summary."""

        class MockResult:
            def __init__(self, records):
                self._records = records

            def single(self):
                return self._records[0] if self._records else None

            def __iter__(self):
                return iter(self._records)

        class SourceConn:
            def execute_read(self, query, params=None):
                if "SchemaMetadata" in query:
                    return MockResult(
                        [
                            {
                                "m": {
                                    "reference_genome": "GRCh38",
                                    "chr_naming_style": "ucsc",
                                    "schema_version": "1",
                                }
                            }
                        ]
                    )
                if "collect(s.sampleId)" in query:
                    return MockResult([{"ids": ["S1", "S2"]}])
                if "collect(p.populationId)" in query:
                    return MockResult([{"ids": ["POP1"]}])
                if "Sample" in query and "IN_POPULATION" in query:
                    return MockResult(
                        [
                            {
                                "sampleId": "S1",
                                "population": "POP1",
                                "packed_index": 0,
                                "sex": 0,
                                "source_dataset": "ds",
                                "source_file": "f.vcf",
                            },
                            {
                                "sampleId": "S2",
                                "population": "POP1",
                                "packed_index": 1,
                                "sex": 0,
                                "source_dataset": "ds",
                                "source_file": "f.vcf",
                            },
                        ]
                    )
                if "Population" in query and "n_samples" in query:
                    return MockResult(
                        [
                            {
                                "populationId": "POP1",
                                "name": "POP1",
                                "n_samples": 2,
                                "a_n": 1.0,
                                "a_n2": 1.0,
                            }
                        ]
                    )
                if "AnnotationVersion" in query:
                    return MockResult([{"c": 0}])
                return MockResult([])

        class TargetConn:
            def execute_read(self, query, params=None):
                if "SchemaMetadata" in query:
                    return MockResult(
                        [
                            {
                                "m": {
                                    "reference_genome": "GRCh38",
                                    "chr_naming_style": "ucsc",
                                    "schema_version": "1",
                                }
                            }
                        ]
                    )
                if "collect(s.sampleId)" in query:
                    return MockResult([{"ids": ["T1", "T2"]}])
                if "collect(p.populationId)" in query:
                    return MockResult([{"ids": ["POP1"]}])
                return MockResult([])

        merger = DatabaseMerger(
            source_conn=SourceConn(),
            target_conn=TargetConn(),
            dry_run=True,
        )
        result = merger.run()

        assert result["dry_run"] is True
        assert result["n_variants_extended"] == 0
        assert result["n_variants_created"] == 0
        assert result["n_samples_merged"] == 0
        assert result["n_populations_created"] == 0
