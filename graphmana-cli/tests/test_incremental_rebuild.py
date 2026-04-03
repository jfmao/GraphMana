"""Tests for incremental_rebuild module helpers and core logic.

Tests the CSV-based incremental rebuild path: helper functions for
packed array serialization, HomRef fast extension, and CSV round-trips.
"""

from __future__ import annotations

import numpy as np
import pytest

from graphmana.ingest.incremental_rebuild import (
    _bytes_to_csv,
    _csv_bytes_to_python,
    _csv_int_list,
    _csv_str_list,
    _extend_homref_fast,
    _gt_packed_length,
    _neo4j_bytes,
    _phase_packed_length,
)


# ---------------------------------------------------------------------------
# Packed length calculations
# ---------------------------------------------------------------------------


class TestPackedLengths:
    """Test byte-length calculations for packed arrays."""

    def test_gt_packed_length_1_sample(self):
        assert _gt_packed_length(1) == 1  # ceil(1/4)

    def test_gt_packed_length_4_samples(self):
        """4 samples exactly fills 1 byte (2 bits each)."""
        assert _gt_packed_length(4) == 1

    def test_gt_packed_length_5_samples(self):
        """5 samples crosses byte boundary."""
        assert _gt_packed_length(5) == 2

    def test_gt_packed_length_3202_samples(self):
        """1KGP chr22 benchmark: 3202 samples."""
        assert _gt_packed_length(3202) == 801

    def test_phase_packed_length_1_sample(self):
        assert _phase_packed_length(1) == 1  # ceil(1/8)

    def test_phase_packed_length_8_samples(self):
        """8 samples exactly fills 1 byte (1 bit each)."""
        assert _phase_packed_length(8) == 1

    def test_phase_packed_length_9_samples(self):
        """9 samples crosses byte boundary."""
        assert _phase_packed_length(9) == 2

    def test_phase_packed_length_3202_samples(self):
        assert _phase_packed_length(3202) == 401

    def test_gt_length_zero_samples(self):
        assert _gt_packed_length(0) == 0

    def test_phase_length_zero_samples(self):
        assert _phase_packed_length(0) == 0


# ---------------------------------------------------------------------------
# CSV byte serialization round-trip
# ---------------------------------------------------------------------------


class TestByteSerialization:
    """Test _bytes_to_csv and _csv_bytes_to_python round-trip."""

    def test_roundtrip_simple(self):
        """Bytes survive CSV serialization → deserialization."""
        original = bytes([0, 1, 127, 128, 255])
        csv_str = _bytes_to_csv(original)
        restored = _csv_bytes_to_python(csv_str)
        assert restored == original

    def test_bytes_to_csv_signed_format(self):
        """Values 128-255 are written as signed Java bytes (-128 to -1)."""
        data = bytes([128, 255])
        csv_str = _bytes_to_csv(data)
        parts = csv_str.split(";")
        assert parts[0] == "-128"  # 128 → -128 in signed Java
        assert parts[1] == "-1"    # 255 → -1 in signed Java

    def test_bytes_to_csv_unsigned_format(self):
        """Values 0-127 are written as-is."""
        data = bytes([0, 1, 42, 127])
        csv_str = _bytes_to_csv(data)
        parts = csv_str.split(";")
        assert parts == ["0", "1", "42", "127"]

    def test_empty_bytes(self):
        assert _bytes_to_csv(b"") == ""
        assert _bytes_to_csv(None) == ""

    def test_csv_bytes_to_python_empty(self):
        assert _csv_bytes_to_python("") == b""

    def test_roundtrip_gt_packed(self):
        """Round-trip a realistic gt_packed array (4 samples)."""
        from graphmana.ingest.genotype_packer import vectorized_gt_pack, unpack_genotypes

        # Pack 4 samples: HomRef, Het, HomAlt, Missing
        cyvcf2_codes = np.array([0, 1, 3, 2], dtype=np.int8)
        packed = vectorized_gt_pack(cyvcf2_codes)

        csv_str = _bytes_to_csv(packed)
        restored = _csv_bytes_to_python(csv_str)

        assert restored == packed
        unpacked = unpack_genotypes(restored, 4)
        # Remap: cyvcf2 [0,1,3,2] → packed [0,1,2,3]
        from graphmana.ingest.genotype_packer import GT_REMAP
        expected = GT_REMAP[cyvcf2_codes]
        np.testing.assert_array_equal(unpacked, expected)


# ---------------------------------------------------------------------------
# CSV list helpers
# ---------------------------------------------------------------------------


class TestCSVListHelpers:
    """Test _csv_int_list and _csv_str_list."""

    def test_csv_int_list_basic(self):
        assert _csv_int_list("1;2;3") == [1, 2, 3]

    def test_csv_int_list_single(self):
        assert _csv_int_list("42") == [42]

    def test_csv_int_list_empty(self):
        assert _csv_int_list("") == []

    def test_csv_str_list_basic(self):
        assert _csv_str_list("AFR;EUR;EAS") == ["AFR", "EUR", "EAS"]

    def test_csv_str_list_single(self):
        assert _csv_str_list("POP1") == ["POP1"]

    def test_csv_str_list_empty(self):
        assert _csv_str_list("") == []


# ---------------------------------------------------------------------------
# _neo4j_bytes conversion
# ---------------------------------------------------------------------------


class TestNeo4jBytes:
    """Test _neo4j_bytes conversion from various Neo4j return types."""

    def test_none_returns_empty(self):
        assert _neo4j_bytes(None) == b""

    def test_bytes_passthrough(self):
        assert _neo4j_bytes(b"\x01\x02") == b"\x01\x02"

    def test_bytearray_to_bytes(self):
        assert _neo4j_bytes(bytearray([1, 2, 3])) == b"\x01\x02\x03"

    def test_list_with_unsigned(self):
        """Neo4j returns byte arrays as lists; values may be 0-255."""
        result = _neo4j_bytes([0, 127, 128, 255])
        assert result == bytes([0, 127, 128, 255])


# ---------------------------------------------------------------------------
# HomRef fast extension
# ---------------------------------------------------------------------------


class TestExtendHomrefFast:
    """Test _extend_homref_fast for zero-padding packed arrays."""

    def test_gt_extend_to_next_byte(self):
        """Extend gt_packed from 4 samples (1 byte) to 8 (2 bytes)."""
        old = bytes([0b10110100])  # 4 samples packed
        result = _extend_homref_fast(old, 4, 8, is_gt=True)
        assert len(result) == 2  # 8 samples = 2 bytes for gt
        assert result[0] == old[0]  # original preserved
        assert result[1] == 0  # HomRef padding = zeros

    def test_phase_extend_to_next_byte(self):
        """Extend phase_packed from 8 samples (1 byte) to 16 (2 bytes)."""
        old = bytes([0b10101010])  # 8 samples packed
        result = _extend_homref_fast(old, 8, 16, is_gt=False)
        assert len(result) == 2
        assert result[0] == old[0]
        assert result[1] == 0

    def test_no_extension_needed(self):
        """If old array already long enough, truncate to exact length."""
        old = bytes([0xFF, 0xFF])  # 2 bytes = 8 gt samples
        result = _extend_homref_fast(old, 4, 5, is_gt=True)
        assert len(result) == 2  # 5 samples = 2 gt bytes
        assert result == old[:2]

    def test_extend_single_sample(self):
        """Extend from 1 sample to 2 (both within same byte)."""
        old = bytes([0b01])  # 1 sample (Het = 01)
        result = _extend_homref_fast(old, 1, 2, is_gt=True)
        assert len(result) == 1  # 2 samples still fits in 1 gt byte
        assert result[0] == old[0]  # new bits are zero (HomRef)

    def test_extend_from_empty(self):
        """Extend from 0 samples to 4."""
        result = _extend_homref_fast(b"", 0, 4, is_gt=True)
        assert len(result) == 1  # 4 gt samples = 1 byte
        assert result[0] == 0  # all HomRef

    def test_extend_preserves_existing_bits(self):
        """Verify existing packed data is not corrupted."""
        from graphmana.ingest.genotype_packer import vectorized_gt_pack, unpack_genotypes

        cyvcf2_codes = np.array([0, 1, 3, 2], dtype=np.int8)
        packed = vectorized_gt_pack(cyvcf2_codes)

        extended = _extend_homref_fast(bytes(packed), 4, 8, is_gt=True)
        unpacked_original = unpack_genotypes(packed, 4)
        unpacked_extended = unpack_genotypes(extended, 8)

        # First 4 samples unchanged
        np.testing.assert_array_equal(unpacked_extended[:4], unpacked_original)
        # New 4 samples are HomRef (0)
        np.testing.assert_array_equal(unpacked_extended[4:8], np.zeros(4, dtype=np.int8))

    def test_byte_boundary_crossing(self):
        """Extend from 4 (1 byte) to 5 (2 bytes) — crosses gt byte boundary."""
        old = bytes([0b10110100])  # 4 samples
        result = _extend_homref_fast(old, 4, 5, is_gt=True)
        assert len(result) == 2
        assert result[0] == old[0]
        assert result[1] == 0

    def test_phase_byte_boundary_crossing(self):
        """Extend from 8 (1 byte) to 9 (2 bytes) — crosses phase byte boundary."""
        old = bytes([0b11111111])  # 8 samples, all phased
        result = _extend_homref_fast(old, 8, 9, is_gt=False)
        assert len(result) == 2
        assert result[0] == old[0]
        assert result[1] == 0  # new sample unphased
