"""Schema v1.1 coverage: called_packed mask + sparse gt_packed encoding.

These tests exercise the new behaviors introduced in v1.1 to preserve the
HomRef-vs-Missing distinction across incremental batches and to extend the
sample-count ceiling via sparse storage. See ``docs/gvcf-workflow.md`` and
``tasks/v1.1_sparse_called_packed.md`` for design notes.
"""

from __future__ import annotations

import numpy as np

from graphmana.export.base import BaseExporter
from graphmana.ingest.array_ops import (
    _genotype_contributions,
    extend_called_packed,
    pad_called_for_new_variant,
    pad_gt_for_new_variant,
)
from graphmana.ingest.genotype_packer import (
    build_called_packed,
    build_called_packed_all,
    decode_gt_blob,
    encode_gt_blob,
    unpack_called_packed,
    unpack_genotypes,
    vectorized_gt_pack,
)

# ---------------------------------------------------------------------------
# Packer primitives
# ---------------------------------------------------------------------------


class TestCalledPackedPacker:
    def test_roundtrip_mixed(self):
        gt = np.array([0, 1, 2, 3, 2, 0, 0, 1, 2, 0], dtype=np.int8)  # cyvcf2 codes
        packed = build_called_packed(gt)
        unpacked = unpack_called_packed(packed, len(gt))
        # cyvcf2 code 2 = MISSING → bit cleared; everything else → bit set
        expected = np.array([1, 1, 0, 1, 0, 1, 1, 1, 0, 1], dtype=np.uint8)
        np.testing.assert_array_equal(unpacked, expected)

    def test_legacy_none_treats_as_all_called(self):
        np.testing.assert_array_equal(
            unpack_called_packed(None, 11), np.ones(11, dtype=np.uint8)
        )
        np.testing.assert_array_equal(
            unpack_called_packed(b"", 7), np.ones(7, dtype=np.uint8)
        )

    def test_build_called_packed_all(self):
        assert np.all(unpack_called_packed(build_called_packed_all(17, 1), 17) == 1)
        assert np.all(unpack_called_packed(build_called_packed_all(17, 0), 17) == 0)


# ---------------------------------------------------------------------------
# Sparse gt_packed encoding
# ---------------------------------------------------------------------------


class TestSparseGtBlob:
    def test_dense_blob_roundtrip(self):
        gt = np.array([0, 1, 2, 3] * 25, dtype=np.int8)  # balanced, not sparse-friendly
        dense = vectorized_gt_pack(gt)
        blob = encode_gt_blob(dense, 100)
        assert blob[0] == 0x00  # dense tag
        assert decode_gt_blob(blob, 100) == dense

    def test_sparse_blob_roundtrip_rare_variant(self):
        # 1000-sample variant with only 3 non-HomRef samples → sparse should win
        gt = np.zeros(1000, dtype=np.int8)
        gt[[7, 42, 900]] = [1, 3, 1]
        dense = vectorized_gt_pack(gt)
        blob = encode_gt_blob(dense, 1000)
        assert blob[0] == 0x01
        # Sparse should be substantially smaller than dense.
        assert len(blob) < len(dense) // 3
        decoded = decode_gt_blob(blob, 1000)
        assert decoded == dense
        # And the per-sample slots round-trip.
        assert list(unpack_genotypes(decoded, 1000)) == list(unpack_genotypes(dense, 1000))

    def test_legacy_untagged_blob_still_decodes(self):
        gt = np.array([0, 1, 2, 3] * 10, dtype=np.int8)
        dense = vectorized_gt_pack(gt)  # schema v1.0 bare dense bytes
        assert decode_gt_blob(dense, 40) == dense


# ---------------------------------------------------------------------------
# Cross-batch padding (Phase C)
# ---------------------------------------------------------------------------


class TestPadNewVariantCalled:
    def test_default_marks_existing_uncalled(self):
        # Batch 2 introduces a new variant; existing samples were not seen.
        new_gt = np.array([0, 1, 2, 3], dtype=np.int8)  # last is MISSING in cyvcf2
        called = pad_called_for_new_variant(n_existing=5, new_gt_types=new_gt)
        cm = unpack_called_packed(called, 9)
        assert list(cm) == [0, 0, 0, 0, 0, 1, 1, 0, 1]
        # And the gt pad is Missing (code 3) for existing samples.
        gt_bytes = pad_gt_for_new_variant(5, new_gt)
        codes = unpack_genotypes(gt_bytes, 9)
        assert list(codes[:5]) == [3, 3, 3, 3, 3]

    def test_legacy_mode_marks_existing_called(self):
        new_gt = np.array([0, 1, 2, 3], dtype=np.int8)
        called = pad_called_for_new_variant(5, new_gt, assume_homref=True)
        cm = unpack_called_packed(called, 9)
        assert list(cm[:5]) == [1, 1, 1, 1, 1]  # existing samples flagged called
        gt_bytes = pad_gt_for_new_variant(5, new_gt, assume_homref=True)
        codes = unpack_genotypes(gt_bytes, 9)
        assert list(codes[:5]) == [0, 0, 0, 0, 0]  # existing samples HomRef


class TestExtendCalled:
    def test_new_samples_respect_missing_status(self):
        ex = extend_called_packed(
            existing=None, n_existing=3, new_gt_types=np.array([0, 2, 1], dtype=np.int8)
        )
        # First 3 samples: legacy "all called". Next 3: HomRef(1), Missing(0), Het(1).
        cm = unpack_called_packed(ex, 6)
        assert list(cm) == [1, 1, 1, 1, 0, 1]


# ---------------------------------------------------------------------------
# Allele stats honor called mask
# ---------------------------------------------------------------------------


class TestAlleleStatsHonorCalled:
    def test_uncalled_samples_excluded_from_an(self):
        # 4 samples: Het, HomAlt, HomRef, Missing — but sample 2 (HomAlt) was
        # not interrogated in this batch.
        gt_codes = np.array([1, 2, 0, 3], dtype=np.uint8)
        called = np.array([1, 0, 1, 1], dtype=np.uint8)
        contribs = _genotype_contributions(gt_codes, called)
        # an excludes sample 2 (uncalled) and sample 3 (called-missing): 4 alleles total.
        assert contribs == {"ac_delta": 1, "an_delta": 4, "het_delta": 1, "hom_alt_delta": 0}

    def test_legacy_none_means_all_counted(self):
        gt_codes = np.array([1, 2, 0, 3], dtype=np.uint8)
        contribs = _genotype_contributions(gt_codes)
        assert contribs == {"ac_delta": 3, "an_delta": 6, "het_delta": 1, "hom_alt_delta": 1}


# ---------------------------------------------------------------------------
# Exporter unpack masks uncalled samples to Missing
# ---------------------------------------------------------------------------


class TestExporterUnpackHonorsCalled:
    """The FULL PATH base unpack must coerce uncalled slots to gt=3."""

    def _unpack(self, gt_codes, called_bits=None):
        """Build a fake variant dict and run it through BaseExporter._unpack_variant_genotypes."""
        n = len(gt_codes)

        # Build dense gt_packed directly from packed codes (not cyvcf2).
        from graphmana.ingest.array_ops import _pack_codes_direct

        gt_packed = _pack_codes_direct(np.asarray(gt_codes, dtype=np.uint8))
        props = {
            "gt_packed": gt_packed,
            "phase_packed": b"",
            "ploidy_packed": None,
            "called_packed": (
                build_called_packed_all(n, 1)
                if called_bits is None
                else np.packbits(
                    np.asarray(called_bits, dtype=np.uint8), bitorder="little"
                ).tobytes()
            ),
        }

        # BaseExporter is abstract; stand up a minimal subclass for testing.
        class _Dummy(BaseExporter):
            def export(self, output):  # pragma: no cover - unused
                return {}

        dummy = _Dummy.__new__(_Dummy)
        BaseExporter.__init__(dummy, conn=None)
        gt, phase, ploidy = dummy._unpack_variant_genotypes(
            props, np.arange(n, dtype=np.int64)
        )
        return gt

    def test_all_called_returns_original_codes(self):
        gt = self._unpack([0, 1, 2, 3])
        assert list(gt) == [0, 1, 2, 3]

    def test_uncalled_slots_forced_to_missing(self):
        # Sample 1 (Het) and sample 2 (HomAlt) are flagged uncalled.
        gt = self._unpack([0, 1, 2, 0], called_bits=[1, 0, 0, 1])
        assert list(gt) == [0, 3, 3, 0]
