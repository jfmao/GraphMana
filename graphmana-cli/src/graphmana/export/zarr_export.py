"""Zarr/sgkit-compatible exporter (FULL PATH).

Exports variant data as a Zarr v2 store with datasets following
sgkit conventions: call_genotype, variant_contig, variant_position,
variant_allele, sample_id.
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np

from graphmana.export.base import BaseExporter

logger = logging.getLogger(__name__)

DEFAULT_CHUNK_SIZE = 10000


def _check_zarr():
    """Check zarr is available, raise ImportError with clear message if not."""
    try:
        import zarr  # noqa: F401
    except ImportError:
        raise ImportError(
            "Zarr export requires the 'zarr' package. " "Install it with: pip install zarr"
        )


class ZarrExporter(BaseExporter):
    """Export variant data as sgkit-compatible Zarr store (FULL PATH)."""

    def export(
        self,
        output: Path,
        *,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> dict:
        """Export Zarr format.

        Args:
            output: Output directory path for the Zarr store.
            chunk_size: Number of variants per chunk (default 10000).

        Returns:
            Summary dict with n_variants, n_samples, format.
        """
        _check_zarr()
        import zarr

        output = Path(output)
        output.parent.mkdir(parents=True, exist_ok=True)

        samples = self._load_samples()
        n_samples = len(samples)
        packed_indices = np.array(sorted(s["packed_index"] for s in samples), dtype=np.int64)
        idx_to_sample = {s["packed_index"]: s["sampleId"] for s in samples}
        sample_ids = [idx_to_sample[idx] for idx in packed_indices]

        target_chroms = self._get_target_chromosomes()
        ploidy = 2  # diploid default

        # First pass: count variants (no data buffering)
        n_variants = 0
        for chrom in target_chroms:
            for _props in self._iter_variants(chrom):
                n_variants += 1

        if n_variants == 0:
            logger.warning("No variants to export")
            store = zarr.open(str(output), mode="w")
            store.attrs["sgkit_version"] = "0.0.0"
            return {
                "n_variants": 0,
                "n_samples": n_samples,
                "chromosomes": target_chroms,
                "format": "zarr",
            }

        v_chunk = min(chunk_size, n_variants)

        # Create Zarr store with pre-sized datasets
        store = zarr.open(str(output), mode="w")

        store.create_dataset(
            "sample_id",
            data=np.array(sample_ids, dtype="U"),
            chunks=(n_samples,),
        )

        variant_contig = store.zeros(
            "variant_contig", shape=(n_variants,), chunks=(v_chunk,), dtype="U32"
        )
        variant_position = store.zeros(
            "variant_position", shape=(n_variants,), chunks=(v_chunk,), dtype=np.int64
        )
        variant_allele = store.zeros(
            "variant_allele", shape=(n_variants, 2), chunks=(v_chunk, 2), dtype="U64"
        )
        call_genotype = store.full(
            "call_genotype",
            fill_value=-1,
            shape=(n_variants, n_samples, ploidy),
            chunks=(v_chunk, n_samples, ploidy),
            dtype=np.int8,
        )

        # Genotype lookup: gt_code → (allele0, allele1)
        _GT_MAP = np.array(
            [[0, 0], [0, 1], [1, 1], [-1, -1]], dtype=np.int8
        )

        # Second pass: write directly to Zarr chunk by chunk
        v_idx = 0
        for chrom in target_chroms:
            for props in self._iter_variants(chrom):
                variant_contig[v_idx] = props.get("chr", "")
                variant_position[v_idx] = props.get("pos", 0)
                variant_allele[v_idx, 0] = props.get("ref", "")
                variant_allele[v_idx, 1] = props.get("alt", "")

                gt_codes, phase_bits, ploidy_flags = self._unpack_variant_genotypes(
                    props, packed_indices
                )
                props = self._maybe_recalculate_af(props, gt_codes, ploidy_flags)

                call_genotype[v_idx] = _GT_MAP[gt_codes]
                v_idx += 1

        # Attributes
        store.attrs["sgkit_version"] = "0.0.0"
        store.attrs["source"] = "graphmana"
        store.attrs["n_variants"] = n_variants
        store.attrs["n_samples"] = n_samples

        logger.info(
            "Zarr export: %d variants, %d samples, chunk_size=%d",
            n_variants,
            n_samples,
            chunk_size,
        )
        return {
            "n_variants": n_variants,
            "n_samples": n_samples,
            "chromosomes": target_chroms,
            "format": "zarr",
        }
