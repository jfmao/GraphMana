"""SeqArray GDS format exporter (FULL PATH).

Exports variant data as an HDF5 file with a structure mirroring the
SeqArray R package (GDS format). Can be read by SeqArray::seqOpen()
after converting from HDF5 with SeqArray::seqHDF5toGDS().
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np

from graphmana.export.base import BaseExporter

logger = logging.getLogger(__name__)


def _check_h5py():
    """Check h5py is available, raise ImportError with clear message if not."""
    try:
        import h5py  # noqa: F401
    except ImportError:
        raise ImportError(
            "GDS export requires the 'h5py' package. " "Install it with: pip install h5py"
        )


class GDSExporter(BaseExporter):
    """Export variant data as SeqArray GDS via HDF5 (FULL PATH)."""

    def export(self, output: Path, **kwargs) -> dict:
        """Export GDS format.

        Args:
            output: Output HDF5 file path (.gds or .h5).

        Returns:
            Summary dict with n_variants, n_samples, format.
        """
        _check_h5py()
        import h5py

        output = Path(output)
        output.parent.mkdir(parents=True, exist_ok=True)

        samples = self._load_samples()
        n_samples = len(samples)
        packed_indices = np.array(sorted(s["packed_index"] for s in samples), dtype=np.int64)
        idx_to_sample = {s["packed_index"]: s["sampleId"] for s in samples}
        sample_ids = [idx_to_sample[idx] for idx in packed_indices]

        target_chroms = self._get_target_chromosomes()

        # First pass: count variants without buffering data
        n_variants = 0
        for chrom in target_chroms:
            for _props in self._iter_variants(chrom):
                n_variants += 1

        # Genotype remap: 0→0(ref), 1→1(het), 2→2(alt), 3→255(missing)
        _GT_REMAP = np.array([0, 1, 2, 255], dtype=np.uint8)

        with h5py.File(str(output), "w") as f:
            f.attrs["FileFormat"] = "SEQ_ARRAY"
            f.attrs["source"] = "graphmana"

            dt = h5py.string_dtype()
            f.create_dataset("sample.id", data=np.array(sample_ids, dtype="S"), dtype=dt)

            if n_variants == 0:
                logger.warning("No variants to export")
                return {
                    "n_variants": 0,
                    "n_samples": n_samples,
                    "chromosomes": target_chroms,
                    "format": "gds",
                }

            # Pre-create resizable datasets
            ds_vid = f.create_dataset("variant.id", shape=(n_variants,), dtype=dt)
            ds_chr = f.create_dataset("chromosome", shape=(n_variants,), dtype=dt)
            ds_pos = f.create_dataset("position", shape=(n_variants,), dtype=np.int32)
            ds_allele = f.create_dataset("allele", shape=(n_variants,), dtype=dt)

            geno_grp = f.create_group("genotype")
            ds_geno = geno_grp.create_dataset(
                "data", shape=(n_variants, n_samples), dtype=np.uint8, fillvalue=255
            )

            # Second pass: stream variants directly into HDF5
            v_idx = 0
            for chrom in target_chroms:
                for props in self._iter_variants(chrom):
                    ds_vid[v_idx] = props.get("variantId", "")
                    ds_chr[v_idx] = props.get("chr", "")
                    ds_pos[v_idx] = props.get("pos", 0)
                    ds_allele[v_idx] = f"{props.get('ref', '')},{props.get('alt', '')}"

                    gt_codes, phase_bits, ploidy_flags = self._unpack_variant_genotypes(
                        props, packed_indices
                    )
                    props = self._maybe_recalculate_af(props, gt_codes, ploidy_flags)
                    ds_geno[v_idx] = _GT_REMAP[gt_codes]
                    v_idx += 1

        logger.info("GDS export: %d variants, %d samples", n_variants, n_samples)
        return {
            "n_variants": n_variants,
            "n_samples": n_samples,
            "chromosomes": target_chroms,
            "format": "gds",
        }
