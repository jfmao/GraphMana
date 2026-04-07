"""PLINK 2.0 format exporter (FULL PATH).

Exports .pgen/.pvar/.psam files. Supports biallelic SNPs and INDELs.

PLINK 2.0 pgenlib encoding:
  0 = HomRef, 1 = Het, 2 = HomAlt, 3 = Missing

GraphMana packed encoding:
  0 = HomRef, 1 = Het, 2 = HomAlt, 3 = Missing

No remap needed — encodings are identical.
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np

from graphmana.export.base import BaseExporter

logger = logging.getLogger(__name__)


def format_pvar_line(props: dict) -> str:
    """Format a .pvar line for a variant.

    Format: #CHROM  POS  ID  REF  ALT
    """
    chrom = props.get("chr", "0")
    pos = props.get("pos", 0)
    vid = props.get("variantId", ".")
    ref = props.get("ref", "N")
    alt = props.get("alt", "N")
    return f"{chrom}\t{pos}\t{vid}\t{ref}\t{alt}"


def format_psam_line(sample: dict) -> str:
    """Format a .psam line for a sample.

    Format: #FID  IID  SEX
    SEX: 1=male, 2=female, NA=unknown
    """
    fid = sample.get("population", "0")
    iid = sample["sampleId"]
    sex_val = sample.get("sex")
    if sex_val == "male" or sex_val == "1" or sex_val == 1:
        sex = "1"
    elif sex_val == "female" or sex_val == "2" or sex_val == 2:
        sex = "2"
    else:
        sex = "NA"
    return f"{fid}\t{iid}\t{sex}"


class PLINK2Exporter(BaseExporter):
    """Export to PLINK 2.0 format (.pgen/.pvar/.psam)."""

    def export(self, output: Path) -> dict:
        """Export PLINK 2.0 format.

        Uses a two-pass approach per chromosome:
          1. Write .pvar lines, count variants
          2. Create PgenWriter with known variant_ct, write .pgen

        Note: PgenWriter requires variant_ct upfront and writes a binary
        format that cannot be concatenated per-chromosome, so parallel
        export is not supported for this format.

        Args:
            output: Output path stem (e.g. "out" produces out.pgen, out.pvar, out.psam).

        Returns:
            Summary dict with n_variants, n_samples.
        """
        if self._threads > 1:
            logger.warning(
                "PLINK 2.0 export does not support parallel mode "
                "(PgenWriter requires upfront variant count). "
                "Using single-threaded export."
            )

        try:
            import pgenlib
        except ImportError:
            raise ImportError(
                "pgenlib is required for PLINK 2.0 export. " "Install it with: pip install pgenlib"
            )

        output = Path(output)
        stem = output.parent / output.stem
        pgen_path = stem.with_suffix(".pgen")
        pvar_path = stem.with_suffix(".pvar")
        psam_path = stem.with_suffix(".psam")

        samples = self._load_samples()
        target_chroms = self._get_target_chromosomes()
        packed_indices = np.array([s["packed_index"] for s in samples], dtype=np.int64)
        n_samples = len(samples)

        # Write .psam
        stem.parent.mkdir(parents=True, exist_ok=True)
        with open(psam_path, "w") as f:
            f.write("#FID\tIID\tSEX\n")
            for sample in samples:
                f.write(format_psam_line(sample) + "\n")

        # Single pass: collect .pvar lines and genotype data, then write both.
        # PgenWriter requires variant_ct upfront, so we buffer variant data
        # in memory for the .pgen write. For chr22-scale data (~1M variants)
        # this requires ~3 GB RAM (1M x 3202 x 1 byte); for whole-genome,
        # consider the PLINK 1.9 export + plink2 --make-pgen conversion path.
        pvar_lines = []
        gt_buffer = []

        for chrom in target_chroms:
            for props in self._iter_variants(chrom):
                pvar_lines.append(format_pvar_line(props))
                gt_codes, _, ploidy_flags = self._unpack_variant_genotypes(
                    props, packed_indices
                )
                props = self._maybe_recalculate_af(props, gt_codes, ploidy_flags)
                gt_buffer.append(gt_codes.astype(np.int32))

        n_variants = len(pvar_lines)

        # Write .pvar
        stem.parent.mkdir(parents=True, exist_ok=True)
        with open(pvar_path, "w") as f:
            f.write("#CHROM\tPOS\tID\tREF\tALT\n")
            for line in pvar_lines:
                f.write(line + "\n")

        if n_variants == 0:
            logger.warning("No variants to export")
            return {
                "n_variants": 0,
                "n_samples": n_samples,
                "chromosomes": target_chroms,
                "format": "plink2",
                "files": [str(pgen_path), str(pvar_path), str(psam_path)],
            }

        # Write .pgen
        writer = pgenlib.PgenWriter(
            filename=bytes(str(pgen_path), "utf-8"),
            sample_ct=n_samples,
            variant_ct=n_variants,
            nonref_flags=False,
        )
        try:
            for gt_row in gt_buffer:
                writer.append_biallelic(gt_row)
        finally:
            writer.close()

        logger.info(
            "PLINK 2.0 export: %d variants, %d samples",
            n_variants,
            n_samples,
        )
        return {
            "n_variants": n_variants,
            "n_samples": n_samples,
            "chromosomes": target_chroms,
            "format": "plink2",
            "files": [str(pgen_path), str(pvar_path), str(psam_path)],
        }
