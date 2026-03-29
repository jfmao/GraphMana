"""PLINK 1.9 binary format exporter (FULL PATH).

Exports .bed/.bim/.fam files for biallelic SNPs.

PLINK BED encoding (SNP-major mode):
  00 = Homozygous A1 (ALT in our convention)
  01 = Missing
  10 = Heterozygous
  11 = Homozygous A2 (REF in our convention)

GraphMana packed encoding:
  00 = HomRef, 01 = Het, 10 = HomAlt, 11 = Missing

Remap: {0->3, 1->2, 2->0, 3->1}
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np

from graphmana.export.base import BaseExporter

logger = logging.getLogger(__name__)

# Remap GraphMana genotype codes to PLINK BED encoding
# GraphMana: 0=HomRef → PLINK 11 (3), 1=Het → PLINK 10 (2),
#            2=HomAlt → PLINK 00 (0), 3=Missing → PLINK 01 (1)
PLINK_REMAP = np.array([3, 2, 0, 1], dtype=np.uint8)

# PLINK BED magic bytes: SNP-major mode
BED_MAGIC = bytes([0x6C, 0x1B, 0x01])


def gt_to_plink_packed(gt_codes: np.ndarray) -> bytes:
    """Remap GraphMana genotype codes to PLINK and pack 4 samples/byte.

    Args:
        gt_codes: int8 array of GraphMana codes (0-3).

    Returns:
        PLINK-encoded packed bytes.
    """
    remapped = PLINK_REMAP[gt_codes.astype(np.uint8)]
    n_padded = ((len(remapped) + 3) // 4) * 4
    padded = np.zeros(n_padded, dtype=np.uint8)
    padded[: len(remapped)] = remapped
    groups = padded.reshape(-1, 4)
    packed = groups[:, 0] | (groups[:, 1] << 2) | (groups[:, 2] << 4) | (groups[:, 3] << 6)
    return packed.tobytes()


def format_fam_line(sample: dict) -> str:
    """Format a .fam line for a sample.

    Format: FID IID PID MID SEX PHENO
    """
    fid = sample.get("population", "0")
    iid = sample["sampleId"]
    sex_val = sample.get("sex")
    # PLINK sex: 1=male, 2=female, 0=unknown
    if sex_val == "male" or sex_val == "1" or sex_val == 1:
        sex = "1"
    elif sex_val == "female" or sex_val == "2" or sex_val == 2:
        sex = "2"
    else:
        sex = "0"
    return f"{fid}\t{iid}\t0\t0\t{sex}\t-9"


def format_bim_line(props: dict) -> str:
    """Format a .bim line for a variant.

    Format: CHR VARID CM POS A1 A2
    A1 = ALT (effect allele), A2 = REF
    """
    chrom = props.get("chr", "0")
    vid = props.get("variantId", ".")
    pos = props.get("pos", 0)
    ref = props.get("ref", "N")
    alt = props.get("alt", "N")
    return f"{chrom}\t{vid}\t0\t{pos}\t{alt}\t{ref}"


class PLINKExporter(BaseExporter):
    """Export to PLINK 1.9 binary format (.bed/.bim/.fam)."""

    def export(self, output: Path) -> dict:
        """Export PLINK 1.9 format.

        Args:
            output: Output path stem (e.g. "out" produces out.bed, out.bim, out.fam).

        Returns:
            Summary dict with n_variants, n_samples, n_skipped.
        """
        if self._threads > 1:
            return self._export_parallel(output)

        output = Path(output)
        stem = output.parent / output.stem
        bed_path = stem.with_suffix(".bed")
        bim_path = stem.with_suffix(".bim")
        fam_path = stem.with_suffix(".fam")

        samples = self._load_samples()
        target_chroms = self._get_target_chromosomes()

        packed_indices = np.array([s["packed_index"] for s in samples], dtype=np.int64)

        # Write .fam
        stem.parent.mkdir(parents=True, exist_ok=True)
        with open(fam_path, "w") as f:
            for sample in samples:
                f.write(format_fam_line(sample) + "\n")

        n_variants = 0
        n_skipped = 0

        with open(bed_path, "wb") as bed_f, open(bim_path, "w") as bim_f:
            bed_f.write(BED_MAGIC)

            for chrom in target_chroms:
                for props in self._iter_variants(chrom):
                    # PLINK 1.9: biallelic SNPs only
                    vtype = props.get("variant_type")
                    if vtype and vtype != "SNP":
                        n_skipped += 1
                        continue

                    gt_codes, _, _ = self._unpack_variant_genotypes(props, packed_indices)
                    bed_f.write(gt_to_plink_packed(gt_codes))
                    bim_f.write(format_bim_line(props) + "\n")
                    n_variants += 1

        if n_skipped > 0:
            logger.warning(
                "Skipped %d non-SNP variants (PLINK 1.9 biallelic SNPs only)",
                n_skipped,
            )

        logger.info(
            "PLINK export: %d variants, %d samples, %d skipped",
            n_variants,
            len(samples),
            n_skipped,
        )
        return {
            "n_variants": n_variants,
            "n_samples": len(samples),
            "n_skipped": n_skipped,
            "chromosomes": target_chroms,
            "format": "plink",
            "files": [str(bed_path), str(bim_path), str(fam_path)],
        }

    def _export_parallel(self, output: Path) -> dict:
        """Parallel PLINK export: .fam once, .bed/.bim per chromosome."""
        from graphmana.export.parallel import run_export_parallel

        output = Path(output)
        stem = output.parent / output.stem
        bed_path = stem.with_suffix(".bed")
        bim_path = stem.with_suffix(".bim")
        fam_path = stem.with_suffix(".fam")

        samples = self._load_samples()
        target_chroms = self._get_target_chromosomes()

        # Write .fam once (shared across all chromosomes)
        stem.parent.mkdir(parents=True, exist_ok=True)
        with open(fam_path, "w") as f:
            for sample in samples:
                f.write(format_fam_line(sample) + "\n")

        def write_header(out_path, conn):
            out_path = Path(out_path)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            with open(out_path, "wb") as f:
                f.write(BED_MAGIC)

        def merge_plink(out_path, chr_files):
            with open(bed_path, "ab") as bed_out, open(bim_path, "w") as bim_out:
                for _chrom, tmp_path in chr_files:
                    tmp_stem = tmp_path.parent / tmp_path.stem
                    worker_bed = tmp_stem.with_suffix(".bed")
                    worker_bim = tmp_stem.with_suffix(".bim")

                    if worker_bed.exists():
                        with open(worker_bed, "rb") as f:
                            data = f.read()
                            if data[:3] == BED_MAGIC:
                                data = data[3:]
                            bed_out.write(data)

                    if worker_bim.exists():
                        with open(worker_bim, "r") as f:
                            bim_out.write(f.read())

        summary = run_export_parallel(
            PLINKExporter,
            self._conn,
            threads=self._threads,
            output=bed_path,
            filter_config=self._filter_config,
            target_chroms=target_chroms,
            export_kwargs={},
            header_writer=write_header,
            merge_func=merge_plink,
            recalculate_af=self._recalculate_af,
        )
        summary["n_samples"] = len(samples)
        summary["format"] = "plink"
        summary["files"] = [str(bed_path), str(bim_path), str(fam_path)]
        return summary
