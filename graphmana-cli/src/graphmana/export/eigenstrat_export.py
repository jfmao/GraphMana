"""EIGENSTRAT format exporter (FULL PATH).

Exports .geno/.snp/.ind files for smartPCA and AdmixTools.
Biallelic SNPs only (same constraint as PLINK).

EIGENSTRAT genotype encoding counts REF alleles:
  2 = HomRef (2 copies of REF)
  1 = Het (1 copy of REF)
  0 = HomAlt (0 copies of REF)
  9 = Missing

GraphMana remap: [2, 1, 0, 9]
  GraphMana 0 (HomRef)  -> EIGENSTRAT 2
  GraphMana 1 (Het)     -> EIGENSTRAT 1
  GraphMana 2 (HomAlt)  -> EIGENSTRAT 0
  GraphMana 3 (Missing) -> EIGENSTRAT 9
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np

from graphmana.export.base import BaseExporter

logger = logging.getLogger(__name__)

EIGENSTRAT_REMAP = np.array([2, 1, 0, 9], dtype=np.uint8)


def gt_to_eigenstrat(gt_codes: np.ndarray) -> str:
    """Remap GraphMana genotype codes to EIGENSTRAT digit string.

    Args:
        gt_codes: int8 array of GraphMana codes (0-3).

    Returns:
        String of EIGENSTRAT digits (e.g. "2219102").
    """
    remapped = EIGENSTRAT_REMAP[gt_codes.astype(np.uint8)]
    return "".join(str(d) for d in remapped)


def format_snp_line(props: dict) -> str:
    """Format a .snp line for a variant.

    Format: snpId chr genetic_dist physical_pos ref alt
    """
    vid = props.get("variantId", ".")
    chrom = props.get("chr", "0")
    pos = props.get("pos", 0)
    ref = props.get("ref", "N")
    alt = props.get("alt", "N")
    return f"{vid}\t{chrom}\t0.0\t{pos}\t{ref}\t{alt}"


def format_ind_line(sample: dict) -> str:
    """Format an .ind line for a sample.

    Format: sampleId sex population
    """
    sid = sample["sampleId"]
    sex_val = sample.get("sex")
    if sex_val == "male" or sex_val == "1" or sex_val == 1:
        sex = "M"
    elif sex_val == "female" or sex_val == "2" or sex_val == 2:
        sex = "F"
    else:
        sex = "U"
    pop = sample.get("population", "Unknown")
    return f"{sid}\t{sex}\t{pop}"


class EIGENSTRATExporter(BaseExporter):
    """Export to EIGENSTRAT format (.geno/.snp/.ind)."""

    def export(self, output: Path) -> dict:
        """Export EIGENSTRAT format.

        Args:
            output: Output path stem (e.g. "out" produces out.geno, out.snp, out.ind).

        Returns:
            Summary dict with n_variants, n_samples, n_skipped.
        """
        output = Path(output)
        stem = output.parent / output.stem
        geno_path = stem.with_suffix(".geno")
        snp_path = stem.with_suffix(".snp")
        ind_path = stem.with_suffix(".ind")

        samples = self._load_samples()
        target_chroms = self._get_target_chromosomes()
        packed_indices = np.array([s["packed_index"] for s in samples], dtype=np.int64)

        # Write .ind
        stem.parent.mkdir(parents=True, exist_ok=True)
        with open(ind_path, "w") as f:
            for sample in samples:
                f.write(format_ind_line(sample) + "\n")

        n_variants = 0
        n_skipped = 0

        with open(geno_path, "w") as geno_f, open(snp_path, "w") as snp_f:
            for chrom in target_chroms:
                for props in self._iter_variants(chrom):
                    # Biallelic SNPs only
                    vtype = props.get("variant_type")
                    if vtype and vtype != "SNP":
                        n_skipped += 1
                        continue

                    gt_codes, _, ploidy_flags = self._unpack_variant_genotypes(
                        props, packed_indices
                    )
                    props = self._maybe_recalculate_af(props, gt_codes, ploidy_flags)
                    geno_f.write(gt_to_eigenstrat(gt_codes) + "\n")
                    snp_f.write(format_snp_line(props) + "\n")
                    n_variants += 1

        if n_skipped > 0:
            logger.warning(
                "Skipped %d non-SNP variants (EIGENSTRAT biallelic SNPs only)",
                n_skipped,
            )

        logger.info(
            "EIGENSTRAT export: %d variants, %d samples, %d skipped",
            n_variants,
            len(samples),
            n_skipped,
        )
        return {
            "n_variants": n_variants,
            "n_samples": len(samples),
            "n_skipped": n_skipped,
            "chromosomes": target_chroms,
            "format": "eigenstrat",
            "files": [str(geno_path), str(snp_path), str(ind_path)],
        }
