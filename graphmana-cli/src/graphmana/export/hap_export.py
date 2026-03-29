"""Haplotype format exporter (FULL PATH).

Exports per-chromosome .hap/.map files for selscan.
Phased data required — raises error if no phase data available.

.map format (one line per variant):
  chr  variantId  genetic_distance  position

.hap format (haplotype-major, two rows per diploid sample, space-separated 0/1):
  0 1 0 0 1
  1 0 0 1 1

Haplotype encoding per sample:
  HomRef(0) -> hap1=0, hap2=0
  Het(1), phase=0 -> hap1=0, hap2=1; phase=1 -> hap1=1, hap2=0
  HomAlt(2) -> hap1=1, hap2=1
  Missing(3) -> hap1=0, hap2=0 (treated as ref, logged warning)

Per-chromosome output: selscan runs per-chromosome.
  Single chromosome -> stem.hap, stem.map
  Multiple chromosomes -> stem_chr1.hap, stem_chr1.map, etc.
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np

from graphmana.export.base import BaseExporter

logger = logging.getLogger(__name__)


def format_map_line(props: dict) -> str:
    """Format a .map line for a variant.

    Format: chr variantId genetic_distance position

    Args:
        props: Variant properties dict.

    Returns:
        Space-separated map line.
    """
    chrom = props.get("chr", "0")
    vid = props.get("variantId", ".")
    pos = props.get("pos", 0)
    return f"{chrom} {vid} 0.0 {pos}"


def gt_phase_to_haplotypes(
    gt_codes: np.ndarray,
    phase_bits: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    """Convert genotype codes and phase bits to haplotype arrays.

    Args:
        gt_codes: int8 array of GraphMana codes (0-3) for one variant.
        phase_bits: uint8 array of phase bits for one variant.

    Returns:
        (hap1, hap2): uint8 arrays of 0/1 for all samples.
    """
    n = len(gt_codes)
    hap1 = np.zeros(n, dtype=np.uint8)
    hap2 = np.zeros(n, dtype=np.uint8)

    # HomAlt: both haplotypes carry alt
    homalt = gt_codes == 2
    hap1[homalt] = 1
    hap2[homalt] = 1

    # Het: one haplotype carries alt, phase determines which
    het = gt_codes == 1
    het_phased = het & (phase_bits == 1)
    het_unphased = het & (phase_bits == 0)
    # phase=1: ALT on second haplotype → hap1=0(REF), hap2=1(ALT)
    hap2[het_phased] = 1
    # phase=0: ALT on first haplotype → hap1=1(ALT), hap2=0(REF)
    hap1[het_unphased] = 1

    # HomRef (0) and Missing (3) stay as 0,0
    return hap1, hap2


class HAPExporter(BaseExporter):
    """Export to selscan haplotype format (.hap/.map).

    Phased data required. Outputs per-chromosome files.
    """

    def export(self, output: Path) -> dict:
        """Export haplotype format.

        Args:
            output: Output path stem. Produces .hap/.map files.

        Returns:
            Summary dict with n_variants, n_samples, format, files.

        Raises:
            ValueError: If no phase data is available.
        """
        output = Path(output)
        stem = output.parent / output.stem
        stem.parent.mkdir(parents=True, exist_ok=True)

        samples = self._load_samples()
        target_chroms = self._get_target_chromosomes()
        packed_indices = np.array([s["packed_index"] for s in samples], dtype=np.int64)

        n_variants_total = 0
        n_missing = 0
        files: list[str] = []
        multi_chrom = len(target_chroms) > 1

        for chrom in target_chroms:
            # Buffer per-chromosome data
            hap1_list: list[np.ndarray] = []
            hap2_list: list[np.ndarray] = []
            map_lines: list[str] = []
            has_phase = False

            for props in self._iter_variants(chrom):
                gt_codes, phase_bits, ploidy_flags = self._unpack_variant_genotypes(
                    props, packed_indices
                )
                props = self._maybe_recalculate_af(props, gt_codes, ploidy_flags)

                # Check phase data availability
                if phase_bits is not None and np.any(phase_bits != 0):
                    has_phase = True

                # Count missing genotypes treated as ref
                n_missing += int(np.sum(gt_codes == 3))

                h1, h2 = gt_phase_to_haplotypes(gt_codes, phase_bits)
                hap1_list.append(h1)
                hap2_list.append(h2)
                map_lines.append(format_map_line(props))

            if not hap1_list:
                continue

            # For the first chromosome with variants, check if we have any phase data
            if not has_phase and n_variants_total == 0 and len(hap1_list) > 0:
                raise ValueError(
                    "No phase data available. Haplotype export requires phased genotypes."
                )

            n_chrom_variants = len(hap1_list)
            n_variants_total += n_chrom_variants

            # Build haplotype matrix: (n_variants, n_samples)
            hap1_matrix = (
                np.column_stack(hap1_list).T
                if hap1_list
                else np.empty((0, len(samples)), dtype=np.uint8)
            )
            hap2_matrix = (
                np.column_stack(hap2_list).T
                if hap2_list
                else np.empty((0, len(samples)), dtype=np.uint8)
            )

            # Determine file paths
            if multi_chrom:
                hap_path = Path(f"{stem}_{chrom}.hap")
                map_path = Path(f"{stem}_{chrom}.map")
            else:
                hap_path = stem.with_suffix(".hap")
                map_path = stem.with_suffix(".map")

            # Write .map
            with open(map_path, "w") as f:
                for line in map_lines:
                    f.write(line + "\n")

            # Write .hap — two rows per sample (hap1 then hap2)
            with open(hap_path, "w") as f:
                for si in range(len(samples)):
                    h1_row = hap1_matrix[:, si]
                    h2_row = hap2_matrix[:, si]
                    f.write(" ".join(str(v) for v in h1_row) + "\n")
                    f.write(" ".join(str(v) for v in h2_row) + "\n")

            files.extend([str(hap_path), str(map_path)])

        if n_missing > 0:
            logger.warning(
                "Haplotype export: %d missing genotypes treated as ref (0)",
                n_missing,
            )

        logger.info(
            "Haplotype export: %d variants, %d samples, %d chromosomes",
            n_variants_total,
            len(samples),
            len(target_chroms),
        )
        return {
            "n_variants": n_variants_total,
            "n_samples": len(samples),
            "n_missing": n_missing,
            "chromosomes": target_chroms,
            "format": "hap",
            "files": files,
        }
