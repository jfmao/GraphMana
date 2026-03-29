"""Beagle format exporter (FULL PATH).

Exports tab-separated Beagle genotype file with variant-major orientation.
One row per marker, two columns per sample (phased haplotype alleles).

Format:
  marker  alleleA  alleleB  sample1  sample1  sample2  sample2
  chr1_100_A_T  A  T  A  T  A  A

Allele conversion per sample (uses phase_bits for het):
  HomRef(0) -> (ref, ref)
  Het(1), phase=0 -> (ref, alt); phase=1 -> (alt, ref)
  HomAlt(2) -> (alt, alt)
  Missing(3) -> (".", ".")
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np

from graphmana.export.base import BaseExporter

logger = logging.getLogger(__name__)


def format_beagle_header(samples: list[dict]) -> str:
    """Build Beagle header line with sample IDs (each ID appears twice).

    Args:
        samples: List of sample dicts with 'sampleId' key.

    Returns:
        Tab-separated header line.
    """
    parts = ["marker", "alleleA", "alleleB"]
    for s in samples:
        sid = s["sampleId"]
        parts.append(sid)
        parts.append(sid)
    return "\t".join(parts)


def format_beagle_variant_line(
    props: dict,
    gt_codes: np.ndarray,
    phase_bits: np.ndarray,
    ref: str,
    alt: str,
) -> str:
    """Format one Beagle variant row.

    Args:
        props: Variant properties dict with variantId.
        gt_codes: int8 array of GraphMana codes (0-3) per sample.
        phase_bits: uint8 array of phase bits per sample.
        ref: Reference allele string.
        alt: Alternate allele string.

    Returns:
        Tab-separated line for this variant.
    """
    vid = props.get("variantId", ".")
    parts = [vid, ref, alt]

    for i in range(len(gt_codes)):
        gt = gt_codes[i]
        if gt == 0:
            parts.append(ref)
            parts.append(ref)
        elif gt == 1:
            if phase_bits[i]:
                # phase=1: ALT on second haplotype → REF, ALT
                parts.append(ref)
                parts.append(alt)
            else:
                # phase=0: ALT on first haplotype → ALT, REF
                parts.append(alt)
                parts.append(ref)
        elif gt == 2:
            parts.append(alt)
            parts.append(alt)
        else:
            parts.append(".")
            parts.append(".")

    return "\t".join(parts)


class BeagleExporter(BaseExporter):
    """Export to Beagle genotype format.

    Variant-major streaming export — no buffering required.
    SNPs and INDELs are both supported.
    """

    def export(self, output: Path) -> dict:
        """Export Beagle format.

        Args:
            output: Output file path.

        Returns:
            Summary dict with n_variants, n_samples, format.
        """
        output = Path(output)
        output.parent.mkdir(parents=True, exist_ok=True)

        samples = self._load_samples()
        target_chroms = self._get_target_chromosomes()
        packed_indices = np.array([s["packed_index"] for s in samples], dtype=np.int64)

        n_variants = 0

        with open(output, "w") as f:
            f.write(format_beagle_header(samples) + "\n")

            for chrom in target_chroms:
                for props in self._iter_variants(chrom):
                    ref = props.get("ref", "N")
                    alt = props.get("alt", "N")
                    gt_codes, phase_bits, ploidy_flags = self._unpack_variant_genotypes(
                        props, packed_indices
                    )
                    props = self._maybe_recalculate_af(props, gt_codes, ploidy_flags)
                    f.write(
                        format_beagle_variant_line(props, gt_codes, phase_bits, ref, alt) + "\n"
                    )
                    n_variants += 1

        logger.info(
            "Beagle export: %d variants, %d samples",
            n_variants,
            len(samples),
        )
        return {
            "n_variants": n_variants,
            "n_samples": len(samples),
            "chromosomes": target_chroms,
            "format": "beagle",
        }
