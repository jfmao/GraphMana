"""Genepop format exporter (FULL PATH).

Exports sample-major genotype data for the Genepop program.
Uses 6-digit genotype codes (3-digit allele pairs).

Format:
  GraphMana Genepop Export
  Locus_1
  Locus_2
  Pop
  sample1 ,  001001 001002
  sample2 ,  002002 000000
  Pop
  sample3 ,  001002 001001

Genotype coding:
  001001 = HomRef (allele 001 / allele 001)
  001002 = Het (allele 001 / allele 002)
  002002 = HomAlt (allele 002 / allele 002)
  000000 = Missing

Biallelic variants only. SNPs and INDELs are both supported
(allele codes are abstract).
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np

from graphmana.export.base import BaseExporter

logger = logging.getLogger(__name__)

# Lookup: GraphMana gt code -> 6-digit Genepop string
_GENEPOP_CODES = ("001001", "001002", "002002", "000000")


def gt_to_genepop_code(gt_code: int) -> str:
    """Convert a single GraphMana genotype code to a 6-digit Genepop string.

    Args:
        gt_code: 0=HomRef, 1=Het, 2=HomAlt, 3=Missing.

    Returns:
        6-digit string (e.g. "001001", "000000").
    """
    return _GENEPOP_CODES[gt_code]


def gt_to_genepop_codes(gt_codes: np.ndarray) -> list[str]:
    """Convert an array of genotype codes to Genepop strings.

    Args:
        gt_codes: int8 array of GraphMana codes (0-3) for one sample
            across all variants.

    Returns:
        List of 6-digit Genepop strings.
    """
    return [_GENEPOP_CODES[g] for g in gt_codes]


def format_genepop_locus_name(props: dict) -> str:
    """Format a locus name from variant properties.

    Args:
        props: Variant properties dict.

    Returns:
        Locus name string.
    """
    return props.get("variantId", ".")


class GenepopExporter(BaseExporter):
    """Export to Genepop format.

    Sample-major format with population blocks — buffers all variant
    genotypes before writing.
    """

    def export(self, output: Path) -> dict:
        """Export Genepop format.

        Args:
            output: Output file path.

        Returns:
            Summary dict with n_variants, n_samples, format, populations.
        """
        output = Path(output)
        output.parent.mkdir(parents=True, exist_ok=True)

        samples = self._load_samples()
        target_chroms = self._get_target_chromosomes()
        packed_indices = np.array([s["packed_index"] for s in samples], dtype=np.int64)

        # Buffer all variant genotypes and locus names
        gt_list: list[np.ndarray] = []
        locus_names: list[str] = []

        for chrom in target_chroms:
            for props in self._iter_variants(chrom):
                gt_codes, _, ploidy_flags = self._unpack_variant_genotypes(
                    props, packed_indices
                )
                props = self._maybe_recalculate_af(props, gt_codes, ploidy_flags)
                gt_list.append(gt_codes)
                locus_names.append(format_genepop_locus_name(props))

        n_variants = len(gt_list)
        n_samples = len(samples)

        # Build genotype matrix (n_variants, n_samples) for efficient sample access
        if n_variants > 0:
            gt_matrix = np.column_stack(gt_list).T  # (n_variants, n_samples)
        else:
            gt_matrix = np.empty((0, n_samples), dtype=np.int8)

        # Group samples by population preserving order
        pop_order: list[str] = []
        pop_samples: dict[str, list[int]] = {}
        for si, s in enumerate(samples):
            pop = s.get("population", "Unknown")
            if pop not in pop_samples:
                pop_order.append(pop)
                pop_samples[pop] = []
            pop_samples[pop].append(si)

        # Write
        with open(output, "w") as f:
            f.write("GraphMana Genepop Export\n")
            for name in locus_names:
                f.write(name + "\n")

            for pop in pop_order:
                f.write("Pop\n")
                for si in pop_samples[pop]:
                    sid = samples[si]["sampleId"]
                    if n_variants > 0:
                        sample_gts = gt_matrix[:, si]
                        codes = gt_to_genepop_codes(sample_gts)
                        f.write(f"{sid} ,  {' '.join(codes)}\n")
                    else:
                        f.write(f"{sid} ,\n")

        logger.info(
            "Genepop export: %d variants, %d samples, %d populations",
            n_variants,
            n_samples,
            len(pop_order),
        )
        return {
            "n_variants": n_variants,
            "n_samples": n_samples,
            "chromosomes": target_chroms,
            "format": "genepop",
            "populations": pop_order,
        }
