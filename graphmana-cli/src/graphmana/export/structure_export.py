"""STRUCTURE format exporter (FULL PATH).

Exports sample-major genotype data for the STRUCTURE program.

Allele coding: 1=REF, 2=ALT, -9=missing.

Two output formats:
  onerow: One row per sample. sampleId popIndex allele1_L1 allele2_L1 ...
  tworow: Two rows per sample. Each row is one haplotype across all loci.

Because STRUCTURE is sample-major, all variant genotypes are buffered
into a numpy matrix before writing.
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np

from graphmana.export.base import BaseExporter

logger = logging.getLogger(__name__)

# Remap: GraphMana 0(HomRef)->1(REF), 1(Het)->special, 2(HomAlt)->2(ALT), 3(Missing)->-9
# Het alleles depend on phase; handled in gt_to_structure_alleles.


def gt_to_structure_alleles(
    gt_codes: np.ndarray,
    phase_bits: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    """Convert genotype codes to STRUCTURE allele pairs.

    Args:
        gt_codes: int8 array of GraphMana codes (0-3) for one variant.
        phase_bits: uint8 array of phase bits for one variant.

    Returns:
        (allele1, allele2): int16 arrays where 1=REF, 2=ALT, -9=missing.
    """
    n = len(gt_codes)
    a1 = np.full(n, -9, dtype=np.int16)
    a2 = np.full(n, -9, dtype=np.int16)

    homref = gt_codes == 0
    a1[homref] = 1
    a2[homref] = 1

    het = gt_codes == 1
    het_phased = het & (phase_bits == 1)
    het_unphased = het & (phase_bits == 0)
    # phase=1: ALT on second haplotype → a1=REF(1), a2=ALT(2)
    a1[het_phased] = 1
    a2[het_phased] = 2
    # phase=0: ALT on first haplotype → a1=ALT(2), a2=REF(1)
    a1[het_unphased] = 2
    a2[het_unphased] = 1

    homalt = gt_codes == 2
    a1[homalt] = 2
    a2[homalt] = 2

    # Missing (gt_codes == 3) stays -9
    return a1, a2


def format_structure_sample_onerow(
    sid: str,
    pop_idx: int,
    allele_pairs: np.ndarray,
) -> str:
    """Format one STRUCTURE onerow line.

    Args:
        sid: Sample ID.
        pop_idx: Population index (1-based).
        allele_pairs: int16 array of shape (n_loci, 2).

    Returns:
        Formatted line: sampleId popIndex a1_L1 a2_L1 a1_L2 a2_L2 ...
    """
    parts = [sid, str(pop_idx)]
    for locus in range(allele_pairs.shape[0]):
        parts.append(str(allele_pairs[locus, 0]))
        parts.append(str(allele_pairs[locus, 1]))
    return "\t".join(parts)


def format_structure_sample_tworow(
    sid: str,
    pop_idx: int,
    hap: np.ndarray,
) -> str:
    """Format one STRUCTURE tworow line (one haplotype).

    Args:
        sid: Sample ID.
        pop_idx: Population index (1-based).
        hap: int16 array of shape (n_loci,) — one haplotype.

    Returns:
        Formatted line: sampleId popIndex allele_L1 allele_L2 ...
    """
    parts = [sid, str(pop_idx)]
    parts.extend(str(v) for v in hap)
    return "\t".join(parts)


class STRUCTUREExporter(BaseExporter):
    """Export to STRUCTURE format.

    Sample-major format — buffers all variant genotypes before writing.
    """

    def export(
        self,
        output: Path,
        *,
        output_format: str = "onerow",
    ) -> dict:
        """Export STRUCTURE format.

        Args:
            output: Output file path.
            output_format: "onerow" or "tworow".

        Returns:
            Summary dict with n_variants, n_samples, format.
        """
        if output_format not in ("onerow", "tworow"):
            raise ValueError(
                f"Invalid structure format: {output_format!r}. Use 'onerow' or 'tworow'."
            )

        output = Path(output)
        output.parent.mkdir(parents=True, exist_ok=True)

        samples = self._load_samples()
        target_chroms = self._get_target_chromosomes()
        packed_indices = np.array([s["packed_index"] for s in samples], dtype=np.int64)

        # Build population index mapping (1-based)
        pop_names = []
        pop_index_map: dict[str, int] = {}
        for s in samples:
            pop = s.get("population", "Unknown")
            if pop not in pop_index_map:
                pop_index_map[pop] = len(pop_names) + 1
                pop_names.append(pop)

        # Buffer all variant genotypes
        gt_list: list[np.ndarray] = []
        phase_list: list[np.ndarray] = []

        for chrom in target_chroms:
            for props in self._iter_variants(chrom):
                gt_codes, phase_bits, ploidy_flags = self._unpack_variant_genotypes(
                    props, packed_indices
                )
                props = self._maybe_recalculate_af(props, gt_codes, ploidy_flags)
                gt_list.append(gt_codes)
                phase_list.append(phase_bits)

        n_variants = len(gt_list)
        n_samples = len(samples)

        if n_variants == 0:
            with open(output, "w") as f:
                pass
            logger.info("STRUCTURE export: 0 variants, %d samples", n_samples)
            return {
                "n_variants": 0,
                "n_samples": n_samples,
                "chromosomes": target_chroms,
                "format": "structure",
                "output_format": output_format,
            }

        # Convert to allele matrices: (n_variants, n_samples, 2)
        allele_matrix = np.full((n_variants, n_samples, 2), -9, dtype=np.int16)
        for v in range(n_variants):
            a1, a2 = gt_to_structure_alleles(gt_list[v], phase_list[v])
            allele_matrix[v, :, 0] = a1
            allele_matrix[v, :, 1] = a2

        # Write sample-by-sample
        with open(output, "w") as f:
            for si in range(n_samples):
                sid = samples[si]["sampleId"]
                pop = samples[si].get("population", "Unknown")
                pop_idx = pop_index_map[pop]
                sample_alleles = allele_matrix[:, si, :]  # (n_variants, 2)

                if output_format == "onerow":
                    f.write(format_structure_sample_onerow(sid, pop_idx, sample_alleles) + "\n")
                else:
                    hap1 = sample_alleles[:, 0]
                    hap2 = sample_alleles[:, 1]
                    f.write(format_structure_sample_tworow(sid, pop_idx, hap1) + "\n")
                    f.write(format_structure_sample_tworow(sid, pop_idx, hap2) + "\n")

        logger.info(
            "STRUCTURE export (%s): %d variants, %d samples",
            output_format,
            n_variants,
            n_samples,
        )
        return {
            "n_variants": n_variants,
            "n_samples": n_samples,
            "chromosomes": target_chroms,
            "format": "structure",
            "output_format": output_format,
            "populations": pop_names,
        }
