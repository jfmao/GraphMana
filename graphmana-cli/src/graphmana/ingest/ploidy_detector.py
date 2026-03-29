"""Detect per-sample ploidy from cyvcf2 variant genotype lists."""

from __future__ import annotations

import numpy as np


def detect_ploidy(variant) -> tuple[str, np.ndarray]:
    """Detect per-sample ploidy from a cyvcf2 Variant.

    Examines genotype list element lengths: 3-element = diploid ([a0, a1, phased]),
    2-element = haploid ([a0, phased]).

    Args:
        variant: a cyvcf2 Variant object.

    Returns:
        (mode, haploid_flags) where mode is 'all_diploid', 'all_haploid',
        or 'mixed'. haploid_flags is a boolean array (True = haploid).
    """
    gts = variant.genotypes
    n = len(gts)
    haploid_flags = np.zeros(n, dtype=bool)
    for i, g in enumerate(gts):
        if len(g) == 2:
            haploid_flags[i] = True
    n_hap = int(haploid_flags.sum())
    if n_hap == 0:
        return "all_diploid", haploid_flags
    if n_hap == n:
        return "all_haploid", haploid_flags
    return "mixed", haploid_flags
