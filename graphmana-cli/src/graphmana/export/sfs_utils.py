"""Shared SFS (Site Frequency Spectrum) computation utilities.

Used by both dadi and fastsimcoal2 exporters. Implements hypergeometric
projection for handling variable missing data across variants.
"""

from __future__ import annotations

from math import comb

import numpy as np


def hypergeometric_projection(ac: int, an: int, proj: int) -> np.ndarray:
    """Project allele count from an alleles to proj alleles.

    Uses the hypergeometric distribution to down-sample observed allele
    counts to a fixed projection size. This handles variable missing data
    across variants by projecting each variant to the same sample size.

    Args:
        ac: Observed derived/alt allele count.
        an: Observed total alleles (2*n_called for diploid).
        proj: Target projection size (number of alleles).

    Returns:
        Array of length proj+1 with expected count contribution per bin.
        sfs[k] = P(k derived alleles in sample of proj | ac in total an).
    """
    if an < proj or an == 0:
        return np.zeros(proj + 1, dtype=np.float64)

    result = np.zeros(proj + 1, dtype=np.float64)
    denom = comb(an, proj)
    if denom == 0:
        return result

    ref_count = an - ac
    for k in range(proj + 1):
        if k > ac or (proj - k) > ref_count:
            continue
        num = comb(ac, k) * comb(ref_count, proj - k)
        result[k] = num / denom

    return result


def build_sfs_1d(
    variants: list[dict],
    pop_idx: int,
    projection: int,
    *,
    polarized: bool = True,
) -> np.ndarray:
    """Build a 1D SFS from variant property dicts.

    Args:
        variants: List of variant property dicts with ac[], an[], is_polarized.
        pop_idx: Index into pop_ids[]/ac[]/an[] for the target population.
        projection: Projection size (number of alleles).
        polarized: If True, use derived allele counts (skip unpolarized).
            If False, fold the SFS.

    Returns:
        1D array of length projection+1 with SFS counts.
    """
    sfs = np.zeros(projection + 1, dtype=np.float64)

    for props in variants:
        ac_arr = props.get("ac")
        an_arr = props.get("an")
        if ac_arr is None or an_arr is None:
            continue
        if pop_idx >= len(ac_arr) or pop_idx >= len(an_arr):
            continue

        ac_val = int(ac_arr[pop_idx])
        an_val = int(an_arr[pop_idx])

        if an_val < projection:
            continue  # not enough data to project

        if polarized and not props.get("is_polarized", False):
            continue

        proj = hypergeometric_projection(ac_val, an_val, projection)
        sfs += proj

    if not polarized:
        sfs = fold_sfs(sfs)

    return sfs


def build_sfs_2d(
    variants: list[dict],
    pop_idx1: int,
    pop_idx2: int,
    proj1: int,
    proj2: int,
    *,
    polarized: bool = True,
) -> np.ndarray:
    """Build a 2D joint SFS from variant property dicts.

    Args:
        variants: List of variant property dicts.
        pop_idx1: Index for first population.
        pop_idx2: Index for second population.
        proj1: Projection size for first population.
        proj2: Projection size for second population.
        polarized: If True, use derived allele counts.

    Returns:
        2D array of shape (proj1+1, proj2+1) with joint SFS counts.
    """
    sfs = np.zeros((proj1 + 1, proj2 + 1), dtype=np.float64)

    for props in variants:
        ac_arr = props.get("ac")
        an_arr = props.get("an")
        if ac_arr is None or an_arr is None:
            continue
        if pop_idx1 >= len(ac_arr) or pop_idx2 >= len(ac_arr):
            continue
        if pop_idx1 >= len(an_arr) or pop_idx2 >= len(an_arr):
            continue

        ac1 = int(ac_arr[pop_idx1])
        an1 = int(an_arr[pop_idx1])
        ac2 = int(ac_arr[pop_idx2])
        an2 = int(an_arr[pop_idx2])

        if an1 < proj1 or an2 < proj2:
            continue

        if polarized and not props.get("is_polarized", False):
            continue

        proj_1 = hypergeometric_projection(ac1, an1, proj1)
        proj_2 = hypergeometric_projection(ac2, an2, proj2)
        sfs += np.outer(proj_1, proj_2)

    if not polarized:
        sfs = fold_sfs_2d(sfs)

    return sfs


def fold_sfs(sfs: np.ndarray) -> np.ndarray:
    """Fold an unfolded 1D SFS to minor allele frequency spectrum.

    Combines bins k and n-k. The middle bin (if n is even) stays unchanged.

    Args:
        sfs: Unfolded 1D SFS of length n+1.

    Returns:
        Folded SFS of the same length (upper bins zeroed).
    """
    n = len(sfs) - 1
    folded = np.zeros_like(sfs)
    for k in range(n + 1):
        j = min(k, n - k)
        folded[j] += sfs[k]
    return folded


def fold_sfs_2d(sfs: np.ndarray) -> np.ndarray:
    """Fold an unfolded 2D joint SFS.

    For 2D, folding maps (i, j) -> (n1-i, n2-j) when i+j > (n1+n2)/2.

    Args:
        sfs: Unfolded 2D SFS of shape (n1+1, n2+1).

    Returns:
        Folded 2D SFS.
    """
    n1 = sfs.shape[0] - 1
    n2 = sfs.shape[1] - 1
    total = n1 + n2
    folded = np.zeros_like(sfs)
    for i in range(n1 + 1):
        for j in range(n2 + 1):
            if i + j <= total / 2:
                folded[i, j] += sfs[i, j]
            else:
                folded[n1 - i, n2 - j] += sfs[i, j]
    return folded
