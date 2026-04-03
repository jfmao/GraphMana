"""dadi SFS format exporter (FAST PATH).

Exports site frequency spectrum in dadi .fs format.
Reads pre-computed ac[], an[] arrays directly — no genotype unpacking.
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np

from graphmana.export.base import BaseExporter
from graphmana.export.sfs_utils import (
    fold_sfs,
    fold_sfs_2d,
    hypergeometric_projection,
)

logger = logging.getLogger(__name__)


def _resolve_pop_indices(pop_ids: list[str], target_pops: list[str]) -> list[int]:
    """Find indices of target populations in pop_ids array."""
    indices = []
    for pop in target_pops:
        try:
            indices.append(pop_ids.index(pop))
        except ValueError:
            raise ValueError(f"Population {pop!r} not found in variant pop_ids: {pop_ids}")
    return indices


class SFSDadiExporter(BaseExporter):
    """Export SFS in dadi .fs format (FAST PATH — no genotype unpacking)."""

    def export(
        self,
        output: Path,
        *,
        populations: list[str],
        projection: list[int],
        polarized: bool = True,
        include_monomorphic: bool = False,
    ) -> dict:
        """Export dadi SFS format.

        Args:
            output: Output file path.
            populations: Target populations (1-3).
            projection: Projection sizes per population.
            polarized: If True, unfolded (derived allele) SFS.
            include_monomorphic: If True, include monomorphic sites in the SFS.

        Returns:
            Summary dict.
        """
        if len(populations) != len(projection):
            raise ValueError(
                f"Number of populations ({len(populations)}) must match "
                f"number of projections ({len(projection)})"
            )
        if not 1 <= len(populations) <= 3:
            raise ValueError("dadi SFS supports 1-3 populations")

        target_chroms = self._get_target_chromosomes()
        n_pops = len(populations)

        # Initialize SFS array — accumulate directly while streaming variants
        # to avoid storing 70M+ variant dicts in memory.
        if n_pops == 1:
            sfs = np.zeros(projection[0] + 1, dtype=np.float64)
        elif n_pops == 2:
            sfs = np.zeros((projection[0] + 1, projection[1] + 1), dtype=np.float64)
        else:
            raise NotImplementedError("3-population SFS not yet implemented")

        pop_ids: list[str] | None = None
        pop_indices: list[int] | None = None
        n_used = 0

        for chrom in target_chroms:
            for props in self._iter_variants_fast(chrom, ordered=False):
                var_pop_ids = props.get("pop_ids")
                if var_pop_ids is None:
                    continue
                if pop_ids is None:
                    pop_ids = list(var_pop_ids)
                    pop_indices = _resolve_pop_indices(pop_ids, populations)

                # Skip monomorphic sites unless requested
                if not include_monomorphic:
                    ac_total = props.get("ac_total", 0)
                    an_total = props.get("an_total", 0)
                    if ac_total == 0 or ac_total == an_total:
                        continue

                ac_arr = props.get("ac")
                an_arr = props.get("an")
                if ac_arr is None or an_arr is None:
                    continue

                # Accumulate into SFS directly
                if n_pops == 1:
                    idx = pop_indices[0]
                    if idx >= len(ac_arr) or idx >= len(an_arr):
                        continue
                    ac_val, an_val = int(ac_arr[idx]), int(an_arr[idx])
                    if an_val < projection[0]:
                        continue
                    if polarized and not props.get("is_polarized", False):
                        continue
                    sfs += hypergeometric_projection(ac_val, an_val, projection[0])
                else:  # n_pops == 2
                    i1, i2 = pop_indices[0], pop_indices[1]
                    if i1 >= len(ac_arr) or i2 >= len(ac_arr):
                        continue
                    if i1 >= len(an_arr) or i2 >= len(an_arr):
                        continue
                    ac1, an1 = int(ac_arr[i1]), int(an_arr[i1])
                    ac2, an2 = int(ac_arr[i2]), int(an_arr[i2])
                    if an1 < projection[0] or an2 < projection[1]:
                        continue
                    if polarized and not props.get("is_polarized", False):
                        continue
                    p1 = hypergeometric_projection(ac1, an1, projection[0])
                    p2 = hypergeometric_projection(ac2, an2, projection[1])
                    sfs += np.outer(p1, p2)

                n_used += 1

        if pop_ids is None:
            raise ValueError("No variants with population data found")

        # Fold if not polarized
        if not polarized:
            sfs = fold_sfs(sfs) if n_pops == 1 else fold_sfs_2d(sfs)

        # Write dadi .fs format
        output = Path(output)
        output.parent.mkdir(parents=True, exist_ok=True)

        with open(output, "w") as f:
            f.write(
                f"# {n_used} SNPs; populations: {' '.join(populations)}; "
                f"projections: {' '.join(str(p) for p in projection)}\n"
            )
            dims = [str(p + 1) for p in projection]
            f.write(" ".join(dims) + "\n")
            flat = sfs.flatten()
            f.write(" ".join(f"{v:.6f}" for v in flat) + "\n")
            mask = np.zeros_like(flat, dtype=int)
            if not polarized:
                mask[0] = 1
                mask[-1] = 1
            f.write(" ".join(str(int(m)) for m in mask) + "\n")

        logger.info(
            "dadi SFS export: %d variants, populations=%s, projection=%s",
            n_used,
            populations,
            projection,
        )
        return {
            "n_variants": n_used,
            "n_samples": self._get_sample_count(),
            "populations": populations,
            "projection": projection,
            "polarized": polarized,
            "chromosomes": target_chroms,
            "format": "sfs-dadi",
        }
