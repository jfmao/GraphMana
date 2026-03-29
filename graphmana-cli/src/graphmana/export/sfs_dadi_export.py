"""dadi SFS format exporter (FAST PATH).

Exports site frequency spectrum in dadi .fs format.
Reads pre-computed ac[], an[] arrays directly — no genotype unpacking.
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np

from graphmana.export.base import BaseExporter
from graphmana.export.sfs_utils import build_sfs_1d, build_sfs_2d

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

        # Collect all variants (FAST PATH — only reads pop arrays, not genotypes)
        all_variants: list[dict] = []
        pop_ids: list[str] | None = None
        for chrom in target_chroms:
            for props in self._iter_variants(chrom):
                var_pop_ids = props.get("pop_ids")
                if var_pop_ids is None:
                    continue
                if pop_ids is None:
                    pop_ids = list(var_pop_ids)
                # Skip monomorphic sites unless requested
                if not include_monomorphic:
                    ac_total = props.get("ac_total", 0)
                    an_total = props.get("an_total", 0)
                    if ac_total == 0 or ac_total == an_total:
                        continue
                all_variants.append(props)

        if pop_ids is None:
            raise ValueError("No variants with population data found")

        pop_indices = _resolve_pop_indices(pop_ids, populations)

        # Build SFS
        if len(populations) == 1:
            sfs = build_sfs_1d(all_variants, pop_indices[0], projection[0], polarized=polarized)
        elif len(populations) == 2:
            sfs = build_sfs_2d(
                all_variants,
                pop_indices[0],
                pop_indices[1],
                projection[0],
                projection[1],
                polarized=polarized,
            )
        else:
            # 3-pop: build as nested outer products
            raise NotImplementedError("3-population SFS not yet implemented")

        # Write dadi .fs format
        output = Path(output)
        output.parent.mkdir(parents=True, exist_ok=True)

        with open(output, "w") as f:
            n_used = len(all_variants)
            f.write(
                f"# {n_used} SNPs; populations: {' '.join(populations)}; "
                f"projections: {' '.join(str(p) for p in projection)}\n"
            )

            # Dimensions line
            dims = [str(p + 1) for p in projection]
            f.write(" ".join(dims) + "\n")

            # Flattened SFS counts
            flat = sfs.flatten()
            f.write(" ".join(f"{v:.6f}" for v in flat) + "\n")

            # Mask line: mask bin 0 and last bin for folded SFS
            mask = np.zeros_like(flat, dtype=int)
            if not polarized:
                mask[0] = 1
                mask[-1] = 1
            f.write(" ".join(str(int(m)) for m in mask) + "\n")

        logger.info(
            "dadi SFS export: %d variants, populations=%s, projection=%s",
            len(all_variants),
            populations,
            projection,
        )
        return {
            "n_variants": len(all_variants),
            "n_samples": self._get_sample_count(),
            "populations": populations,
            "projection": projection,
            "polarized": polarized,
            "chromosomes": target_chroms,
            "format": "sfs-dadi",
        }
