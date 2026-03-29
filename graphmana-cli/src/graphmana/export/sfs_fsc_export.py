"""fastsimcoal2 SFS format exporter (FAST PATH).

Exports site frequency spectrum in fastsimcoal2 .obs format.
Same SFS computation as dadi, different file formatting.
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


class SFSFscExporter(BaseExporter):
    """Export SFS in fastsimcoal2 .obs format (FAST PATH — no genotype unpacking)."""

    def export(
        self,
        output: Path,
        *,
        populations: list[str],
        projection: list[int],
        polarized: bool = True,
        include_monomorphic: bool = False,
    ) -> dict:
        """Export fastsimcoal2 SFS format.

        Args:
            output: Output file path.
            populations: Target populations (1-2).
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
        if not 1 <= len(populations) <= 2:
            raise ValueError("fastsimcoal2 SFS supports 1-2 populations")

        target_chroms = self._get_target_chromosomes()

        # Collect all variants (FAST PATH)
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
        else:
            sfs = build_sfs_2d(
                all_variants,
                pop_indices[0],
                pop_indices[1],
                projection[0],
                projection[1],
                polarized=polarized,
            )

        # Write fsc .obs format
        output = Path(output)
        output.parent.mkdir(parents=True, exist_ok=True)

        with open(output, "w") as f:
            f.write("1 observations\n")

            if len(populations) == 1:
                self._write_1d(f, sfs, populations[0], projection[0])
            else:
                self._write_2d(f, sfs, populations, projection)

        logger.info(
            "fsc SFS export: %d variants, populations=%s, projection=%s",
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
            "format": "sfs-fsc",
        }

    @staticmethod
    def _write_1d(f, sfs: np.ndarray, pop_name: str, proj: int) -> None:
        """Write 1-population fsc format."""
        # Header: d0_0 d0_1 ... d0_n
        headers = [f"d0_{i}" for i in range(proj + 1)]
        f.write("\t".join(headers) + "\n")
        # Counts
        f.write("\t".join(f"{v:.6f}" for v in sfs) + "\n")

    @staticmethod
    def _write_2d(
        f,
        sfs: np.ndarray,
        populations: list[str],
        projection: list[int],
    ) -> None:
        """Write 2-population joint fsc format."""
        n1, n2 = projection[0], projection[1]
        # Header line: npops n1 n2
        f.write(f"{len(populations)}\t{n1}\t{n2}\n")
        # Column headers: d1_0 d1_1 ... d1_n2
        headers = [f"d1_{j}" for j in range(n2 + 1)]
        f.write("\t".join(headers) + "\n")
        # Rows: one per d0 bin
        for i in range(n1 + 1):
            f.write("\t".join(f"{sfs[i, j]:.6f}" for j in range(n2 + 1)) + "\n")
