"""TreeMix allele count matrix exporter (FAST PATH).

Paper showcase format — exports in seconds at any sample count.
Reads pre-computed pop_ids[], ac[], an[] arrays directly from Variant
nodes. No genotype unpacking.
"""

from __future__ import annotations

import gzip
import logging
from pathlib import Path

from graphmana.export.base import BaseExporter

logger = logging.getLogger(__name__)


class TreeMixExporter(BaseExporter):
    """Export TreeMix allele count matrix (FAST PATH — no genotype unpacking)."""

    def export(self, output: Path) -> dict:
        """Export TreeMix format.

        Args:
            output: Output file path (will be gzipped).

        Returns:
            Summary dict with n_variants, populations.
        """
        target_chroms = self._get_target_chromosomes()
        pop_filter = (
            set(self._filter_config.populations) if self._filter_config.populations else None
        )

        n_variants = 0
        pop_names: list[str] | None = None
        pop_indices: list[int] | None = None

        output = Path(output)
        output.parent.mkdir(parents=True, exist_ok=True)
        # Ensure .gz extension
        out_path = output if str(output).endswith(".gz") else Path(str(output) + ".gz")

        with gzip.open(out_path, "wt") as f:
            for chrom in target_chroms:
                for props in self._iter_variants(chrom):
                    var_pop_ids = props.get("pop_ids")
                    ac = props.get("ac")
                    an = props.get("an")

                    if var_pop_ids is None or ac is None or an is None:
                        continue

                    an_total = props.get("an_total", 0)
                    if an_total == 0:
                        continue

                    # Resolve population indices on first variant
                    if pop_names is None:
                        if pop_filter:
                            pop_indices = [i for i, p in enumerate(var_pop_ids) if p in pop_filter]
                            pop_names = [var_pop_ids[i] for i in pop_indices]
                        else:
                            pop_indices = list(range(len(var_pop_ids)))
                            pop_names = list(var_pop_ids)

                        if not pop_names:
                            logger.warning("No matching populations found")
                            return {
                                "n_variants": 0,
                                "populations": [],
                                "format": "treemix",
                            }
                        f.write(" ".join(pop_names) + "\n")

                    # Write ac,an pairs for each population
                    pairs = []
                    for idx in pop_indices:
                        if idx < len(ac) and idx < len(an):
                            pairs.append(f"{ac[idx]},{an[idx]}")
                        else:
                            pairs.append("0,0")
                    f.write(" ".join(pairs) + "\n")
                    n_variants += 1

        if pop_names is None:
            pop_names = []

        n_samples = self._get_sample_count()
        logger.info("TreeMix export: %d variants, %d populations", n_variants, len(pop_names))
        return {
            "n_variants": n_variants,
            "n_samples": n_samples,
            "populations": pop_names,
            "chromosomes": target_chroms,
            "format": "treemix",
        }
