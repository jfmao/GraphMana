"""BED format exporter (FAST PATH).

Exports variant positions as BED intervals for bedtools/IGV.
No genotype unpacking — reads variant node properties directly.
"""

from __future__ import annotations

import logging
from pathlib import Path

from graphmana.export.base import BaseExporter

logger = logging.getLogger(__name__)

AVAILABLE_EXTRA_COLUMNS = [
    "variant_type",
    "af_total",
    "ac_total",
    "an_total",
    "call_rate",
    "gene_symbol",
    "consequence",
    "impact",
]


class BEDExporter(BaseExporter):
    """Export variant positions as BED (FAST PATH — no genotype unpacking)."""

    def export(self, output: Path, *, extra_columns: list[str] | None = None) -> dict:
        """Export BED format.

        Args:
            output: Output file path.
            extra_columns: Additional columns beyond chr/start/end/name.

        Returns:
            Summary dict with n_variants.
        """
        extras = extra_columns or []
        target_chroms = self._get_target_chromosomes()

        n_variants = 0
        output = Path(output)
        output.parent.mkdir(parents=True, exist_ok=True)

        with open(output, "w") as f:
            for chrom in target_chroms:
                for props in self._iter_variants_fast(chrom, ordered=False):
                    pos = props.get("pos", 0)
                    start = pos - 1  # BED is 0-based half-open
                    end = pos
                    name = props.get("variantId", ".")
                    parts = [chrom, str(start), str(end), name]
                    for col in extras:
                        val = props.get(col)
                        if val is None:
                            parts.append(".")
                        elif isinstance(val, float):
                            parts.append(f"{val:.6g}")
                        else:
                            parts.append(str(val))
                    f.write("\t".join(parts) + "\n")
                    n_variants += 1

        n_samples = self._get_sample_count()
        logger.info("BED export: %d variants", n_variants)
        return {
            "n_variants": n_variants,
            "n_samples": n_samples,
            "chromosomes": target_chroms,
            "format": "bed",
        }
