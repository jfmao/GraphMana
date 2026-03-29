"""TSV variant table exporter (FAST PATH).

Exports variant properties as tab-separated values without unpacking
per-sample genotypes. Reads variant node properties directly.
"""

from __future__ import annotations

import logging
from pathlib import Path

from graphmana.export.base import BaseExporter

logger = logging.getLogger(__name__)

DEFAULT_COLUMNS = [
    "variantId",
    "chr",
    "pos",
    "ref",
    "alt",
    "variant_type",
    "af_total",
]

AVAILABLE_COLUMNS = [
    "variantId",
    "chr",
    "pos",
    "ref",
    "alt",
    "variant_type",
    "ac_total",
    "an_total",
    "af_total",
    "call_rate",
    "ancestral_allele",
    "is_polarized",
    "qual",
    "filter",
]


class TSVExporter(BaseExporter):
    """Export variant properties as TSV (FAST PATH — no genotype unpacking)."""

    def export(self, output: Path, *, columns: list[str] | None = None) -> dict:
        """Export TSV format.

        Args:
            output: Output file path.
            columns: List of column names to include. Defaults to DEFAULT_COLUMNS.

        Returns:
            Summary dict with n_variants, columns.
        """
        if self._threads > 1:
            return self._export_parallel(output, columns=columns)

        cols = columns if columns else list(DEFAULT_COLUMNS)
        target_chroms = self._get_target_chromosomes()

        n_variants = 0
        output = Path(output)
        output.parent.mkdir(parents=True, exist_ok=True)

        with open(output, "w") as f:
            f.write("\t".join(cols) + "\n")

            for chrom in target_chroms:
                for props in self._iter_variants(chrom):
                    values = []
                    for col in cols:
                        val = props.get(col)
                        if val is None:
                            values.append(".")
                        elif isinstance(val, float):
                            values.append(f"{val:.6g}")
                        else:
                            values.append(str(val))
                    f.write("\t".join(values) + "\n")
                    n_variants += 1

        n_samples = self._get_sample_count()
        logger.info("TSV export: %d variants, %d columns", n_variants, len(cols))
        return {
            "n_variants": n_variants,
            "n_samples": n_samples,
            "columns": cols,
            "chromosomes": target_chroms,
            "format": "tsv",
        }

    def _export_parallel(self, output: Path, *, columns: list[str] | None = None) -> dict:
        """Parallel TSV export: header once, data lines per chromosome."""
        from graphmana.export.parallel import run_export_parallel

        cols = columns if columns else list(DEFAULT_COLUMNS)
        target_chroms = self._get_target_chromosomes()

        def write_header(out_path, conn):
            out_path = Path(out_path)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            with open(out_path, "w") as f:
                f.write("\t".join(cols) + "\n")

        summary = run_export_parallel(
            TSVExporter,
            self._conn,
            threads=self._threads,
            output=Path(output),
            filter_config=self._filter_config,
            target_chroms=target_chroms,
            export_kwargs={"columns": cols},
            header_writer=write_header,
            recalculate_af=self._recalculate_af,
        )
        summary["columns"] = cols
        summary["format"] = "tsv"
        return summary
