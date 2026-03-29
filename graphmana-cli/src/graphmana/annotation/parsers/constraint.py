"""gnomAD gene constraint importer — loads pLI/LOEUF/mis_z/syn_z onto Gene nodes."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterator

from graphmana.annotation.parsers.base import BaseAnnotationParser
from graphmana.db.queries import UPDATE_GENE_CONSTRAINT_BATCH

logger = logging.getLogger(__name__)


class GeneConstraintParser(BaseAnnotationParser):
    """Parse gnomAD constraint TSV and set constraint metrics on Gene nodes.

    Expected TSV columns (tab-separated, header required)::

        gene  transcript  obs_mis  exp_mis  oe_mis  ...  pLI  oe_lof_upper  mis_z  syn_z

    Only ``gene``, ``pLI``, ``oe_lof_upper``, ``mis_z``, and ``syn_z``
    are used. Genes are matched by symbol (Gene nodes must already exist
    from a prior VEP/SnpEff annotation import).
    """

    @property
    def source_name(self) -> str:
        return "gnomAD_constraint"

    def _parse_file(self, input_path: Path, **kwargs) -> Iterator[dict]:
        """Yield {symbol, pli, loeuf, mis_z, syn_z} dicts."""
        import gzip

        opener = gzip.open if _is_gzipped(input_path) else open
        with opener(input_path, "rt") as f:
            header = None
            for line in f:
                if line.startswith("#"):
                    continue
                parts = line.rstrip("\n").split("\t")
                if header is None:
                    header = parts
                    continue
                if len(parts) < len(header):
                    continue
                row = dict(zip(header, parts))
                symbol = row.get("gene", "").strip()
                if not symbol:
                    continue
                yield {
                    "symbol": symbol,
                    "pli": _safe_float(row.get("pLI")),
                    "loeuf": _safe_float(row.get("oe_lof_upper")),
                    "mis_z": _safe_float(row.get("mis_z")),
                    "syn_z": _safe_float(row.get("syn_z")),
                }

    def _load_batch(self, batch: list[dict]) -> int:
        with self._conn.driver.session() as session:
            result = session.run(UPDATE_GENE_CONSTRAINT_BATCH, {"updates": batch})
            record = result.single()
            return record["matched"] if record else 0


def _safe_float(value: str | None) -> float | None:
    """Convert string to float, returning None for empty/NA/invalid values."""
    if not value or value.strip() in ("", "NA", "nan", "NaN", "."):
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _is_gzipped(path: Path) -> bool:
    """Check if a file is gzipped by reading the magic bytes."""
    with open(path, "rb") as f:
        return f.read(2) == b"\x1f\x8b"
