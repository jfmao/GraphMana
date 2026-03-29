"""CADD score importer — loads CADD TSV into Variant.cadd_phred / cadd_raw."""

from __future__ import annotations

import gzip
import logging
from pathlib import Path
from typing import Iterator

from graphmana.annotation.parsers.base import BaseAnnotationParser
from graphmana.db.queries import UPDATE_VARIANT_CADD_BATCH

logger = logging.getLogger(__name__)


class CADDParser(BaseAnnotationParser):
    """Parse CADD TSV files and set cadd_phred / cadd_raw on Variant nodes.

    CADD TSV format (tab-separated, may be gzipped)::

        ## CADD header comments
        #Chrom  Pos  Ref  Alt  RawScore  PHRED
        1       10177  A  AC   0.234     3.456
    """

    @property
    def source_name(self) -> str:
        return "CADD"

    def _parse_file(self, input_path: Path, *, chr_prefix: str = "") -> Iterator[dict]:
        """Yield {variantId, cadd_phred, cadd_raw} dicts.

        Args:
            input_path: Path to CADD TSV (plain or gzipped).
            chr_prefix: Prefix to prepend to chromosome (e.g. 'chr').
        """
        opener = gzip.open if _is_gzipped(input_path) else open
        with opener(input_path, "rt") as f:
            for line in f:
                if line.startswith("#"):
                    continue
                parts = line.rstrip("\n").split("\t")
                if len(parts) < 6:
                    continue
                chrom, pos, ref, alt, raw_score, phred = parts[:6]
                try:
                    cadd_raw = float(raw_score)
                    cadd_phred = float(phred)
                except (ValueError, TypeError):
                    continue
                variant_id = f"{chr_prefix}{chrom}:{pos}:{ref}:{alt}"
                yield {
                    "variantId": variant_id,
                    "cadd_raw": cadd_raw,
                    "cadd_phred": cadd_phred,
                }

    def _load_batch(self, batch: list[dict]) -> int:
        with self._conn.driver.session() as session:
            result = session.run(UPDATE_VARIANT_CADD_BATCH, {"updates": batch})
            record = result.single()
            return record["matched"] if record else 0


def _is_gzipped(path: Path) -> bool:
    """Check if a file is gzipped by reading the magic bytes."""
    with open(path, "rb") as f:
        return f.read(2) == b"\x1f\x8b"
