"""BED region annotator — creates RegulatoryElement nodes and IN_REGION edges."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterator

from graphmana.annotation.parsers.base import BaseAnnotationParser
from graphmana.db.queries import (
    CREATE_IN_REGION_BATCH,
    MERGE_REGULATORY_ELEMENT_BATCH,
)

# Batch variant-region overlap query: processes all regions in one UNWIND
# instead of one query per region (~1000x fewer Cypher round-trips).
FIND_VARIANTS_IN_INTERVAL_BATCH = """
UNWIND $regions AS r
MATCH (v:Variant)
WHERE v.chr = r.chr AND v.pos >= r.start AND v.pos <= r.end
RETURN v.variantId AS variantId, r.id AS regionId
"""

logger = logging.getLogger(__name__)


class BEDRegionParser(BaseAnnotationParser):
    """Parse BED files and create RegulatoryElement nodes + IN_REGION edges.

    BED format (tab-separated, 0-based half-open intervals)::

        chr1  1000  2000  enhancer_1  500  +

    Only the first 4 columns are required. Column 4 (name) becomes the
    element ID if present; otherwise a synthetic ID is generated.
    """

    def __init__(self, conn, *, region_type: str = "regulatory") -> None:
        super().__init__(conn)
        self._region_type = region_type

    @property
    def source_name(self) -> str:
        return "BED_region"

    def _parse_file(self, input_path: Path, **kwargs) -> Iterator[dict]:
        """Yield {id, type, chr, start, end, source} dicts.

        BED coordinates are 0-based half-open; we convert to 1-based
        closed intervals to match VCF/variant coordinate convention.
        """
        import gzip

        opener = gzip.open if _is_gzipped(input_path) else open
        counter = 0
        with opener(input_path, "rt") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or line.startswith("track"):
                    continue
                parts = line.split("\t")
                if len(parts) < 3:
                    continue
                chrom = parts[0]
                try:
                    # BED is 0-based half-open; convert to 1-based closed
                    start = int(parts[1]) + 1
                    end = int(parts[2])
                except (ValueError, IndexError):
                    continue
                if start > end:
                    continue
                name = parts[3] if len(parts) >= 4 else None
                counter += 1
                element_id = name if name else f"{chrom}:{start}-{end}_{counter}"
                yield {
                    "id": element_id,
                    "type": self._region_type,
                    "chr": chrom,
                    "start": start,
                    "end": end,
                    "source": str(input_path.name),
                }

    def _load_batch(self, batch: list[dict]) -> int:
        # Step 1: MERGE RegulatoryElement nodes
        with self._conn.driver.session() as session:
            session.run(MERGE_REGULATORY_ELEMENT_BATCH, {"elements": batch})

        # Step 2: Find overlapping variants and create edges in one query.
        # Uses UNWIND to process all regions in a single Cypher execution
        # instead of one query per region (1000x fewer round-trips).
        total_edges = 0
        with self._conn.driver.session() as session:
            result = session.run(
                FIND_VARIANTS_IN_INTERVAL_BATCH,
                {"regions": batch},
            )
            edge_batch = [
                {"variantId": r["variantId"], "regionId": r["regionId"]}
                for r in result
            ]

        if edge_batch:
            for i in range(0, len(edge_batch), 5000):
                sub = edge_batch[i : i + 5000]
                with self._conn.driver.session() as session:
                    session.run(CREATE_IN_REGION_BATCH, {"edges": sub})
                total_edges += len(sub)

        return total_edges


def _is_gzipped(path: Path) -> bool:
    with open(path, "rb") as f:
        return f.read(2) == b"\x1f\x8b"
