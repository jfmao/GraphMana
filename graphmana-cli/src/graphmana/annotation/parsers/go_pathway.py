"""GO term and pathway importers — creates GOTerm/Pathway nodes and edges to Gene."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterator

from graphmana.annotation.parsers.base import BaseAnnotationParser
from graphmana.db.queries import (
    CREATE_GO_HIERARCHY_BATCH,
    CREATE_HAS_GO_TERM_BATCH,
    CREATE_IN_PATHWAY_BATCH,
    MERGE_GOTERM_BATCH,
    MERGE_PATHWAY_BATCH,
)

logger = logging.getLogger(__name__)


class GOParser(BaseAnnotationParser):
    """Parse GAF (Gene Association Format) files and create GOTerm nodes + edges.

    GAF 2.2 format (tab-separated, ``!`` comment lines)::

        !gaf-version: 2.2
        UniProtKB  A0A123  GENE1  ...  GO:0003674  ...  F  ...

    Columns used:
        - 2: DB Object Symbol (gene symbol)
        - 4: GO ID (e.g. GO:0003674)
        - 8: Aspect (F=molecular_function, P=biological_process, C=cellular_component)

    Optionally loads OBO hierarchy file to create IS_A edges between GO terms.
    """

    def __init__(self, conn, *, obo_path: str | Path | None = None) -> None:
        super().__init__(conn)
        self._obo_path = Path(obo_path) if obo_path else None

    @property
    def source_name(self) -> str:
        return "GO"

    def _parse_file(self, input_path: Path, **kwargs) -> Iterator[dict]:
        """Yield {gene_symbol, go_id, go_name, namespace} dicts from GAF."""
        import gzip

        opener = gzip.open if _is_gzipped(input_path) else open
        seen = set()
        with opener(input_path, "rt") as f:
            for line in f:
                if line.startswith("!"):
                    continue
                parts = line.rstrip("\n").split("\t")
                if len(parts) < 9:
                    continue
                gene_symbol = parts[2].strip()
                go_id = parts[4].strip()
                aspect = parts[8].strip()
                if not gene_symbol or not go_id:
                    continue
                # Map aspect letter to namespace
                namespace = _ASPECT_MAP.get(aspect, "unknown")
                # Deduplicate gene-GO pairs
                key = (gene_symbol, go_id)
                if key in seen:
                    continue
                seen.add(key)
                yield {
                    "gene_symbol": gene_symbol,
                    "go_id": go_id,
                    "go_name": "",  # Name comes from OBO if provided
                    "namespace": namespace,
                }

    def _load_batch(self, batch: list[dict]) -> int:
        # Step 1: MERGE GOTerm nodes
        terms = {}
        for rec in batch:
            if rec["go_id"] not in terms:
                terms[rec["go_id"]] = {
                    "id": rec["go_id"],
                    "name": rec["go_name"],
                    "namespace": rec["namespace"],
                }
        with self._conn.driver.session() as session:
            session.run(MERGE_GOTERM_BATCH, {"terms": list(terms.values())})

        # Step 2: CREATE gene-GO edges
        edges = [{"gene_symbol": r["gene_symbol"], "go_id": r["go_id"]} for r in batch]
        with self._conn.driver.session() as session:
            session.run(CREATE_HAS_GO_TERM_BATCH, {"edges": edges})

        return len(edges)

    def load(self, input_path, version, **kwargs):
        """Override to also load OBO hierarchy after main load."""
        result = super().load(input_path, version, **kwargs)

        if self._obo_path and self._obo_path.exists():
            n_hier = self._load_obo_hierarchy(self._obo_path)
            result["n_hierarchy_edges"] = n_hier
            logger.info("Loaded %d IS_A hierarchy edges from OBO", n_hier)

        return result

    def _load_obo_hierarchy(self, obo_path: Path) -> int:
        """Parse OBO file and create IS_A edges + update GO term names."""
        terms, hierarchy = _parse_obo(obo_path)

        # Update term names in batches
        term_list = [
            {"id": tid, "name": t["name"], "namespace": t["namespace"]} for tid, t in terms.items()
        ]
        for i in range(0, len(term_list), 10_000):
            batch = term_list[i : i + 10_000]
            with self._conn.driver.session() as session:
                session.run(MERGE_GOTERM_BATCH, {"terms": batch})

        # Create IS_A edges in batches
        total = 0
        for i in range(0, len(hierarchy), 10_000):
            batch = hierarchy[i : i + 10_000]
            with self._conn.driver.session() as session:
                session.run(CREATE_GO_HIERARCHY_BATCH, {"edges": batch})
            total += len(batch)

        return total


class PathwayParser(BaseAnnotationParser):
    """Parse pathway TSV files and create Pathway nodes + Gene-Pathway edges.

    Expected TSV format (tab-separated, header required)::

        gene_symbol  pathway_id  pathway_name  source

    ``source`` identifies the database (e.g. 'KEGG', 'Reactome').
    """

    def __init__(self, conn, *, pathway_source: str = "KEGG") -> None:
        super().__init__(conn)
        self._pathway_source = pathway_source

    @property
    def source_name(self) -> str:
        return f"Pathway_{self._pathway_source}"

    def _parse_file(self, input_path: Path, **kwargs) -> Iterator[dict]:
        """Yield {gene_symbol, pathway_id, pathway_name, source} dicts."""
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
                if len(parts) < 3:
                    continue
                row = dict(zip(header, parts))
                gene_symbol = row.get("gene_symbol", "").strip()
                pathway_id = row.get("pathway_id", "").strip()
                pathway_name = row.get("pathway_name", "").strip()
                source = row.get("source", self._pathway_source).strip()
                if not gene_symbol or not pathway_id:
                    continue
                yield {
                    "gene_symbol": gene_symbol,
                    "pathway_id": pathway_id,
                    "pathway_name": pathway_name,
                    "source": source,
                }

    def _load_batch(self, batch: list[dict]) -> int:
        # Step 1: MERGE Pathway nodes
        pathways = {}
        for rec in batch:
            if rec["pathway_id"] not in pathways:
                pathways[rec["pathway_id"]] = {
                    "id": rec["pathway_id"],
                    "name": rec["pathway_name"],
                    "source": rec["source"],
                }
        with self._conn.driver.session() as session:
            session.run(MERGE_PATHWAY_BATCH, {"pathways": list(pathways.values())})

        # Step 2: CREATE gene-pathway edges
        edges = [{"gene_symbol": r["gene_symbol"], "pathway_id": r["pathway_id"]} for r in batch]
        with self._conn.driver.session() as session:
            session.run(CREATE_IN_PATHWAY_BATCH, {"edges": edges})

        return len(edges)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_ASPECT_MAP = {
    "F": "molecular_function",
    "P": "biological_process",
    "C": "cellular_component",
}


def _parse_obo(path: Path) -> tuple[dict[str, dict], list[dict]]:
    """Parse a GO OBO file, extracting term names/namespaces and IS_A edges.

    Returns:
        (terms, hierarchy) where terms = {go_id: {name, namespace}}
        and hierarchy = [{child_id, parent_id}, ...].
    """
    terms: dict[str, dict] = {}
    hierarchy: list[dict] = []

    current_id = None
    current_name = ""
    current_namespace = ""
    in_term = False

    with open(path) as f:
        for line in f:
            line = line.strip()
            if line == "[Term]":
                in_term = True
                current_id = None
                current_name = ""
                current_namespace = ""
            elif line == "" or line.startswith("["):
                if in_term and current_id:
                    terms[current_id] = {
                        "name": current_name,
                        "namespace": current_namespace,
                    }
                in_term = line.startswith("[Term]") if line else False
                if in_term:
                    current_id = None
                    current_name = ""
                    current_namespace = ""
            elif in_term:
                if line.startswith("id: "):
                    current_id = line[4:].strip()
                elif line.startswith("name: "):
                    current_name = line[6:].strip()
                elif line.startswith("namespace: "):
                    current_namespace = line[11:].strip()
                elif line.startswith("is_a: "):
                    parent_id = line[6:].split("!")[0].strip()
                    if current_id and parent_id:
                        hierarchy.append({"child_id": current_id, "parent_id": parent_id})

    # Don't forget the last term
    if in_term and current_id:
        terms[current_id] = {"name": current_name, "namespace": current_namespace}

    return terms, hierarchy


def _is_gzipped(path: Path) -> bool:
    with open(path, "rb") as f:
        return f.read(2) == b"\x1f\x8b"
