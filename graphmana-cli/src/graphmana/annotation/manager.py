"""Annotation versioning — load, list, and remove annotation layers."""

from __future__ import annotations

import csv
import logging
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from graphmana.db.connection import GraphManaConnection
from graphmana.db.queries import (
    CREATE_ANNOTATION_VERSION,
    CREATE_CONSEQUENCE_BATCH,
    DELETE_ANNOTATION_VERSION,
    DELETE_EDGES_BY_VERSION_BATCH,
    DELETE_ORPHAN_GENES,
    GET_ANNOTATION_VERSION,
    LIST_ANNOTATION_VERSIONS,
    MERGE_CONSEQUENCE_BATCH,
    MERGE_GENE_BATCH,
)

logger = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = 5000


class AnnotationManager:
    """Manage annotation versions stored as AnnotationVersion nodes.

    Supports loading new annotation layers from VEP/SnpEff-annotated VCFs,
    listing versions, and removing versions with edge cleanup.
    """

    def __init__(self, conn: GraphManaConnection) -> None:
        self._conn = conn

    def load(
        self,
        input_path: str | Path,
        version: str,
        mode: str = "add",
        *,
        annotation_type: str = "auto",
        description: str = "",
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> dict:
        """Load annotations from a VEP/SnpEff VCF into the graph.

        Args:
            input_path: Path to VEP/SnpEff annotated VCF.
            version: Version label (becomes version_id and annotation_version on edges).
            mode: One of 'add', 'update', 'replace'.
            annotation_type: 'auto', 'vep', or 'snpeff'. Auto-detects from VCF header.
            description: Human-readable description for the AnnotationVersion node.
            batch_size: Number of edges per Cypher UNWIND batch.

        Returns:
            Dict with n_genes, n_edges, mode, version.
        """
        if mode not in ("add", "update", "replace"):
            raise ValueError(f"Invalid mode: {mode!r}. Must be 'add', 'update', or 'replace'.")

        # Step 1: If replace mode, delete existing edges for this version + orphan genes
        if mode == "replace":
            deleted = self._delete_edges_for_version(version, batch_size=batch_size)
            with self._conn.driver.session() as session:
                result = session.run(DELETE_ORPHAN_GENES)
                record = result.single()
                n_orphans = record["deleted"] if record else 0
            logger.info(
                "Replace mode: deleted %d existing edges, %d orphan genes for version %r",
                deleted,
                n_orphans,
                version,
            )

        # Step 2: Parse VCF via VEPParser to a temp dir
        from graphmana.ingest.vep_parser import VEPParser

        with tempfile.TemporaryDirectory(prefix="graphmana_ann_") as tmp_dir:
            ann_source = "VEP"
            if annotation_type == "snpeff":
                ann_source = "SnpEff"

            vep = VEPParser(
                input_path,
                tmp_dir,
                variant_id_set=None,
                annotation_source=ann_source,
                annotation_version=version,
            )
            vep.run()

            # Override annotation_source with what was actually detected
            detected_source = "VEP" if vep.ann_format == "CSQ" else "SnpEff"

            tmp_path = Path(tmp_dir)
            gene_csv = tmp_path / "gene_nodes.csv"
            edge_csv = tmp_path / "has_consequence_edges.csv"

            genes = _read_gene_csv(gene_csv)
            edges = _read_edge_csv(edge_csv, detected_source)

        # Step 3: MERGE Gene nodes in batches
        for i in range(0, len(genes), batch_size):
            batch = genes[i : i + batch_size]
            with self._conn.driver.session() as session:
                session.run(MERGE_GENE_BATCH, {"genes": batch})

        # Step 4: CREATE or MERGE edges in batches
        query = MERGE_CONSEQUENCE_BATCH if mode == "update" else CREATE_CONSEQUENCE_BATCH
        n_loaded = 0
        for i in range(0, len(edges), batch_size):
            batch = edges[i : i + batch_size]
            with self._conn.driver.session() as session:
                session.run(query, {"edges": batch})
            n_loaded += len(batch)

        # Step 5: Upsert AnnotationVersion tracking node
        now = datetime.now(timezone.utc).isoformat()
        with self._conn.driver.session() as session:
            session.run(
                CREATE_ANNOTATION_VERSION,
                {
                    "version_id": version,
                    "source": detected_source,
                    "version": version,
                    "loaded_date": now,
                    "n_annotations": n_loaded,
                    "description": description,
                },
            )

        logger.info(
            "Annotation load complete: version=%r, mode=%s, %d genes, %d edges",
            version,
            mode,
            len(genes),
            n_loaded,
        )

        return {
            "version": version,
            "mode": mode,
            "source": detected_source,
            "n_genes": len(genes),
            "n_edges": n_loaded,
        }

    def list(self) -> list[dict]:
        """List all annotation versions.

        Returns:
            List of AnnotationVersion property dicts, ordered by loaded_date DESC.
        """
        with self._conn.driver.session() as session:
            result = session.run(LIST_ANNOTATION_VERSIONS)
            return [dict(record["a"]) for record in result]

    def get(self, version_id: str) -> dict | None:
        """Get a single annotation version by version_id.

        Returns:
            AnnotationVersion property dict, or None if not found.
        """
        with self._conn.driver.session() as session:
            result = session.run(GET_ANNOTATION_VERSION, {"version_id": version_id})
            record = result.single()
            return dict(record["a"]) if record else None

    def remove(self, version: str, *, cleanup_genes: bool = True) -> dict:
        """Remove an annotation version and its edges.

        Args:
            version: Version string to remove.
            cleanup_genes: If True, delete Gene nodes with no remaining edges.

        Returns:
            Dict with n_edges_deleted, n_genes_deleted, version.
        """
        # Delete edges in batches
        n_deleted = self._delete_edges_for_version(version)

        # Delete the AnnotationVersion node
        with self._conn.driver.session() as session:
            session.run(DELETE_ANNOTATION_VERSION, {"version_id": version})

        # Clean up orphan genes
        n_genes_deleted = 0
        if cleanup_genes:
            with self._conn.driver.session() as session:
                result = session.run(DELETE_ORPHAN_GENES)
                record = result.single()
                n_genes_deleted = record["deleted"] if record else 0

        logger.info(
            "Annotation remove: version=%r, %d edges deleted, %d orphan genes removed",
            version,
            n_deleted,
            n_genes_deleted,
        )

        return {
            "version": version,
            "n_edges_deleted": n_deleted,
            "n_genes_deleted": n_genes_deleted,
        }

    def _delete_edges_for_version(
        self, version: str, *, batch_size: int = DEFAULT_BATCH_SIZE
    ) -> int:
        """Delete all HAS_CONSEQUENCE edges for a version in batches."""
        total = 0
        while True:
            with self._conn.driver.session() as session:
                result = session.run(
                    DELETE_EDGES_BY_VERSION_BATCH,
                    {"version": version, "batch_size": batch_size},
                )
                record = result.single()
                deleted = record["deleted"] if record else 0
            total += deleted
            if deleted < batch_size:
                break
        return total


def _read_gene_csv(path: Path) -> list[dict]:
    """Read gene_nodes.csv into a list of dicts for Cypher UNWIND.

    Args:
        path: Path to gene_nodes.csv (neo4j-admin format with header).

    Returns:
        List of {geneId, symbol, biotype} dicts.
    """
    genes: list[dict] = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            genes.append(
                {
                    "geneId": row.get("geneId:ID(Gene)", ""),
                    "symbol": row.get("symbol", ""),
                    "biotype": row.get("biotype", ""),
                }
            )
    return genes


def _read_edge_csv(path: Path, detected_source: str) -> list[dict]:
    """Read has_consequence_edges.csv into a list of dicts for Cypher UNWIND.

    Args:
        path: Path to has_consequence_edges.csv (neo4j-admin format with header).
        detected_source: Actual annotation source detected from VCF ('VEP' or 'SnpEff').

    Returns:
        List of edge property dicts.
    """
    edges: list[dict] = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            edges.append(
                {
                    "variantId": row.get(":START_ID(Variant)", ""),
                    "geneId": row.get(":END_ID(Gene)", ""),
                    "consequence": row.get("consequence", ""),
                    "impact": row.get("impact", ""),
                    "feature": row.get("feature", ""),
                    "feature_type": row.get("feature_type", ""),
                    "sift_score": _to_float_or_none(row.get("sift_score:float", "")),
                    "sift_pred": row.get("sift_pred", ""),
                    "polyphen_score": _to_float_or_none(row.get("polyphen_score:float", "")),
                    "polyphen_pred": row.get("polyphen_pred", ""),
                    "cadd_phred": _to_float_or_none(row.get("cadd_phred:float", "")),
                    "revel": _to_float_or_none(row.get("revel:float", "")),
                    "annotation_source": detected_source,
                    "annotation_version": row.get("annotation_version", ""),
                }
            )
    return edges


def _to_float_or_none(value: str) -> float | None:
    """Convert string to float, or None if empty/invalid."""
    if not value or not value.strip():
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None
