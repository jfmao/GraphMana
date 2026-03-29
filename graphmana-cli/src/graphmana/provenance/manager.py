"""Provenance tracking — ingestion logs and VCF header audit trail."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from graphmana.db.connection import GraphManaConnection
from graphmana.db.queries import (
    CREATE_INGESTION_LOG,
    GET_INGESTION_LOG,
    GET_VCF_HEADER,
    LIST_INGESTION_LOGS,
    LIST_VCF_HEADERS,
    PROVENANCE_SUMMARY,
)

logger = logging.getLogger(__name__)


class ProvenanceManager:
    """Manage ingestion logs and VCF header provenance records.

    IngestionLog nodes are created during import; VCFHeader nodes are created
    by the CSV emitter. This manager provides read access to both, plus
    creation of IngestionLog entries.
    """

    def __init__(self, conn: GraphManaConnection) -> None:
        self._conn = conn

    def record_ingestion(
        self,
        source_file: str,
        dataset_id: str,
        mode: str,
        n_samples: int,
        n_variants: int,
        filters_applied: str = "",
        fidelity: str = "default",
        reference_genome: str = "unknown",
    ) -> dict:
        """Create an IngestionLog node recording a successful import.

        Args:
            source_file: Path to the source VCF/BCF file.
            dataset_id: Dataset identifier for provenance.
            mode: Import mode ('initial' or 'incremental').
            n_samples: Number of samples imported.
            n_variants: Number of variants imported.
            filters_applied: JSON string of filters used.
            fidelity: Fidelity level ('minimal', 'default', 'full').
            reference_genome: Reference genome identifier.

        Returns:
            Dict of IngestionLog node properties.
        """
        now = datetime.now(timezone.utc).isoformat()
        log_id = f"{dataset_id}_{now}"

        with self._conn.driver.session() as session:
            result = session.run(
                CREATE_INGESTION_LOG,
                {
                    "log_id": log_id,
                    "source_file": source_file,
                    "dataset_id": dataset_id,
                    "mode": mode,
                    "import_date": now,
                    "n_samples": n_samples,
                    "n_variants": n_variants,
                    "filters_applied": filters_applied,
                    "fidelity": fidelity,
                    "reference_genome": reference_genome,
                },
            )
            record = result.single()
            props = dict(record["l"]) if record else {}

        logger.info(
            "Recorded ingestion log: %s (%s, %d samples, %d variants)",
            log_id,
            mode,
            n_samples,
            n_variants,
        )
        return props

    def list_ingestions(self) -> list[dict]:
        """List all ingestion logs, ordered by import_date DESC.

        Returns:
            List of IngestionLog property dicts.
        """
        with self._conn.driver.session() as session:
            result = session.run(LIST_INGESTION_LOGS)
            return [dict(record["l"]) for record in result]

    def get_ingestion(self, log_id: str) -> dict | None:
        """Get a single ingestion log by log_id.

        Returns:
            IngestionLog property dict, or None if not found.
        """
        with self._conn.driver.session() as session:
            result = session.run(GET_INGESTION_LOG, {"log_id": log_id})
            record = result.single()
            return dict(record["l"]) if record else None

    def list_vcf_headers(self) -> list[dict]:
        """List all VCF header nodes, ordered by import_date DESC.

        Returns:
            List of VCFHeader property dicts.
        """
        with self._conn.driver.session() as session:
            result = session.run(LIST_VCF_HEADERS)
            return [dict(record["h"]) for record in result]

    def get_vcf_header(self, dataset_id: str) -> dict | None:
        """Get a single VCF header by dataset_id.

        Returns:
            VCFHeader property dict, or None if not found.
        """
        with self._conn.driver.session() as session:
            result = session.run(GET_VCF_HEADER, {"dataset_id": dataset_id})
            record = result.single()
            return dict(record["h"]) if record else None

    def summary(self) -> dict:
        """Aggregate provenance summary across all ingestions.

        Returns:
            Dict with n_ingestions, total_samples_imported,
            total_variants_imported, first_import, last_import, source_files.
        """
        with self._conn.driver.session() as session:
            result = session.run(PROVENANCE_SUMMARY)
            record = result.single()
            if record is None:
                return {
                    "n_ingestions": 0,
                    "total_samples_imported": 0,
                    "total_variants_imported": 0,
                    "first_import": None,
                    "last_import": None,
                    "source_files": [],
                }
            return {
                "n_ingestions": record["n_ingestions"],
                "total_samples_imported": record["total_samples_imported"],
                "total_variants_imported": record["total_variants_imported"],
                "first_import": record["first_import"],
                "last_import": record["last_import"],
                "source_files": list(record["source_files"]),
            }
