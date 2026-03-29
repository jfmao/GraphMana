"""Liftover orchestrator — converts variant coordinates across genome assemblies.

Coordinates the full liftover workflow: fetch variants per chromosome, map
coordinates via ``LiftoverConverter``, detect self-collisions, batch-update
the graph, rebuild NEXT chains, and update SchemaMetadata.
"""

from __future__ import annotations

import csv
import logging
from datetime import datetime, timezone
from pathlib import Path

from graphmana.db.connection import GraphManaConnection
from graphmana.db.queries import (
    DELETE_NEXT_CHAIN_FOR_CHR,
    FETCH_CHROMOSOMES,
    FETCH_VARIANT_COORDS_BY_CHR,
    LIFTOVER_ENSURE_CHROMOSOME,
    LIFTOVER_FLAG_UNMAPPED_BATCH,
    LIFTOVER_REPOINT_ON_CHROMOSOME_BATCH,
    LIFTOVER_UPDATE_SCHEMA_REFERENCE,
    LIFTOVER_UPDATE_VARIANT_BATCH,
    REBUILD_NEXT_CHAIN_FOR_CHR,
)
from graphmana.liftover.chain_parser import LiftoverConverter, LiftoverResult, UnmappedVariant

logger = logging.getLogger(__name__)


class GraphLiftover:
    """Orchestrate reference genome liftover for all variants in the database.

    Args:
        conn: Active GraphMana database connection.
        converter: Configured ``LiftoverConverter`` with the chain file loaded.
        target_reference: Name of the target reference assembly (e.g. "GRCh38").
    """

    def __init__(
        self,
        conn: GraphManaConnection,
        converter: LiftoverConverter,
        target_reference: str,
    ) -> None:
        self._conn = conn
        self._converter = converter
        self._target_reference = target_reference

    def run(
        self,
        *,
        dry_run: bool = False,
        reject_file: str | Path | None = None,
        update_annotations: bool = False,
        batch_size: int = 500,
    ) -> dict:
        """Execute the liftover workflow.

        Args:
            dry_run: If True, compute mappings and summary but make no DB changes.
            reject_file: Path to write unmapped/ambiguous variants as TSV.
            update_annotations: If True, attempt to update Gene coordinates (currently
                logs a warning and skips — Gene nodes lack coordinate properties).
            batch_size: Number of variants per DB write batch.

        Returns:
            Summary dict with mapped, unmapped, ambiguous, collision, and
            chromosomes_affected counts.
        """
        if update_annotations:
            logger.warning(
                "--update-annotations: Gene coordinate liftover is not yet supported "
                "(Gene nodes lack chr/start/end properties). Skipping."
            )

        # 1. Fetch all chromosomes
        chromosomes = self._fetch_chromosomes()
        logger.info("Found %d chromosomes to process", len(chromosomes))

        # 2. Map all variants
        all_mapped: list[tuple[str, LiftoverResult]] = []  # (old_variantId, result)
        all_unmapped: list[UnmappedVariant] = []

        for chrom in chromosomes:
            variants = self._fetch_variant_coords(chrom)
            logger.info("Chromosome %s: %d variants to map", chrom, len(variants))

            for var in variants:
                result = self._converter.convert(
                    var["variantId"], var["chr"], var["pos"], var["ref"], var["alt"]
                )
                if isinstance(result, LiftoverResult):
                    all_mapped.append((var["variantId"], result))
                else:
                    all_unmapped.append(result)

        # 3. Detect self-collisions (two source variants → same target variant ID)
        collisions: list[UnmappedVariant] = []
        seen_ids: dict[str, str] = {}  # new_variant_id → first old_variant_id
        deduped_mapped: list[tuple[str, LiftoverResult]] = []

        for old_vid, result in all_mapped:
            if result.new_variant_id in seen_ids:
                # First occurrence stays mapped; subsequent become collisions
                collisions.append(
                    UnmappedVariant(
                        old_vid,
                        result.new_chr,
                        result.new_pos,
                        result.new_ref,
                        result.new_alt,
                        "collision",
                    )
                )
            else:
                seen_ids[result.new_variant_id] = old_vid
                deduped_mapped.append((old_vid, result))

        all_unmapped.extend(collisions)

        summary = {
            "mapped": len(deduped_mapped),
            "unmapped": sum(1 for u in all_unmapped if u.reason == "unmapped"),
            "ambiguous": sum(1 for u in all_unmapped if u.reason == "ambiguous"),
            "collision": len(collisions),
            "total_variants": len(deduped_mapped) + len(all_unmapped),
            "target_reference": self._target_reference,
            "chromosomes_processed": len(chromosomes),
        }

        logger.info(
            "Liftover summary: %d mapped, %d unmapped, %d ambiguous, %d collisions",
            summary["mapped"],
            summary["unmapped"],
            summary["ambiguous"],
            summary["collision"],
        )

        # 4. Write reject file
        if reject_file:
            self._write_reject_file(reject_file, all_unmapped)

        if dry_run:
            summary["dry_run"] = True
            return summary

        # 5. Apply DB updates
        affected_chrs: set[str] = set()

        # 5a. Batch-update mapped variants
        for i in range(0, len(deduped_mapped), batch_size):
            batch = deduped_mapped[i : i + batch_size]
            updates = []
            for old_vid, result in batch:
                affected_chrs.add(result.new_chr)
                updates.append(
                    {
                        "old_variantId": old_vid,
                        "variantId": result.new_variant_id,
                        "chr": result.new_chr,
                        "pos": result.new_pos,
                        "ref": result.new_ref,
                        "alt": result.new_alt,
                        "liftover_status": "mapped",
                        "original_variantId": old_vid,
                    }
                )
            with self._conn.driver.session() as session:
                session.run(LIFTOVER_UPDATE_VARIANT_BATCH, {"updates": updates})

        # 5b. Batch-flag unmapped/ambiguous/collision
        for i in range(0, len(all_unmapped), batch_size):
            batch = all_unmapped[i : i + batch_size]
            updates = []
            for uv in batch:
                affected_chrs.add(uv.chr)
                updates.append(
                    {
                        "variantId": uv.variant_id,
                        "liftover_status": uv.reason,
                        "original_variantId": uv.variant_id,
                    }
                )
            with self._conn.driver.session() as session:
                session.run(LIFTOVER_FLAG_UNMAPPED_BATCH, {"updates": updates})

        # 5c. Ensure target chromosome nodes exist and re-point ON_CHROMOSOME edges
        repoint_updates: list[dict] = []
        new_chrs: set[str] = set()

        for old_vid, result in deduped_mapped:
            new_chrs.add(result.new_chr)
            repoint_updates.append(
                {
                    "variantId": result.new_variant_id,
                    "new_chr": result.new_chr,
                }
            )

        # Ensure all target chromosomes exist
        with self._conn.driver.session() as session:
            for chr_id in new_chrs:
                session.run(LIFTOVER_ENSURE_CHROMOSOME, {"chromosomeId": chr_id})

        # Re-point ON_CHROMOSOME edges in batches
        for i in range(0, len(repoint_updates), batch_size):
            batch = repoint_updates[i : i + batch_size]
            with self._conn.driver.session() as session:
                session.run(LIFTOVER_REPOINT_ON_CHROMOSOME_BATCH, {"updates": batch})

        # 5d. Rebuild NEXT chains on all affected chromosomes
        # Include source chromosomes (variants may have moved away)
        for chrom in chromosomes:
            affected_chrs.add(chrom)

        for chrom in sorted(affected_chrs):
            logger.info("Rebuilding NEXT chain for chromosome %s", chrom)
            with self._conn.driver.session() as session:
                session.run(DELETE_NEXT_CHAIN_FOR_CHR, {"chr": chrom})
                session.run(REBUILD_NEXT_CHAIN_FOR_CHR, {"chr": chrom})

        # 5e. Update SchemaMetadata
        with self._conn.driver.session() as session:
            session.run(
                LIFTOVER_UPDATE_SCHEMA_REFERENCE,
                {
                    "reference_genome": self._target_reference,
                    "last_modified": datetime.now(timezone.utc).isoformat(),
                },
            )

        summary["chromosomes_affected"] = len(affected_chrs)
        return summary

    def _fetch_chromosomes(self) -> list[str]:
        """Fetch all chromosome IDs from the database."""
        with self._conn.driver.session() as session:
            result = session.run(FETCH_CHROMOSOMES)
            return [r["chr"] for r in result]

    def _fetch_variant_coords(self, chrom: str) -> list[dict]:
        """Fetch lightweight variant coordinates for a chromosome."""
        with self._conn.driver.session() as session:
            result = session.run(FETCH_VARIANT_COORDS_BY_CHR, {"chr": chrom})
            return [
                {
                    "variantId": r["variantId"],
                    "chr": r["chr"],
                    "pos": r["pos"],
                    "ref": r["ref"],
                    "alt": r["alt"],
                }
                for r in result
            ]

    @staticmethod
    def _write_reject_file(path: str | Path, unmapped: list[UnmappedVariant]) -> None:
        """Write unmapped variants to a TSV reject file."""
        path = Path(path)
        with open(path, "w", newline="") as f:
            writer = csv.writer(f, delimiter="\t")
            writer.writerow(["variantId", "chr", "pos", "ref", "alt", "reason"])
            for uv in unmapped:
                writer.writerow([uv.variant_id, uv.chr, uv.pos, uv.ref, uv.alt, uv.reason])
        logger.info("Wrote %d rejected variants to %s", len(unmapped), path)
