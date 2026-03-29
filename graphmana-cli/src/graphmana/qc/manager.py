"""Quality control — variant, sample, and batch QC reports."""

from __future__ import annotations

import logging

import numpy as np

from graphmana.db.connection import GraphManaConnection
from graphmana.db.queries import (
    FETCH_CHROMOSOMES,
    FETCH_SAMPLES,
    FETCH_VARIANTS_BY_CHR,
    QC_POPULATION_SUMMARY,
    QC_VARIANT_CHR_COUNTS,
    QC_VARIANT_SUMMARY,
    QC_VARIANT_TYPE_COUNTS,
    UPDATE_SAMPLE_QC_BATCH,
)
from graphmana.ingest.genotype_packer import unpack_genotypes

logger = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = 5000


class QCManager:
    """Compute quality control statistics on a GraphMana database.

    Three QC modes:
    - **variant**: Aggregate Variant node properties (fast, Cypher-only).
    - **sample**: Compute per-sample stats by scanning variants and unpacking
      genotypes (FULL PATH — O(V * N/4)). Stores results on Sample nodes.
    - **batch**: Per-population summaries (fast, Cypher-only).
    """

    def __init__(self, conn: GraphManaConnection) -> None:
        self._conn = conn

    def run(self, qc_type: str = "all") -> dict:
        """Run QC and return results dict.

        Args:
            qc_type: One of 'variant', 'sample', 'batch', 'all'.

        Returns:
            Dict with keys matching requested qc_type(s).
        """
        result: dict = {}
        if qc_type in ("variant", "all"):
            result["variant"] = self.variant_qc()
        if qc_type in ("sample", "all"):
            result["sample"] = self.sample_qc()
        if qc_type in ("batch", "all"):
            result["batch"] = self.batch_qc()
        return result

    def variant_qc(self) -> dict:
        """Compute variant-level QC statistics from existing Variant properties.

        Returns:
            Dict with summary, type_counts, chr_counts.
        """
        with self._conn.driver.session() as session:
            # Summary stats
            rec = session.run(QC_VARIANT_SUMMARY).single()
            summary = dict(rec) if rec else {}

            # Type distribution
            type_counts = [
                {"variant_type": r["variant_type"], "count": r["count"]}
                for r in session.run(QC_VARIANT_TYPE_COUNTS)
            ]

            # Per-chromosome counts
            chr_counts = [
                {"chr": r["chr"], "count": r["count"]} for r in session.run(QC_VARIANT_CHR_COUNTS)
            ]

        logger.info(
            "Variant QC: %d variants, mean call_rate=%.4f",
            summary.get("n_variants", 0),
            summary.get("mean_call_rate") or 0,
        )

        return {
            "summary": summary,
            "type_counts": type_counts,
            "chr_counts": chr_counts,
        }

    def sample_qc(self, *, batch_size: int = DEFAULT_BATCH_SIZE) -> dict:
        """Compute per-sample QC by scanning all variants.

        Iterates every variant, unpacks genotypes, and accumulates per-sample
        counts of het, hom_alt, and missing calls. Stores computed stats
        back on Sample nodes.

        Args:
            batch_size: Number of sample stat updates per Cypher batch.

        Returns:
            Dict with n_samples, n_variants_scanned, and per-sample stats list.
        """
        # Load active samples
        with self._conn.driver.session() as session:
            samples = [dict(r) for r in session.run(FETCH_SAMPLES)]

        if not samples:
            return {"n_samples": 0, "n_variants_scanned": 0, "stats": []}

        n_samples = len(samples)
        packed_indices = np.array([s["packed_index"] for s in samples], dtype=np.int64)
        sample_ids = [s["sampleId"] for s in samples]

        # Accumulators
        het_counts = np.zeros(n_samples, dtype=np.int64)
        hom_alt_counts = np.zeros(n_samples, dtype=np.int64)
        called_counts = np.zeros(n_samples, dtype=np.int64)
        n_variants_scanned = 0

        # Load chromosomes
        with self._conn.driver.session() as session:
            chroms = [dict(r) for r in session.run(FETCH_CHROMOSOMES)]

        # Scan variants per chromosome
        for chrom in chroms:
            chr_name = chrom["chr"]
            with self._conn.driver.session() as session:
                result = session.run(FETCH_VARIANTS_BY_CHR, {"chr": chr_name})
                for record in result:
                    props = dict(record["v"])
                    gt_packed = props.get("gt_packed")
                    if gt_packed is None:
                        continue

                    if isinstance(gt_packed, (list, bytearray)):
                        gt_packed = bytes(gt_packed)

                    n_total = len(gt_packed) * 4
                    gt_all = unpack_genotypes(gt_packed, n_total)

                    max_idx = len(gt_all)
                    safe = packed_indices < max_idx
                    safe_indices = packed_indices[safe]
                    gt_subset = gt_all[safe_indices]

                    # 0=HomRef, 1=Het, 2=HomAlt, 3=Missing
                    het_counts[safe] += gt_subset == 1
                    hom_alt_counts[safe] += gt_subset == 2
                    called_counts[safe] += gt_subset != 3

                    n_variants_scanned += 1

            logger.info("Sample QC: scanned %s (%d variants so far)", chr_name, n_variants_scanned)

        # Compute derived stats
        stats: list[dict] = []
        for i in range(n_samples):
            n_called = int(called_counts[i])
            n_het = int(het_counts[i])
            n_hom_alt = int(hom_alt_counts[i])
            heterozygosity = n_het / n_called if n_called > 0 else 0.0
            call_rate = n_called / n_variants_scanned if n_variants_scanned > 0 else 0.0

            stats.append(
                {
                    "sampleId": sample_ids[i],
                    "n_het": n_het,
                    "n_hom_alt": n_hom_alt,
                    "heterozygosity": round(heterozygosity, 6),
                    "call_rate": round(call_rate, 6),
                }
            )

        # Store stats back on Sample nodes
        for j in range(0, len(stats), batch_size):
            batch = stats[j : j + batch_size]
            with self._conn.driver.session() as session:
                session.run(UPDATE_SAMPLE_QC_BATCH, {"stats": batch})

        logger.info(
            "Sample QC complete: %d samples, %d variants scanned",
            n_samples,
            n_variants_scanned,
        )

        return {
            "n_samples": n_samples,
            "n_variants_scanned": n_variants_scanned,
            "stats": stats,
        }

    def batch_qc(self) -> dict:
        """Compute per-population QC summaries.

        Returns:
            Dict with population_summary list.
        """
        with self._conn.driver.session() as session:
            pop_summary = [dict(r) for r in session.run(QC_POPULATION_SUMMARY)]

        logger.info("Batch QC: %d populations", len(pop_summary))

        return {"population_summary": pop_summary}
