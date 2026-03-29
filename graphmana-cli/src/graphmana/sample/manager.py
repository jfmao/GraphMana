"""Sample management — soft delete, restore, reassignment, hard delete, and listing."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from graphmana.db.connection import GraphManaConnection
from graphmana.db.queries import (
    ACTIVE_SAMPLE_FILTER,
    COUNT_SAMPLES_BY_STATUS,
    DECREMENT_SCHEMA_SAMPLE_COUNT,
    DELETE_POPULATION_IF_EMPTY,
    DELETE_SAMPLE_NODE,
    EXCLUDE_SAMPLES,
    FETCH_VARIANT_BATCH,
    FETCH_VARIANT_IDS_BY_CHR,
    GET_SAMPLE,
    LIST_ALL_SAMPLES,
    LIST_ALL_SAMPLES_WITH_EXCLUDED,
    LIST_SAMPLES_BY_POPULATION,
    LIST_SAMPLES_BY_POPULATION_WITH_EXCLUDED,
    REASSIGN_SAMPLE_POPULATION,
    RESTORE_SAMPLES,
    UPDATE_POPULATION_COUNTS,
    UPDATE_VARIANT_HARD_DELETE_BATCH,
    UPDATE_VARIANT_POP_STATS_BATCH,
)
from graphmana.ingest.array_ops import (
    reassign_pop_stats,
    subtract_sample_from_pop_stats,
    zero_out_gt_packed,
    zero_out_phase_packed,
)
from graphmana.ingest.genotype_packer import unpack_genotypes

logger = logging.getLogger(__name__)


class SampleManager:
    """Manage sample lifecycle — soft delete, restore, reassignment, hard delete, and listing.

    Soft delete sets ``excluded = true`` on Sample nodes. All export queries
    already filter on this flag, so excluded samples are invisible to exports
    without modifying packed arrays or population statistics.

    Hard delete zeros out genotype data in packed arrays and recalculates
    all population statistics, then removes the Sample node entirely.

    Reassignment moves samples between populations, updating all variant
    population arrays accordingly.
    """

    def __init__(self, conn: GraphManaConnection) -> None:
        self._conn = conn

    def remove(
        self,
        sample_ids: list[str],
        *,
        reason: str = "",
    ) -> dict:
        """Soft-delete samples by setting excluded = true.

        Args:
            sample_ids: List of sample IDs to exclude.
            reason: Optional human-readable exclusion reason.

        Returns:
            Dict with n_excluded and sample_ids.

        Raises:
            ValueError: If sample_ids is empty.
        """
        if not sample_ids:
            raise ValueError("No sample IDs provided.")

        with self._conn.driver.session() as session:
            result = session.run(
                EXCLUDE_SAMPLES,
                {"sample_ids": sample_ids, "reason": reason or None},
            )
            record = result.single()
            n_updated = record["updated"] if record else 0

        logger.info(
            "Soft-deleted %d samples (requested %d)",
            n_updated,
            len(sample_ids),
        )

        return {
            "n_excluded": n_updated,
            "sample_ids": sample_ids,
            "reason": reason,
        }

    def restore(self, sample_ids: list[str]) -> dict:
        """Restore soft-deleted samples by clearing the excluded flag.

        Args:
            sample_ids: List of sample IDs to restore.

        Returns:
            Dict with n_restored and sample_ids.

        Raises:
            ValueError: If sample_ids is empty.
        """
        if not sample_ids:
            raise ValueError("No sample IDs provided.")

        with self._conn.driver.session() as session:
            result = session.run(
                RESTORE_SAMPLES,
                {"sample_ids": sample_ids},
            )
            record = result.single()
            n_updated = record["updated"] if record else 0

        logger.info(
            "Restored %d samples (requested %d)",
            n_updated,
            len(sample_ids),
        )

        return {
            "n_restored": n_updated,
            "sample_ids": sample_ids,
        }

    def get(self, sample_id: str) -> dict | None:
        """Get a single sample by ID.

        Returns:
            Sample property dict, or None if not found.
        """
        with self._conn.driver.session() as session:
            result = session.run(GET_SAMPLE, {"sample_id": sample_id})
            record = result.single()
            return dict(record) if record else None

    def list(
        self,
        *,
        population: str | None = None,
        show_excluded: bool = False,
    ) -> list[dict]:
        """List samples, optionally filtered by population and exclusion status.

        Args:
            population: If set, filter to this population only.
            show_excluded: If False, only return active (non-excluded) samples.

        Returns:
            List of sample property dicts.
        """
        if population:
            query = (
                LIST_SAMPLES_BY_POPULATION_WITH_EXCLUDED
                if show_excluded
                else LIST_SAMPLES_BY_POPULATION
            )
            params: dict = {"population": population}
        else:
            query = LIST_ALL_SAMPLES_WITH_EXCLUDED if show_excluded else LIST_ALL_SAMPLES
            params = {}

        with self._conn.driver.session() as session:
            result = session.run(query, params)
            samples = [dict(record) for record in result]

        return samples

    def count(self) -> dict:
        """Count samples by status.

        Returns:
            Dict with total, active, excluded counts.
        """
        with self._conn.driver.session() as session:
            result = session.run(COUNT_SAMPLES_BY_STATUS)
            record = result.single()
            if record:
                return {
                    "total": record["total"],
                    "active": record["active"],
                    "excluded": record["excluded"],
                }
            return {"total": 0, "active": 0, "excluded": 0}

    def reassign(
        self,
        sample_ids: list[str],
        new_population: str,
        *,
        batch_size: int = 100_000,
    ) -> dict:
        """Reassign samples to a different population.

        Updates IN_POPULATION edges, Population node counts, and all variant
        population arrays (ac, an, af, het_count, hom_alt_count, het_exp).

        Args:
            sample_ids: Sample IDs to reassign.
            new_population: Target population ID.
            batch_size: Variant batch size for processing.

        Returns:
            Summary dict with n_reassigned, new_population, n_variants_updated.
        """
        if not sample_ids:
            raise ValueError("No sample IDs provided.")

        # 1. Validate: get sample info (packed_index, current population)
        samples_info = []
        for sid in sample_ids:
            info = self.get(sid)
            if info is None:
                raise ValueError(f"Sample {sid!r} not found.")
            if info["population"] == new_population:
                raise ValueError(f"Sample {sid!r} is already in population {new_population!r}.")
            samples_info.append(info)

        # Verify target population exists
        with self._conn.driver.session() as session:
            result = session.run(
                "MATCH (p:Population {populationId: $pid}) RETURN p",
                {"pid": new_population},
            )
            if not result.single():
                raise ValueError(f"Population {new_population!r} does not exist.")

        # Group by old population for batch processing
        by_old_pop: dict[str, list[dict]] = {}
        for info in samples_info:
            by_old_pop.setdefault(info["population"], []).append(info)

        # 2. Update IN_POPULATION edges
        with self._conn.driver.session() as session:
            for sid in sample_ids:
                session.run(
                    REASSIGN_SAMPLE_POPULATION,
                    {"sample_id": sid, "new_population": new_population},
                )

        # 3. Update Population node counts
        pop_deltas: dict[str, int] = {}
        for info in samples_info:
            pop_deltas[info["population"]] = pop_deltas.get(info["population"], 0) - 1
        pop_deltas[new_population] = pop_deltas.get(new_population, 0) + len(sample_ids)

        pop_updates = []
        with self._conn.driver.session() as session:
            for pid, delta in pop_deltas.items():
                result = session.run(
                    "MATCH (p:Population {populationId: $pid}) " "RETURN p.n_samples AS n",
                    {"pid": pid},
                )
                rec = result.single()
                new_n = (rec["n"] if rec else 0) + delta
                pop_updates.append(
                    {
                        "populationId": pid,
                        "n_samples": new_n,
                        "a_n": sum(1.0 / i for i in range(1, 2 * new_n)) if new_n > 0 else 0.0,
                        "a_n2": (
                            sum(1.0 / (i * i) for i in range(1, 2 * new_n)) if new_n > 0 else 0.0
                        ),
                    }
                )
            session.run(UPDATE_POPULATION_COUNTS, {"pops": pop_updates})

        # 4. Update variant population arrays chromosome by chromosome
        n_variants_updated = 0
        with self._conn.driver.session() as session:
            # Get all chromosomes
            chr_result = session.run("MATCH (c:Chromosome) RETURN c.chromosomeId AS chr")
            chromosomes = [r["chr"] for r in chr_result]

        for chrom in chromosomes:
            with self._conn.driver.session() as session:
                id_result = session.run(FETCH_VARIANT_IDS_BY_CHR, {"chr": chrom})
                all_vids = [r["variantId"] for r in id_result]

            for batch_start in range(0, len(all_vids), batch_size):
                batch_vids = all_vids[batch_start : batch_start + batch_size]
                with self._conn.driver.session() as session:
                    var_result = session.run(FETCH_VARIANT_BATCH, {"variant_ids": batch_vids})
                    variants = [dict(r) for r in var_result]

                updates = []
                for var in variants:
                    gt_packed = var["gt_packed"]
                    if gt_packed is None:
                        continue
                    if isinstance(gt_packed, (list, bytearray)):
                        gt_packed = bytes(gt_packed)

                    n_total = len(gt_packed) * 4
                    gt_all = unpack_genotypes(gt_packed, n_total)

                    # Process each old_pop group
                    current_pop_ids = list(var["pop_ids"])
                    current_ac = list(var["ac"])
                    current_an = list(var["an"])
                    current_het = list(var["het_count"])
                    current_hom = list(var["hom_alt_count"])

                    for old_pop, group_infos in by_old_pop.items():
                        group_indices = np.array(
                            [i["packed_index"] for i in group_infos], dtype=np.int64
                        )
                        safe_indices = group_indices[group_indices < len(gt_all)]
                        if len(safe_indices) == 0:
                            continue
                        gt_codes = gt_all[safe_indices]

                        updated = reassign_pop_stats(
                            current_pop_ids,
                            current_ac,
                            current_an,
                            current_het,
                            current_hom,
                            gt_codes,
                            old_pop,
                            new_population,
                        )
                        current_pop_ids = updated["pop_ids"]
                        current_ac = updated["ac"]
                        current_an = updated["an"]
                        current_het = updated["het_count"]
                        current_hom = updated["hom_alt_count"]

                    # Recompute derived
                    m_af = [a / n if n > 0 else 0.0 for a, n in zip(current_ac, current_an)]
                    m_het_exp = [2.0 * f * (1.0 - f) for f in m_af]
                    ac_total = sum(current_ac)
                    an_total = sum(current_an)
                    af_total = ac_total / an_total if an_total > 0 else 0.0

                    updates.append(
                        {
                            "variantId": var["variantId"],
                            "pop_ids": current_pop_ids,
                            "ac": current_ac,
                            "an": current_an,
                            "af": m_af,
                            "het_count": current_het,
                            "hom_alt_count": current_hom,
                            "het_exp": m_het_exp,
                            "ac_total": ac_total,
                            "an_total": an_total,
                            "af_total": af_total,
                            "call_rate": var["call_rate"],
                        }
                    )

                if updates:
                    with self._conn.driver.session() as session:
                        session.run(UPDATE_VARIANT_POP_STATS_BATCH, {"updates": updates})
                    n_variants_updated += len(updates)

        logger.info(
            "Reassigned %d samples to %s, updated %d variants",
            len(sample_ids),
            new_population,
            n_variants_updated,
        )

        return {
            "n_reassigned": len(sample_ids),
            "new_population": new_population,
            "n_variants_updated": n_variants_updated,
        }

    def hard_remove(
        self,
        sample_ids: list[str],
        *,
        require_soft_deleted: bool = False,
        batch_size: int = 100_000,
    ) -> dict:
        """Permanently remove samples by zeroing packed arrays and recalculating stats.

        Overwrites sample genotype slots with Missing (code 3), zeros phase bits,
        subtracts contributions from population statistics, then deletes the
        Sample node.

        Args:
            sample_ids: Sample IDs to permanently remove.
            require_soft_deleted: If True, only remove samples that are
                already soft-deleted (excluded=true).
            batch_size: Variant batch size for processing.

        Returns:
            Summary dict with n_removed, n_variants_updated,
            populations_affected.
        """
        if not sample_ids:
            raise ValueError("No sample IDs provided.")

        # 1. Validate and collect sample info
        samples_info = []
        for sid in sample_ids:
            info = self.get(sid)
            if info is None:
                raise ValueError(f"Sample {sid!r} not found.")
            if require_soft_deleted and not info.get("excluded"):
                raise ValueError(
                    f"Sample {sid!r} is not soft-deleted. "
                    f"Use --require-soft-deleted=false or soft-delete first."
                )
            samples_info.append(info)

        # 2. Group by population
        by_pop: dict[str, list[dict]] = {}
        for info in samples_info:
            by_pop.setdefault(info["population"], []).append(info)

        all_packed_indices = [info["packed_index"] for info in samples_info]

        # 3. Get total active sample count for call_rate recalculation
        with self._conn.driver.session() as session:
            result = session.run(
                f"MATCH (s:Sample) WHERE {ACTIVE_SAMPLE_FILTER} " "RETURN count(s) AS n"
            )
            total_samples = result.single()["n"]
        new_total_samples = total_samples - len(sample_ids)

        # 4. Process variants chromosome by chromosome
        n_variants_updated = 0
        with self._conn.driver.session() as session:
            chr_result = session.run("MATCH (c:Chromosome) RETURN c.chromosomeId AS chr")
            chromosomes = [r["chr"] for r in chr_result]

        for chrom in chromosomes:
            with self._conn.driver.session() as session:
                id_result = session.run(FETCH_VARIANT_IDS_BY_CHR, {"chr": chrom})
                all_vids = [r["variantId"] for r in id_result]

            for batch_start in range(0, len(all_vids), batch_size):
                batch_vids = all_vids[batch_start : batch_start + batch_size]
                with self._conn.driver.session() as session:
                    var_result = session.run(FETCH_VARIANT_BATCH, {"variant_ids": batch_vids})
                    variants = [dict(r) for r in var_result]

                updates = []
                for var in variants:
                    gt_packed = var["gt_packed"]
                    if gt_packed is None:
                        continue
                    if isinstance(gt_packed, (list, bytearray)):
                        gt_packed = bytes(gt_packed)

                    phase_packed = var.get("phase_packed")
                    if isinstance(phase_packed, (list, bytearray)):
                        phase_packed = bytes(phase_packed)

                    n_total = len(gt_packed) * 4
                    gt_all = unpack_genotypes(gt_packed, n_total)

                    # Zero out gt_packed and phase_packed
                    new_gt = zero_out_gt_packed(gt_packed, all_packed_indices)
                    new_phase = phase_packed
                    if phase_packed:
                        new_phase = zero_out_phase_packed(phase_packed, all_packed_indices)

                    # Subtract contributions per population
                    current_pop_ids = list(var["pop_ids"])
                    current_ac = list(var["ac"])
                    current_an = list(var["an"])
                    current_het = list(var["het_count"])
                    current_hom = list(var["hom_alt_count"])

                    for pop_id, group_infos in by_pop.items():
                        group_indices = np.array(
                            [i["packed_index"] for i in group_infos], dtype=np.int64
                        )
                        safe_indices = group_indices[group_indices < len(gt_all)]
                        if len(safe_indices) == 0:
                            continue
                        gt_codes = gt_all[safe_indices]

                        updated = subtract_sample_from_pop_stats(
                            current_pop_ids,
                            current_ac,
                            current_an,
                            current_het,
                            current_hom,
                            gt_codes,
                            pop_id,
                        )
                        current_pop_ids = updated["pop_ids"]
                        current_ac = updated["ac"]
                        current_an = updated["an"]
                        current_het = updated["het_count"]
                        current_hom = updated["hom_alt_count"]

                    # Recompute derived stats
                    m_af = [a / n if n > 0 else 0.0 for a, n in zip(current_ac, current_an)]
                    m_het_exp = [2.0 * f * (1.0 - f) for f in m_af]
                    ac_total = sum(current_ac)
                    an_total = sum(current_an)
                    af_total = ac_total / an_total if an_total > 0 else 0.0

                    # Recompute call_rate
                    if new_total_samples > 0:
                        n_called = an_total // 2
                        call_rate = n_called / new_total_samples
                    else:
                        call_rate = 0.0

                    updates.append(
                        {
                            "variantId": var["variantId"],
                            "gt_packed": list(new_gt),
                            "phase_packed": list(new_phase) if new_phase else None,
                            "pop_ids": current_pop_ids,
                            "ac": current_ac,
                            "an": current_an,
                            "af": m_af,
                            "het_count": current_het,
                            "hom_alt_count": current_hom,
                            "het_exp": m_het_exp,
                            "ac_total": ac_total,
                            "an_total": an_total,
                            "af_total": af_total,
                            "call_rate": call_rate,
                        }
                    )

                if updates:
                    with self._conn.driver.session() as session:
                        session.run(UPDATE_VARIANT_HARD_DELETE_BATCH, {"updates": updates})
                    n_variants_updated += len(updates)

        # 5. Update Population node counts
        populations_affected = set()
        pop_updates = []
        with self._conn.driver.session() as session:
            for pop_id, group_infos in by_pop.items():
                populations_affected.add(pop_id)
                result = session.run(
                    "MATCH (p:Population {populationId: $pid}) " "RETURN p.n_samples AS n",
                    {"pid": pop_id},
                )
                rec = result.single()
                new_n = max(0, (rec["n"] if rec else 0) - len(group_infos))
                pop_updates.append(
                    {
                        "populationId": pop_id,
                        "n_samples": new_n,
                        "a_n": sum(1.0 / i for i in range(1, 2 * new_n)) if new_n > 0 else 0.0,
                        "a_n2": (
                            sum(1.0 / (i * i) for i in range(1, 2 * new_n)) if new_n > 0 else 0.0
                        ),
                    }
                )
            session.run(UPDATE_POPULATION_COUNTS, {"pops": pop_updates})

        # 6. Delete empty populations and count how many were removed
        n_populations_deleted = 0
        with self._conn.driver.session() as session:
            for pop_id in populations_affected:
                result = session.run(DELETE_POPULATION_IF_EMPTY, {"population_id": pop_id})
                record = result.single()
                if record and record["deleted"] > 0:
                    n_populations_deleted += 1

        # 7. Delete sample nodes
        with self._conn.driver.session() as session:
            for sid in sample_ids:
                session.run(DELETE_SAMPLE_NODE, {"sample_id": sid})

        # 8. Update SchemaMetadata (samples and populations)
        with self._conn.driver.session() as session:
            session.run(
                DECREMENT_SCHEMA_SAMPLE_COUNT,
                {
                    "n_removed": len(sample_ids),
                    "modified_date": datetime.now(timezone.utc).isoformat(),
                },
            )
            if n_populations_deleted > 0:
                session.run(
                    "MATCH (m:SchemaMetadata) "
                    "SET m.n_populations = m.n_populations - $n_removed",
                    {"n_removed": n_populations_deleted},
                )

        logger.info(
            "Hard-removed %d samples, updated %d variants, %d populations affected",
            len(sample_ids),
            n_variants_updated,
            len(populations_affected),
        )

        return {
            "n_removed": len(sample_ids),
            "n_variants_updated": n_variants_updated,
            "populations_affected": sorted(populations_affected),
            "n_populations_deleted": n_populations_deleted,
        }


def load_sample_ids_from_file(path: str | Path) -> list[str]:
    """Read sample IDs from a file (one per line).

    Args:
        path: Path to text file with one sample ID per line.

    Returns:
        List of sample ID strings (empty lines and comments skipped).
    """
    ids: list[str] = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                ids.append(line)
    return ids
