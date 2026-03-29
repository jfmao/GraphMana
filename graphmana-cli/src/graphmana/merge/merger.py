"""Database merger — merge a source GraphMana database into a target.

Reads samples, variants, and population data from a source Neo4j database
and merges them into a target database by concatenating packed genotype
arrays, merging population statistics, and creating new nodes as needed.

The algorithm mirrors IncrementalIngester but operates on two live Neo4j
connections instead of a VCF file + one connection.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone

import numpy as np

from graphmana.db import queries
from graphmana.ingest.array_ops import (
    _pack_bits,
    _pack_codes_direct,
    concatenate_gt_packed,
    concatenate_phase_packed,
    concatenate_ploidy_packed,
    merge_pop_stats,
)
from graphmana.ingest.genotype_packer import unpack_genotypes

logger = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = 500


class MergeValidationError(Exception):
    """Raised when pre-merge validation fails."""


@dataclass
class DatabaseMerger:
    """Merges a source GraphMana database into a target database.

    Both source_conn and target_conn must be live GraphManaConnection
    instances (context-managed). The source is read-only; the target
    receives all writes.

    Args:
        source_conn: GraphManaConnection to the source database.
        target_conn: GraphManaConnection to the target database.
        on_duplicate_sample: "error" to fail on overlapping sample IDs,
            "skip" to silently skip duplicate samples from source.
        dry_run: if True, validate without modifying the target.
    """

    source_conn: object
    target_conn: object
    on_duplicate_sample: str = "error"
    dry_run: bool = False

    # Internal state (set during run)
    _n_target_samples: int = field(default=0, init=False)
    _n_source_samples: int = field(default=0, init=False)
    _target_sample_ids: set = field(default_factory=set, init=False)
    _source_samples: list = field(default_factory=list, init=False)
    _source_pop_ids: list = field(default_factory=list, init=False)
    _target_pop_ids: list = field(default_factory=list, init=False)
    _source_pop_n_samples: dict = field(default_factory=dict, init=False)
    _skipped_sample_ids: set = field(default_factory=set, init=False)

    # Counters
    n_variants_extended: int = field(default=0, init=False)
    n_variants_homref_extended: int = field(default=0, init=False)
    n_variants_created: int = field(default=0, init=False)
    n_samples_merged: int = field(default=0, init=False)
    n_populations_created: int = field(default=0, init=False)
    n_chromosomes_processed: int = field(default=0, init=False)

    def run(self, *, batch_size: int = DEFAULT_BATCH_SIZE) -> dict:
        """Execute the full database merge.

        Args:
            batch_size: number of variants per Cypher batch transaction.

        Returns:
            Summary dict with counts.

        Raises:
            MergeValidationError: if pre-merge validation fails.
        """
        self._validate()

        if self.dry_run:
            logger.info("Dry run complete — no modifications made.")
            return self._summary()

        self._merge_samples(batch_size)
        self._merge_variants(batch_size)
        self._update_metadata()

        logger.info(
            "Merge complete: %d variants extended, %d homref-extended, "
            "%d new variants, %d samples merged, %d populations created",
            self.n_variants_extended,
            self.n_variants_homref_extended,
            self.n_variants_created,
            self.n_samples_merged,
            self.n_populations_created,
        )
        return self._summary()

    def _summary(self) -> dict:
        return {
            "n_variants_extended": self.n_variants_extended,
            "n_variants_homref_extended": self.n_variants_homref_extended,
            "n_variants_created": self.n_variants_created,
            "n_samples_merged": self.n_samples_merged,
            "n_populations_created": self.n_populations_created,
            "n_chromosomes_processed": self.n_chromosomes_processed,
            "n_skipped_samples": len(self._skipped_sample_ids),
            "dry_run": self.dry_run,
        }

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def _validate(self) -> None:
        """Pre-merge validation: reference genome, chr style, schema, samples."""
        source_meta = self._get_schema_metadata(self.source_conn, "source")
        target_meta = self._get_schema_metadata(self.target_conn, "target")

        # Reference genome must match
        src_ref = source_meta.get("reference_genome", "unknown")
        tgt_ref = target_meta.get("reference_genome", "unknown")
        if src_ref != "unknown" and tgt_ref != "unknown" and src_ref != tgt_ref:
            raise MergeValidationError(
                f"Reference genome mismatch: source={src_ref}, target={tgt_ref}. "
                "Run graphmana liftover on one database first."
            )

        # Chr naming style must match
        src_style = source_meta.get("chr_naming_style", "auto")
        tgt_style = target_meta.get("chr_naming_style", "auto")
        if src_style != "auto" and tgt_style != "auto" and src_style != tgt_style:
            raise MergeValidationError(
                f"Chromosome naming style mismatch: source={src_style}, " f"target={tgt_style}."
            )

        # Schema version compatibility
        src_ver = source_meta.get("schema_version", "1")
        tgt_ver = target_meta.get("schema_version", "1")
        if src_ver != tgt_ver:
            raise MergeValidationError(
                f"Schema version mismatch: source={src_ver}, target={tgt_ver}. "
                "Run graphmana migrate on the older database first."
            )

        # Load sample information
        self._load_sample_info()

        # Check for duplicate samples
        source_ids = {s["sampleId"] for s in self._source_samples}
        overlap = source_ids & self._target_sample_ids
        if overlap:
            if self.on_duplicate_sample == "error":
                raise MergeValidationError(
                    f"Duplicate sample IDs found in both databases: "
                    f"{sorted(overlap)[:10]}{'...' if len(overlap) > 10 else ''}. "
                    f"Use --on-duplicate-sample skip to skip them."
                )
            else:
                self._skipped_sample_ids = overlap
                logger.info(
                    "Skipping %d duplicate sample(s): %s",
                    len(overlap),
                    sorted(overlap)[:5],
                )

        # Warn about annotations
        source_ann = self.source_conn.execute_read(
            "MATCH (a:AnnotationVersion) RETURN count(a) AS c"
        ).single()
        if source_ann and source_ann["c"] > 0:
            logger.warning(
                "Source database has %d annotation version(s). "
                "Annotations are NOT merged — only genotype data is transferred.",
                source_ann["c"],
            )

        n_effective = len(self._source_samples) - len(self._skipped_sample_ids)
        logger.info(
            "Validation passed: merging %d source samples into %d target samples " "(total: %d).",
            n_effective,
            self._n_target_samples,
            self._n_target_samples + n_effective,
        )

    def _get_schema_metadata(self, conn, label: str) -> dict:
        result = conn.execute_read(queries.GET_SCHEMA_METADATA)
        rec = result.single()
        if rec is None:
            raise MergeValidationError(
                f"No SchemaMetadata node found in {label} database. "
                "Is this a valid GraphMana database?"
            )
        return dict(rec["m"])

    def _load_sample_info(self) -> None:
        """Load sample and population info from both databases."""
        # Target samples
        result = self.target_conn.execute_read(queries.FETCH_EXISTING_SAMPLE_IDS)
        rec = result.single()
        self._target_sample_ids = set(rec["ids"]) if rec and rec["ids"] else set()
        self._n_target_samples = len(self._target_sample_ids)

        # Target populations
        result = self.target_conn.execute_read(queries.FETCH_EXISTING_POP_IDS)
        rec = result.single()
        self._target_pop_ids = sorted(rec["ids"]) if rec and rec["ids"] else []

        # Source samples
        result = self.source_conn.execute_read(queries.FETCH_SOURCE_SAMPLES)
        self._source_samples = [dict(r) for r in result]
        self._n_source_samples = len(self._source_samples)

        # Source populations
        result = self.source_conn.execute_read(queries.FETCH_SOURCE_POPULATIONS)
        source_pops = [dict(r) for r in result]
        self._source_pop_ids = [p["populationId"] for p in source_pops]
        self._source_pop_n_samples = {p["populationId"]: p["n_samples"] for p in source_pops}

    # ------------------------------------------------------------------
    # Sample merging
    # ------------------------------------------------------------------

    def _merge_samples(self, batch_size: int) -> None:
        """Create Sample + Population nodes in target for source samples."""
        # Get the next packed_index offset
        result = self.target_conn.execute_read(queries.FETCH_MAX_PACKED_INDEX)
        rec = result.single()
        max_idx = rec["max_idx"] if rec and rec["max_idx"] is not None else -1
        packed_index_offset = max_idx + 1

        # Filter out skipped samples, assign new packed indices
        now = datetime.now(timezone.utc).isoformat()
        samples_to_create = []
        edges_to_create = []
        idx = 0
        effective_pop_counts: dict[str, int] = {}

        for s in self._source_samples:
            if s["sampleId"] in self._skipped_sample_ids:
                continue

            new_packed_index = packed_index_offset + idx
            samples_to_create.append(
                {
                    "sampleId": s["sampleId"],
                    "population": s["population"],
                    "packed_index": new_packed_index,
                    "sex": s.get("sex", 0),
                    "source_dataset": s.get("source_dataset", "merge"),
                    "source_file": s.get("source_file", "merge"),
                    "ingestion_date": now,
                }
            )
            edges_to_create.append({"sampleId": s["sampleId"], "populationId": s["population"]})
            effective_pop_counts[s["population"]] = effective_pop_counts.get(s["population"], 0) + 1
            idx += 1

        # Create/update Population nodes
        self._merge_populations(effective_pop_counts)

        # Create Sample nodes in batches
        for i in range(0, len(samples_to_create), batch_size):
            self.target_conn.execute_write(
                queries.CREATE_SAMPLE_BATCH,
                {"samples": samples_to_create[i : i + batch_size]},
            )

        # Create IN_POPULATION edges in batches
        for i in range(0, len(edges_to_create), batch_size):
            self.target_conn.execute_write(
                queries.CREATE_IN_POPULATION_BATCH,
                {"edges": edges_to_create[i : i + batch_size]},
            )

        self.n_samples_merged = len(samples_to_create)
        # Update effective source sample count (after skipping duplicates)
        self._n_source_samples = self.n_samples_merged
        # Update per-population counts to reflect skipped samples
        self._source_pop_n_samples = dict(effective_pop_counts)

    def _merge_populations(self, effective_pop_counts: dict[str, int]) -> None:
        """Create new populations and update existing population counts."""
        existing_pop_set = set(self._target_pop_ids)

        for pid, n_new in sorted(effective_pop_counts.items()):
            if pid not in existing_pop_set:
                # Create new population
                a_n = sum(1.0 / i for i in range(1, max(2 * n_new, 2))) if n_new > 0 else 0.0
                a_n2 = sum(1.0 / (i * i) for i in range(1, max(2 * n_new, 2))) if n_new > 0 else 0.0
                self.target_conn.execute_write(
                    queries.MERGE_POPULATION,
                    {
                        "populationId": pid,
                        "name": pid,
                        "n_samples": n_new,
                        "a_n": a_n,
                        "a_n2": a_n2,
                    },
                )
                self.n_populations_created += 1
            else:
                # Update existing population count
                result = self.target_conn.execute_read(
                    "MATCH (p:Population {populationId: $pid}) " "RETURN p.n_samples AS n",
                    {"pid": pid},
                )
                rec = result.single()
                old_n = rec["n"] if rec else 0
                new_n = old_n + n_new
                a_n = sum(1.0 / i for i in range(1, max(2 * new_n, 2))) if new_n > 0 else 0.0
                a_n2 = sum(1.0 / (i * i) for i in range(1, max(2 * new_n, 2))) if new_n > 0 else 0.0
                self.target_conn.execute_write(
                    queries.UPDATE_POPULATION_COUNTS,
                    {
                        "pops": [
                            {
                                "populationId": pid,
                                "n_samples": new_n,
                                "a_n": a_n,
                                "a_n2": a_n2,
                            }
                        ]
                    },
                )

    # ------------------------------------------------------------------
    # Variant merging
    # ------------------------------------------------------------------

    def _merge_variants(self, batch_size: int) -> None:
        """Merge variants chromosome by chromosome."""
        # Get chromosomes from both databases
        source_chrs = self._fetch_chromosomes(self.source_conn)
        target_chrs = self._fetch_chromosomes(self.target_conn)
        all_chrs = sorted(set(source_chrs) | set(target_chrs))

        n_total = self._n_target_samples + self._n_source_samples

        for chrom in all_chrs:
            in_source = chrom in set(source_chrs)
            in_target = chrom in set(target_chrs)

            if in_source and in_target:
                self._merge_chromosome(chrom, batch_size, n_total)
            elif in_source:
                # Chromosome only in source — create all variants with HomRef
                # padding for target samples, plus Chromosome node
                self._create_chromosome_from_source(chrom, batch_size, n_total)
            else:
                # Chromosome only in target — extend all variants with
                # HomRef for source samples
                self._homref_extend_chromosome(chrom, batch_size, n_total)

            self.n_chromosomes_processed += 1

    def _fetch_chromosomes(self, conn) -> list[str]:
        result = conn.execute_read(queries.FETCH_CHROMOSOMES)
        return [r["chr"] for r in result]

    def _merge_chromosome(self, chrom: str, batch_size: int, n_total: int) -> None:
        """Merge a chromosome present in both source and target."""
        # Fetch variant IDs from both databases
        source_result = self.source_conn.execute_read(
            queries.FETCH_VARIANT_IDS_BY_CHR, {"chr": chrom}
        )
        source_ids = [r["variantId"] for r in source_result]
        source_id_set = set(source_ids)

        target_result = self.target_conn.execute_read(
            queries.FETCH_VARIANT_IDS_BY_CHR, {"chr": chrom}
        )
        target_ids = [r["variantId"] for r in target_result]
        target_id_set = set(target_ids)

        # Partition into three buckets
        to_extend = [vid for vid in target_ids if vid in source_id_set]
        to_homref_extend = [vid for vid in target_ids if vid not in source_id_set]
        to_create = [vid for vid in source_ids if vid not in target_id_set]

        # Process extend bucket
        for i in range(0, len(to_extend), batch_size):
            batch = to_extend[i : i + batch_size]
            self._extend_batch(batch, chrom, n_total)
            self.n_variants_extended += len(batch)

        # Process homref-extend bucket
        for i in range(0, len(to_homref_extend), batch_size):
            batch = to_homref_extend[i : i + batch_size]
            self._homref_extend_batch(batch, n_total)
            self.n_variants_homref_extended += len(batch)

        # Process create bucket
        if to_create:
            for i in range(0, len(to_create), batch_size):
                batch = to_create[i : i + batch_size]
                self._create_batch(batch, chrom, n_total)
                self.n_variants_created += len(batch)
            # Rebuild NEXT chain since new variants were inserted
            self._rebuild_next_chain(chrom)

        logger.info(
            "Chromosome %s: %d extended, %d homref-extended, %d created",
            chrom,
            len(to_extend),
            len(to_homref_extend),
            len(to_create),
        )

    def _extend_batch(self, variant_ids: list[str], chrom: str, n_total: int) -> None:
        """Extend target variants with source genotypes (variant in both DBs)."""
        # Read source variant data
        source_data = self._fetch_source_variant_data(variant_ids)

        def _tx(tx, variant_ids, source_data, n_target, n_source, n_total):
            # Read target variant data
            result = tx.run(queries.FETCH_VARIANT_BATCH, {"variant_ids": variant_ids})
            records = list(result)

            updates = []
            for rec in records:
                vid = rec["variantId"]
                src = source_data.get(vid)
                if src is None:
                    continue

                target_gt = bytes(rec["gt_packed"])
                target_phase = bytes(rec["phase_packed"])
                target_ploidy = rec["ploidy_packed"]
                if target_ploidy is not None:
                    target_ploidy = bytes(target_ploidy)

                source_gt = bytes(src["gt_packed"])
                source_phase = bytes(src["phase_packed"])
                source_ploidy = src["ploidy_packed"]
                if source_ploidy is not None:
                    source_ploidy = bytes(source_ploidy)

                # Concatenate packed arrays
                new_gt = concatenate_gt_packed(target_gt, n_target, source_gt, n_source)
                new_phase = concatenate_phase_packed(target_phase, n_target, source_phase, n_source)
                new_ploidy = concatenate_ploidy_packed(
                    target_ploidy, n_target, source_ploidy, n_source
                )

                # Merge population stats
                tgt_pop_ids = list(rec["pop_ids"]) if rec["pop_ids"] else []
                tgt_ac = list(rec["ac"]) if rec["ac"] else []
                tgt_an = list(rec["an"]) if rec["an"] else []
                tgt_het = list(rec["het_count"]) if rec["het_count"] else []
                tgt_hom = list(rec["hom_alt_count"]) if rec["hom_alt_count"] else []

                src_pop_ids = list(src["pop_ids"]) if src["pop_ids"] else []
                src_ac = list(src["ac"]) if src["ac"] else []
                src_an = list(src["an"]) if src["an"] else []
                src_het = list(src["het_count"]) if src["het_count"] else []
                src_hom = list(src["hom_alt_count"]) if src["hom_alt_count"] else []

                merged = merge_pop_stats(
                    tgt_pop_ids,
                    tgt_ac,
                    tgt_an,
                    tgt_het,
                    tgt_hom,
                    src_pop_ids,
                    src_ac,
                    src_an,
                    src_het,
                    src_hom,
                )

                total_an_diploid = 2 * n_total
                call_rate = merged["an_total"] / total_an_diploid if total_an_diploid > 0 else 0.0

                updates.append(
                    {
                        "variantId": vid,
                        "gt_packed": bytearray(new_gt),
                        "phase_packed": bytearray(new_phase),
                        "ploidy_packed": bytearray(new_ploidy) if new_ploidy else None,
                        "pop_ids": merged["pop_ids"],
                        "ac": merged["ac"],
                        "an": merged["an"],
                        "af": merged["af"],
                        "het_count": merged["het_count"],
                        "hom_alt_count": merged["hom_alt_count"],
                        "het_exp": merged["het_exp"],
                        "ac_total": merged["ac_total"],
                        "an_total": merged["an_total"],
                        "af_total": merged["af_total"],
                        "call_rate": call_rate,
                    }
                )

            if updates:
                tx.run(queries.UPDATE_VARIANT_BATCH, {"updates": updates})

        self.target_conn.execute_write_tx(
            _tx,
            variant_ids=variant_ids,
            source_data=source_data,
            n_target=self._n_target_samples,
            n_source=self._n_source_samples,
            n_total=n_total,
        )

    def _homref_extend_batch(self, variant_ids: list[str], n_total: int) -> None:
        """Extend target-only variants with HomRef for all source samples."""
        n_source = self._n_source_samples
        # Build HomRef packed arrays for source samples
        homref_codes = np.zeros(n_source, dtype=np.uint8)
        homref_gt = _pack_codes_direct(homref_codes)
        zero_phase = _pack_bits(np.zeros(n_source, dtype=np.uint8))

        # HomRef pop stats for source samples
        src_pop_ids = sorted(self._source_pop_n_samples.keys())
        src_ac = [0] * len(src_pop_ids)
        src_an = [2 * self._source_pop_n_samples[pid] for pid in src_pop_ids]
        src_het = [0] * len(src_pop_ids)
        src_hom = [0] * len(src_pop_ids)

        def _tx(
            tx,
            variant_ids,
            homref_gt,
            zero_phase,
            src_pop_ids,
            src_ac,
            src_an,
            src_het,
            src_hom,
            n_target,
            n_source,
            n_total,
        ):
            result = tx.run(queries.FETCH_VARIANT_BATCH, {"variant_ids": variant_ids})
            records = list(result)

            updates = []
            for rec in records:
                vid = rec["variantId"]

                target_gt = bytes(rec["gt_packed"])
                target_phase = bytes(rec["phase_packed"])
                target_ploidy = rec["ploidy_packed"]
                if target_ploidy is not None:
                    target_ploidy = bytes(target_ploidy)

                new_gt = concatenate_gt_packed(target_gt, n_target, homref_gt, n_source)
                new_phase = concatenate_phase_packed(target_phase, n_target, zero_phase, n_source)
                new_ploidy = concatenate_ploidy_packed(target_ploidy, n_target, None, n_source)

                tgt_pop_ids = list(rec["pop_ids"]) if rec["pop_ids"] else []
                tgt_ac = list(rec["ac"]) if rec["ac"] else []
                tgt_an = list(rec["an"]) if rec["an"] else []
                tgt_het = list(rec["het_count"]) if rec["het_count"] else []
                tgt_hom = list(rec["hom_alt_count"]) if rec["hom_alt_count"] else []

                merged = merge_pop_stats(
                    tgt_pop_ids,
                    tgt_ac,
                    tgt_an,
                    tgt_het,
                    tgt_hom,
                    src_pop_ids,
                    src_ac,
                    src_an,
                    src_het,
                    src_hom,
                )

                total_an_diploid = 2 * n_total
                call_rate = merged["an_total"] / total_an_diploid if total_an_diploid > 0 else 0.0

                updates.append(
                    {
                        "variantId": vid,
                        "gt_packed": bytearray(new_gt),
                        "phase_packed": bytearray(new_phase),
                        "ploidy_packed": bytearray(new_ploidy) if new_ploidy else None,
                        "pop_ids": merged["pop_ids"],
                        "ac": merged["ac"],
                        "an": merged["an"],
                        "af": merged["af"],
                        "het_count": merged["het_count"],
                        "hom_alt_count": merged["hom_alt_count"],
                        "het_exp": merged["het_exp"],
                        "ac_total": merged["ac_total"],
                        "an_total": merged["an_total"],
                        "af_total": merged["af_total"],
                        "call_rate": call_rate,
                    }
                )

            if updates:
                tx.run(queries.UPDATE_VARIANT_BATCH, {"updates": updates})

        self.target_conn.execute_write_tx(
            _tx,
            variant_ids=variant_ids,
            homref_gt=homref_gt,
            zero_phase=zero_phase,
            src_pop_ids=src_pop_ids,
            src_ac=src_ac,
            src_an=src_an,
            src_het=src_het,
            src_hom=src_hom,
            n_target=self._n_target_samples,
            n_source=n_source,
            n_total=n_total,
        )

    def _create_batch(self, variant_ids: list[str], chrom: str, n_total: int) -> None:
        """Create source-only variants in target with HomRef padding for target samples."""
        source_data = self._fetch_source_variant_data(variant_ids)
        n_target = self._n_target_samples
        n_source = self._n_source_samples

        # HomRef stats for target populations
        target_homref_an = self._get_target_pop_an()
        tgt_pop_ids = self._target_pop_ids
        tgt_ac = [0] * len(tgt_pop_ids)
        tgt_het = [0] * len(tgt_pop_ids)
        tgt_hom = [0] * len(tgt_pop_ids)

        variants = []
        edges = []

        for vid in variant_ids:
            src = source_data.get(vid)
            if src is None:
                continue

            source_gt = bytes(src["gt_packed"])
            source_phase = bytes(src["phase_packed"])
            source_ploidy = src["ploidy_packed"]
            if source_ploidy is not None:
                source_ploidy = bytes(source_ploidy)

            # Pad target samples as HomRef, then append source
            homref_codes = np.zeros(n_target, dtype=np.uint8)
            source_codes = unpack_genotypes(source_gt, n_source)
            all_codes = np.concatenate([homref_codes, source_codes])
            gt_packed = _pack_codes_direct(all_codes.astype(np.uint8))

            zero_phase_bits = np.zeros(n_target, dtype=np.uint8)
            from graphmana.ingest.genotype_packer import unpack_phase

            source_phase_bits = unpack_phase(source_phase, n_source)
            all_phase = np.concatenate([zero_phase_bits, source_phase_bits])
            phase_packed = _pack_bits(all_phase)

            ploidy_packed = concatenate_ploidy_packed(None, n_target, source_ploidy, n_source)

            # Merge pop stats: HomRef for target pops + source pop stats
            src_pop_ids = list(src["pop_ids"]) if src["pop_ids"] else []
            src_ac = list(src["ac"]) if src["ac"] else []
            src_an = list(src["an"]) if src["an"] else []
            src_het = list(src["het_count"]) if src["het_count"] else []
            src_hom = list(src["hom_alt_count"]) if src["hom_alt_count"] else []

            merged = merge_pop_stats(
                tgt_pop_ids,
                tgt_ac,
                target_homref_an,
                tgt_het,
                tgt_hom,
                src_pop_ids,
                src_ac,
                src_an,
                src_het,
                src_hom,
            )

            total_an_diploid = 2 * n_total
            call_rate = merged["an_total"] / total_an_diploid if total_an_diploid > 0 else 0.0

            variants.append(
                {
                    "variantId": vid,
                    "chr": src["chr"],
                    "pos": src["pos"],
                    "ref": src["ref"],
                    "alt": src["alt"],
                    "variant_type": src["variant_type"],
                    "gt_packed": bytearray(gt_packed),
                    "phase_packed": bytearray(phase_packed),
                    "ploidy_packed": bytearray(ploidy_packed) if ploidy_packed else None,
                    "pop_ids": merged["pop_ids"],
                    "ac": merged["ac"],
                    "an": merged["an"],
                    "af": merged["af"],
                    "het_count": merged["het_count"],
                    "hom_alt_count": merged["hom_alt_count"],
                    "het_exp": merged["het_exp"],
                    "ac_total": merged["ac_total"],
                    "an_total": merged["an_total"],
                    "af_total": merged["af_total"],
                    "call_rate": call_rate,
                }
            )
            edges.append({"variantId": vid, "chr": chrom})

        # Write to target in sub-batches
        sub_batch = 500
        for i in range(0, len(variants), sub_batch):
            self.target_conn.execute_write(
                queries.CREATE_VARIANT_BATCH,
                {"variants": variants[i : i + sub_batch]},
            )
            self.target_conn.execute_write(
                queries.CREATE_ON_CHROMOSOME_BATCH,
                {"edges": edges[i : i + sub_batch]},
            )

    def _create_chromosome_from_source(self, chrom: str, batch_size: int, n_total: int) -> None:
        """Create a chromosome (and all its variants) from source into target."""
        # Ensure the Chromosome node exists in target
        self.target_conn.execute_write("MERGE (c:Chromosome {chromosomeId: $chr})", {"chr": chrom})

        # Fetch all source variant IDs on this chromosome
        result = self.source_conn.execute_read(queries.FETCH_VARIANT_IDS_BY_CHR, {"chr": chrom})
        source_ids = [r["variantId"] for r in result]

        if source_ids:
            for i in range(0, len(source_ids), batch_size):
                batch = source_ids[i : i + batch_size]
                self._create_batch(batch, chrom, n_total)
                self.n_variants_created += len(batch)
            self._rebuild_next_chain(chrom)

        logger.info("Chromosome %s (source-only): %d variants created", chrom, len(source_ids))

    def _homref_extend_chromosome(self, chrom: str, batch_size: int, n_total: int) -> None:
        """Extend all target-only chromosome variants with HomRef for source samples."""
        result = self.target_conn.execute_read(queries.FETCH_VARIANT_IDS_BY_CHR, {"chr": chrom})
        target_ids = [r["variantId"] for r in result]

        for i in range(0, len(target_ids), batch_size):
            batch = target_ids[i : i + batch_size]
            self._homref_extend_batch(batch, n_total)
            self.n_variants_homref_extended += len(batch)

        logger.info(
            "Chromosome %s (target-only): %d variants homref-extended",
            chrom,
            len(target_ids),
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _fetch_source_variant_data(self, variant_ids: list[str]) -> dict:
        """Read full variant data from source DB for a batch of variant IDs."""
        result = self.source_conn.execute_read(
            queries.FETCH_VARIANT_BATCH, {"variant_ids": variant_ids}
        )
        return {r["variantId"]: dict(r) for r in result}

    def _get_target_pop_an(self) -> list[int]:
        """Get allele numbers for target populations (HomRef contribution).

        For a new variant, target samples are all HomRef = called.
        Each target population contributes an = 2 * n_samples.
        """
        result = self.target_conn.execute_read(
            "MATCH (p:Population) RETURN p.populationId AS pid, p.n_samples AS n "
            "ORDER BY p.populationId"
        )
        pop_an = {r["pid"]: 2 * r["n"] for r in result}
        return [pop_an.get(pid, 0) for pid in self._target_pop_ids]

    def _rebuild_next_chain(self, chrom: str) -> None:
        self.target_conn.execute_write(queries.DELETE_NEXT_CHAIN_FOR_CHR, {"chr": chrom})
        self.target_conn.execute_write(queries.REBUILD_NEXT_CHAIN_FOR_CHR, {"chr": chrom})

    def _update_metadata(self) -> None:
        from graphmana.db.schema import ensure_schema
        from graphmana.provenance.manager import ProvenanceManager

        ensure_schema(self.target_conn)

        # Record merge provenance
        prov = ProvenanceManager(self.target_conn)
        prov.record_ingestion(
            source_file="merge",
            dataset_id="merge",
            mode="merge",
            n_samples=self.n_samples_merged,
            n_variants=self.n_variants_extended + self.n_variants_created,
        )
