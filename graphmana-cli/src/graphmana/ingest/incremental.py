"""Incremental sample addition to an existing GraphMana database.

Extends packed genotype arrays and population statistics on existing Variant
nodes via Cypher MERGE/SET on a live Neo4j instance. Creates new Sample and
Population nodes for the added samples.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import numpy as np

from graphmana.db import queries
from graphmana.ingest.array_ops import (
    extend_gt_packed,
    extend_phase_packed,
    extend_ploidy_packed,
    merge_pop_stats,
    pad_gt_for_new_variant,
    pad_phase_for_new_variant,
)

logger = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = 500


@dataclass
class _VariantData:
    """Lightweight container for a new VCF variant's data needed for extension."""

    variant_id: str
    chr: str
    pos: int
    ref: str
    alt: str
    variant_type: str
    gt_types: np.ndarray  # cyvcf2 codes for new samples only
    phase_bits: np.ndarray  # 0/1 phase flags for new samples only
    ploidy_bits: np.ndarray  # 0/1 haploid flags for new samples only
    pop_ids: list[str]
    ac: list[int]
    an: list[int]
    af: list[float]
    het_count: list[int]
    hom_alt_count: list[int]
    het_exp: list[float]
    ac_total: int
    an_total: int
    af_total: float
    call_rate: float
    multiallelic_site: str | None = None
    allele_index: int | None = None


@dataclass
class IncrementalIngester:
    """Extends an existing GraphMana database with new samples.

    The ingester reads variants from a VCFParser (which covers only the new
    samples), then for each existing Variant node extends its packed arrays
    and merges population statistics. Variants in the new VCF but not in the
    DB are created with HomRef padding for existing samples.

    Args:
        conn: live GraphManaConnection.
        pop_map_new: PopulationMap for the new samples.
        n_existing: number of samples currently in the database.
        existing_sample_ids: set of existing sample IDs (for duplicate detection).
        existing_pop_ids: list of existing population IDs.
        packed_index_offset: first packed_index for new samples.
        dataset_id: dataset identifier for provenance.
        source_file: source VCF file path.
        n_total_samples: total samples after addition (existing + new).
    """

    conn: object
    pop_map_new: object  # PopulationMap
    n_existing: int
    existing_sample_ids: set
    existing_pop_ids: list[str]
    packed_index_offset: int
    dataset_id: str
    source_file: str
    n_total_samples: int

    # Counters
    n_variants_extended: int = field(default=0, init=False)
    n_variants_homref_extended: int = field(default=0, init=False)
    n_variants_created: int = field(default=0, init=False)
    n_samples_created: int = field(default=0, init=False)
    n_populations_created: int = field(default=0, init=False)

    def run(self, parser, *, chunk_size: int = DEFAULT_BATCH_SIZE, filter_chain=None) -> dict:
        """Run the full incremental ingestion pipeline.

        Processes the VCF chromosome-by-chromosome to limit memory usage.

        Args:
            parser: VCFParser for the new VCF file.
            chunk_size: number of variants per Cypher batch.
            filter_chain: optional ImportFilterChain.

        Returns:
            Summary dict with counts.
        """
        # 1. Collect new VCF variants grouped by chromosome
        chr_variants = self._collect_variants_by_chr(parser, filter_chain)

        # 2. Get all chromosomes from the DB
        existing_chrs = self._fetch_existing_chromosomes()

        # 3. Process each chromosome
        all_chrs = sorted(set(list(chr_variants.keys()) + existing_chrs))
        for chrom in all_chrs:
            new_on_chr = chr_variants.get(chrom, {})
            self._process_chromosome(chrom, new_on_chr, chunk_size)

        # 4. Create Sample nodes + IN_POPULATION edges
        self._create_sample_nodes()

        # 5. Update Population nodes
        self._update_populations()

        # 6. Update SchemaMetadata
        self._update_schema_metadata()

        summary = {
            "n_variants_extended": self.n_variants_extended,
            "n_variants_homref_extended": self.n_variants_homref_extended,
            "n_variants_created": self.n_variants_created,
            "n_samples_created": self.n_samples_created,
            "n_populations_created": self.n_populations_created,
            "n_total_samples": self.n_total_samples,
        }
        logger.info(
            "Incremental ingestion complete: %d extended, %d homref-extended, "
            "%d new variants, %d samples created",
            self.n_variants_extended,
            self.n_variants_homref_extended,
            self.n_variants_created,
            self.n_samples_created,
        )
        return summary

    def _collect_variants_by_chr(self, parser, filter_chain) -> dict[str, dict[str, _VariantData]]:
        """Iterate the new VCF and collect variant data grouped by chromosome.

        Returns:
            {chr: {variantId: _VariantData}}.
        """
        from graphmana.ingest.genotype_packer import unpack_phase

        chr_variants: dict[str, dict[str, _VariantData]] = {}
        n_new_samples = len(self.pop_map_new.sample_ids)

        for rec in parser:
            if filter_chain is not None and not filter_chain.accepts(rec):
                continue

            # Unpack packed codes and reverse-remap to cyvcf2 codes.
            # packed: 0=HomRef, 1=Het, 2=HomAlt, 3=Missing
            # cyvcf2: 0=HomRef, 1=Het, 3=HomAlt, 2=Missing
            from graphmana.ingest.genotype_packer import unpack_genotypes

            packed_codes = unpack_genotypes(rec.gt_packed, n_new_samples)
            # Reverse remap: packed → cyvcf2
            reverse_remap = np.array([0, 1, 3, 2], dtype=np.int8)
            cyvcf2_codes = reverse_remap[packed_codes]

            phase_bits = unpack_phase(rec.phase_packed, n_new_samples)

            # Ploidy bits
            if rec.ploidy_packed:
                from graphmana.ingest.genotype_packer import unpack_ploidy

                ploidy_bits = unpack_ploidy(rec.ploidy_packed, n_new_samples)
            else:
                ploidy_bits = np.zeros(n_new_samples, dtype=np.uint8)

            vdata = _VariantData(
                variant_id=rec.id,
                chr=rec.chr,
                pos=rec.pos,
                ref=rec.ref,
                alt=rec.alt,
                variant_type=rec.variant_type,
                gt_types=cyvcf2_codes,
                phase_bits=phase_bits,
                ploidy_bits=ploidy_bits,
                pop_ids=self.pop_map_new.pop_ids,
                ac=rec.ac,
                an=rec.an,
                af=rec.af,
                het_count=rec.het_count,
                hom_alt_count=rec.hom_alt_count,
                het_exp=rec.het_exp,
                ac_total=rec.ac_total,
                an_total=rec.an_total,
                af_total=rec.af_total,
                call_rate=rec.call_rate,
                multiallelic_site=rec.multiallelic_site,
                allele_index=rec.allele_index,
            )

            chr_variants.setdefault(rec.chr, {})[rec.id] = vdata

        return chr_variants

    def _fetch_existing_chromosomes(self) -> list[str]:
        """Get list of chromosome IDs from the database."""
        result = self.conn.execute_read(queries.FETCH_CHROMOSOMES)
        return [r["chr"] for r in result]

    def _process_chromosome(
        self,
        chrom: str,
        new_variants: dict[str, _VariantData],
        chunk_size: int,
    ) -> None:
        """Process one chromosome: extend existing variants, create new ones."""
        # Fetch all existing variant IDs on this chromosome
        result = self.conn.execute_read(queries.FETCH_VARIANT_IDS_BY_CHR, {"chr": chrom})
        existing_ids = [r["variantId"] for r in result]
        existing_id_set = set(existing_ids)

        # Partition new variants into extend vs create
        to_extend_ids = [vid for vid in existing_ids if vid in new_variants]
        to_extend_homref_ids = [vid for vid in existing_ids if vid not in new_variants]
        to_create = {vid: vd for vid, vd in new_variants.items() if vid not in existing_id_set}

        n_new_samples = len(self.pop_map_new.sample_ids)

        # Extend variants that have actual genotypes in new VCF
        for i in range(0, len(to_extend_ids), chunk_size):
            batch_ids = to_extend_ids[i : i + chunk_size]
            self._extend_variant_batch(batch_ids, new_variants, n_new_samples)
            self.n_variants_extended += len(batch_ids)

        # Extend variants that are HomRef-only for new samples
        for i in range(0, len(to_extend_homref_ids), chunk_size):
            batch_ids = to_extend_homref_ids[i : i + chunk_size]
            self._extend_homref_batch(batch_ids, n_new_samples)
            self.n_variants_homref_extended += len(batch_ids)

        # Create brand-new variants
        if to_create:
            self._create_new_variant_batch(to_create, chrom)
            self.n_variants_created += len(to_create)
            # Rebuild NEXT chain since new variants were inserted
            self._rebuild_next_chain(chrom)

        if existing_ids or to_create:
            logger.info(
                "Chromosome %s: %d extended, %d homref-extended, %d new",
                chrom,
                len(to_extend_ids),
                len(to_extend_homref_ids),
                len(to_create),
            )

    def _extend_variant_batch(
        self,
        variant_ids: list[str],
        new_variants: dict[str, _VariantData],
        n_new_samples: int,
    ) -> None:
        """Read a batch of existing variants, extend arrays, write back."""

        def _tx(tx, variant_ids, new_variants, n_new_samples, n_existing, n_total):
            # Read existing variant data
            result = tx.run(queries.FETCH_VARIANT_BATCH, {"variant_ids": variant_ids})
            records = list(result)

            updates = []
            for rec in records:
                vid = rec["variantId"]
                vdata = new_variants[vid]

                # Convert Neo4j byte arrays to Python bytes
                existing_gt = bytes(rec["gt_packed"])
                existing_phase = bytes(rec["phase_packed"])
                existing_ploidy = rec["ploidy_packed"]
                if existing_ploidy is not None:
                    existing_ploidy = bytes(existing_ploidy)

                # Extend packed arrays
                new_gt = extend_gt_packed(existing_gt, n_existing, vdata.gt_types)
                new_phase = extend_phase_packed(existing_phase, n_existing, vdata.phase_bits)
                new_ploidy = extend_ploidy_packed(existing_ploidy, n_existing, vdata.ploidy_bits)

                # Merge population stats
                existing_pop_ids = list(rec["pop_ids"]) if rec["pop_ids"] else []
                existing_ac = list(rec["ac"]) if rec["ac"] else []
                existing_an = list(rec["an"]) if rec["an"] else []
                existing_het = list(rec["het_count"]) if rec["het_count"] else []
                existing_hom = list(rec["hom_alt_count"]) if rec["hom_alt_count"] else []

                merged = merge_pop_stats(
                    existing_pop_ids,
                    existing_ac,
                    existing_an,
                    existing_het,
                    existing_hom,
                    vdata.pop_ids,
                    vdata.ac,
                    vdata.an,
                    vdata.het_count,
                    vdata.hom_alt_count,
                )

                # Compute call_rate using total samples
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

        self.conn.execute_write_tx(
            _tx,
            variant_ids=variant_ids,
            new_variants=new_variants,
            n_new_samples=n_new_samples,
            n_existing=self.n_existing,
            n_total=self.n_total_samples,
        )

    def _extend_homref_batch(
        self,
        variant_ids: list[str],
        n_new_samples: int,
    ) -> None:
        """Extend variants not in new VCF with HomRef for all new samples."""
        homref_gt = np.zeros(n_new_samples, dtype=np.int8)  # cyvcf2 code 0 = HomRef
        zero_phase = np.zeros(n_new_samples, dtype=np.uint8)
        zero_ploidy = np.zeros(n_new_samples, dtype=np.uint8)

        # HomRef pop stats for new samples: ac=0, an=2*n per pop, etc.
        new_pop_ids = self.pop_map_new.pop_ids
        new_ac = [0] * len(new_pop_ids)
        new_an = [2 * self.pop_map_new.n_samples_per_pop[pid] for pid in new_pop_ids]
        new_het = [0] * len(new_pop_ids)
        new_hom = [0] * len(new_pop_ids)

        def _tx(
            tx,
            variant_ids,
            homref_gt,
            zero_phase,
            zero_ploidy,
            new_pop_ids,
            new_ac,
            new_an,
            new_het,
            new_hom,
            n_existing,
            n_total,
        ):
            result = tx.run(queries.FETCH_VARIANT_BATCH, {"variant_ids": variant_ids})
            records = list(result)

            updates = []
            for rec in records:
                vid = rec["variantId"]

                existing_gt = bytes(rec["gt_packed"])
                existing_phase = bytes(rec["phase_packed"])
                existing_ploidy = rec["ploidy_packed"]
                if existing_ploidy is not None:
                    existing_ploidy = bytes(existing_ploidy)

                new_gt_packed = extend_gt_packed(existing_gt, n_existing, homref_gt)
                new_phase_packed = extend_phase_packed(existing_phase, n_existing, zero_phase)
                new_ploidy_packed = extend_ploidy_packed(existing_ploidy, n_existing, zero_ploidy)

                existing_pop_ids = list(rec["pop_ids"]) if rec["pop_ids"] else []
                existing_ac = list(rec["ac"]) if rec["ac"] else []
                existing_an = list(rec["an"]) if rec["an"] else []
                existing_het = list(rec["het_count"]) if rec["het_count"] else []
                existing_hom = list(rec["hom_alt_count"]) if rec["hom_alt_count"] else []

                merged = merge_pop_stats(
                    existing_pop_ids,
                    existing_ac,
                    existing_an,
                    existing_het,
                    existing_hom,
                    new_pop_ids,
                    new_ac,
                    new_an,
                    new_het,
                    new_hom,
                )

                total_an_diploid = 2 * n_total
                call_rate = merged["an_total"] / total_an_diploid if total_an_diploid > 0 else 0.0

                updates.append(
                    {
                        "variantId": vid,
                        "gt_packed": bytearray(new_gt_packed),
                        "phase_packed": bytearray(new_phase_packed),
                        "ploidy_packed": (
                            bytearray(new_ploidy_packed) if new_ploidy_packed else None
                        ),
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

        self.conn.execute_write_tx(
            _tx,
            variant_ids=variant_ids,
            homref_gt=homref_gt,
            zero_phase=zero_phase,
            zero_ploidy=zero_ploidy,
            new_pop_ids=new_pop_ids,
            new_ac=new_ac,
            new_an=new_an,
            new_het=new_het,
            new_hom=new_hom,
            n_existing=self.n_existing,
            n_total=self.n_total_samples,
        )

    def _create_new_variant_batch(
        self,
        to_create: dict[str, _VariantData],
        chrom: str,
    ) -> None:
        """Create brand-new Variant nodes with HomRef padding for existing samples."""
        variants = []
        edges = []

        for vid, vdata in to_create.items():
            gt_packed = pad_gt_for_new_variant(self.n_existing, vdata.gt_types)
            phase_packed = pad_phase_for_new_variant(self.n_existing, vdata.phase_bits)
            ploidy_packed = extend_ploidy_packed(None, self.n_existing, vdata.ploidy_bits)

            # For new variants, existing samples contribute HomRef to pop stats
            # Build homref stats for existing populations
            existing_homref_ac = [0] * len(self.existing_pop_ids)
            existing_homref_an = self._get_existing_pop_an()
            existing_homref_het = [0] * len(self.existing_pop_ids)
            existing_homref_hom = [0] * len(self.existing_pop_ids)

            merged = merge_pop_stats(
                self.existing_pop_ids,
                existing_homref_ac,
                existing_homref_an,
                existing_homref_het,
                existing_homref_hom,
                vdata.pop_ids,
                vdata.ac,
                vdata.an,
                vdata.het_count,
                vdata.hom_alt_count,
            )

            total_an_diploid = 2 * self.n_total_samples
            call_rate = merged["an_total"] / total_an_diploid if total_an_diploid > 0 else 0.0

            variants.append(
                {
                    "variantId": vid,
                    "chr": vdata.chr,
                    "pos": vdata.pos,
                    "ref": vdata.ref,
                    "alt": vdata.alt,
                    "variant_type": vdata.variant_type,
                    "gt_packed": bytearray(gt_packed),
                    "phase_packed": bytearray(phase_packed),
                    "ploidy_packed": (bytearray(ploidy_packed) if ploidy_packed else None),
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
                    "multiallelic_site": vdata.multiallelic_site,
                    "allele_index": vdata.allele_index,
                }
            )
            edges.append({"variantId": vid, "chr": chrom})

        # Batch create in chunks
        batch = 500
        for i in range(0, len(variants), batch):
            chunk_v = variants[i : i + batch]
            chunk_e = edges[i : i + batch]
            self.conn.execute_write(queries.CREATE_VARIANT_BATCH, {"variants": chunk_v})
            self.conn.execute_write(queries.CREATE_ON_CHROMOSOME_BATCH, {"edges": chunk_e})

    def _get_existing_pop_an(self) -> list[int]:
        """Get allele numbers for existing populations (assuming all diploid HomRef).

        For a new variant, existing samples are all HomRef = called.
        Each existing population contributes an = 2 * n_samples.
        """
        result = self.conn.execute_read(
            "MATCH (p:Population) RETURN p.populationId AS pid, p.n_samples AS n "
            "ORDER BY p.populationId"
        )
        pop_an = {}
        for r in result:
            pop_an[r["pid"]] = 2 * r["n"]

        return [pop_an.get(pid, 0) for pid in self.existing_pop_ids]

    def _rebuild_next_chain(self, chrom: str) -> None:
        """Delete and rebuild the NEXT chain for a chromosome."""
        self.conn.execute_write(queries.DELETE_NEXT_CHAIN_FOR_CHR, {"chr": chrom})
        self.conn.execute_write(queries.REBUILD_NEXT_CHAIN_FOR_CHR, {"chr": chrom})

    def _create_sample_nodes(self) -> None:
        """Create Sample nodes and IN_POPULATION edges for new samples."""
        from datetime import datetime, timezone

        pop_map = self.pop_map_new
        now = datetime.now(timezone.utc).isoformat()

        samples = []
        edges = []
        for sample_id in pop_map.sample_ids:
            pop_id = pop_map.sample_to_pop[sample_id]
            packed_idx = self.packed_index_offset + pop_map.sample_packed_index[sample_id]
            sex = pop_map.sample_to_sex.get(sample_id, 0)

            samples.append(
                {
                    "sampleId": sample_id,
                    "population": pop_id,
                    "packed_index": packed_idx,
                    "sex": sex,
                    "source_dataset": self.dataset_id,
                    "source_file": self.source_file,
                    "ingestion_date": now,
                }
            )
            edges.append({"sampleId": sample_id, "populationId": pop_id})

        # Batch create
        batch = 500
        for i in range(0, len(samples), batch):
            self.conn.execute_write(
                queries.CREATE_SAMPLE_BATCH, {"samples": samples[i : i + batch]}
            )

        for i in range(0, len(edges), batch):
            self.conn.execute_write(
                queries.CREATE_IN_POPULATION_BATCH, {"edges": edges[i : i + batch]}
            )

        self.n_samples_created = len(samples)

    def _update_populations(self) -> None:
        """Create new Population nodes and update counts for all populations."""
        pop_map = self.pop_map_new
        new_pop_set = set(pop_map.pop_ids)
        existing_pop_set = set(self.existing_pop_ids)

        # Create brand-new populations
        new_only = new_pop_set - existing_pop_set
        for pid in sorted(new_only):
            n = pop_map.n_samples_per_pop[pid]
            a_n = sum(1.0 / i for i in range(1, 2 * n))  # harmonic number
            a_n2 = sum(1.0 / (i * i) for i in range(1, 2 * n))
            self.conn.execute_write(
                queries.MERGE_POPULATION,
                {
                    "populationId": pid,
                    "name": pid,
                    "n_samples": n,
                    "a_n": a_n,
                    "a_n2": a_n2,
                },
            )
            self.n_populations_created += 1

        # Update existing populations with new sample counts
        overlap = new_pop_set & existing_pop_set
        if overlap:
            # Read current counts
            result = self.conn.execute_read(
                "MATCH (p:Population) WHERE p.populationId IN $pids "
                "RETURN p.populationId AS pid, p.n_samples AS n",
                {"pids": sorted(overlap)},
            )
            pops_update = []
            for r in result:
                pid = r["pid"]
                old_n = r["n"]
                added_n = pop_map.n_samples_per_pop.get(pid, 0)
                new_n = old_n + added_n
                a_n = sum(1.0 / i for i in range(1, 2 * new_n)) if new_n > 0 else 0.0
                a_n2 = sum(1.0 / (i * i) for i in range(1, 2 * new_n)) if new_n > 0 else 0.0
                pops_update.append(
                    {
                        "populationId": pid,
                        "n_samples": new_n,
                        "a_n": a_n,
                        "a_n2": a_n2,
                    }
                )
            if pops_update:
                self.conn.execute_write(queries.UPDATE_POPULATION_COUNTS, {"pops": pops_update})

    def _update_schema_metadata(self) -> None:
        """Update SchemaMetadata node with new counts."""
        from graphmana.db.schema import ensure_schema

        ensure_schema(self.conn)
