"""Base exporter with shared sample/variant loading and filtering."""

from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, TypedDict

import numpy as np

from graphmana.db.connection import GraphManaConnection
from graphmana.db import queries as Q
from graphmana.filtering.export_filters import ExportFilter, ExportFilterConfig
from graphmana.ingest.genotype_packer import unpack_genotypes, unpack_phase, unpack_ploidy

logger = logging.getLogger(__name__)


class ExportSummary(TypedDict, total=False):
    """Return type contract for all exporters."""

    n_variants: int
    n_samples: int
    chromosomes: list[str]
    format: str
    n_skipped: int
    files: list[str]
    populations: list[str]


class BaseExporter(ABC):
    """Abstract base for all export format implementations.

    Provides shared methods for loading samples, chromosomes, streaming
    variants from Neo4j, and unpacking genotype arrays.
    """

    def __init__(
        self,
        conn: GraphManaConnection,
        *,
        filter_config: ExportFilterConfig | None = None,
        threads: int = 1,
        recalculate_af: bool = False,
    ) -> None:
        self._conn = conn
        self._filter = ExportFilter(filter_config or ExportFilterConfig())
        self._filter_config = filter_config or ExportFilterConfig()
        self._threads = threads
        self._recalculate_af = recalculate_af
        self._samples: list[dict] | None = None
        self._chromosomes: list[dict] | None = None

    def _get_sample_count(self) -> int:
        """Get sample count without loading full sample metadata.

        Returns cached count if samples already loaded, otherwise runs a
        lightweight COUNT query. For FAST PATH exporters that only need the
        count in their return dict.
        """
        if self._samples is not None:
            return len(self._samples)

        if self._filter.has_population_filter():
            query = (
                "MATCH (s:Sample)-[:IN_POPULATION]->(p:Population) "
                f"WHERE ({Q.ACTIVE_SAMPLE_FILTER}) "
                "AND p.populationId IN $populations "
                "RETURN count(s) AS n"
            )
            params = {"populations": self._filter.populations}
        else:
            query = "MATCH (s:Sample) " f"WHERE {Q.ACTIVE_SAMPLE_FILTER} " "RETURN count(s) AS n"
            params = {}

        with self._conn.driver.session() as session:
            result = session.run(query, params)
            record = result.single()
            return record["n"] if record else 0

    def _load_samples(self) -> list[dict]:
        """Fetch sample metadata from Neo4j.

        Applies population filter if set. Results are cached.
        Returns list of {sampleId, population, packed_index, sex}.
        """
        if self._samples is not None:
            return self._samples

        if self._filter.has_population_filter():
            query = Q.FETCH_SAMPLES_BY_POPULATION
            params = {"populations": self._filter.populations}
        else:
            query = Q.FETCH_SAMPLES
            params = {}

        with self._conn.driver.session() as session:
            result = session.run(query, params)
            self._samples = [dict(record) for record in result]

        if self._filter.has_cohort_filter():
            from graphmana.cohort.manager import CohortManager

            mgr = CohortManager(self._conn)
            cohort_ids = set(mgr.resolve_sample_ids(self._filter.cohort))
            self._samples = [s for s in self._samples if s["sampleId"] in cohort_ids]

        if self._filter.has_sample_id_filter():
            target_ids = set(self._filter.sample_ids)
            self._samples = [s for s in self._samples if s["sampleId"] in target_ids]

        logger.info("Loaded %d samples", len(self._samples))
        return self._samples

    def _load_chromosomes(self) -> list[dict]:
        """Fetch chromosome metadata from Neo4j. Results are cached."""
        if self._chromosomes is not None:
            return self._chromosomes

        with self._conn.driver.session() as session:
            result = session.run(Q.FETCH_CHROMOSOMES)
            self._chromosomes = [dict(record) for record in result]

        logger.info("Loaded %d chromosomes", len(self._chromosomes))
        return self._chromosomes

    def _get_target_chromosomes(self) -> list[str]:
        """Return ordered list of chromosomes to export after filtering."""
        chroms = self._load_chromosomes()
        available = [c["chr"] for c in chroms]
        return self._filter.get_target_chromosomes(available)

    # Export batch size for paginated queries (500K variants per batch).
    # Larger than DEFAULT_BATCH_SIZE (100K) because export reads are streaming
    # and sort-only, while import batches carry packed arrays and write locks.
    # Each batch sorts independently in Neo4j heap, avoiding GC pauses from
    # sorting millions of rows at once.
    _BATCH_SIZE = 500_000

    def _iter_variants(
        self,
        chr_name: str,
        *,
        need_genotypes: bool = True,
        need_order: bool = True,
        start: int | None = None,
        end: int | None = None,
    ) -> Iterator[dict]:
        """Smart variant iterator that selects the optimal query strategy.

        Automatically picks the right Cypher query based on what the caller
        needs, and uses batched pagination for ordered queries to avoid
        Neo4j GC pauses on large chromosomes.

        Args:
            chr_name: Chromosome name.
            need_genotypes: If True, include gt_packed/phase_packed/ploidy_packed
                (FULL PATH). If False, return only population arrays and metadata
                (FAST PATH — ~5x less data per variant).
            need_order: If True, results are ordered by position. If False,
                skip ORDER BY to avoid Neo4j sorting millions of rows in heap.
                Set False for aggregation-only exports (TreeMix, SFS).
            start: Optional start position (inclusive).
            end: Optional end position (inclusive).

        Yields:
            Variant property dicts.
        """
        region = self._filter.parse_region()
        use_region = region is not None or (start is not None and end is not None)
        if region is not None:
            region_start, region_end = region[1], region[2]
        else:
            region_start, region_end = start, end

        # --- Annotation/CADD filters use legacy RETURN v queries ---
        if self._filter.has_annotation_filter() or self._filter.has_cadd_filter():
            yield from self._iter_variants_legacy(chr_name, region_start, region_end, use_region)
            return

        # --- Select query based on need_genotypes × need_order × use_region ---
        if use_region:
            if need_genotypes:
                query = Q.FETCH_VARIANTS_REGION_GENOTYPES
            else:
                query = Q.FETCH_VARIANTS_REGION_FAST if need_order else Q.FETCH_VARIANTS_REGION_FAST_UNORDERED
            params = {"chr": chr_name, "start": region_start, "end": region_end}
            # Region queries are always small enough for a single session
            yield from self._run_streaming_query(query, params, need_genotypes)
            return

        # Full-chromosome queries: use batched pagination for ordered queries
        if need_order:
            yield from self._iter_batched(chr_name, need_genotypes)
        else:
            query = Q.FETCH_VARIANTS_BY_CHR_FAST_UNORDERED
            params = {"chr": chr_name}
            yield from self._run_streaming_query(query, params, need_genotypes=False)

    def _iter_batched(
        self, chr_name: str, need_genotypes: bool
    ) -> Iterator[dict]:
        """Paginate through variants in position-range batches.

        Each batch sorts only _BATCH_SIZE rows in Neo4j heap (instant),
        uses a fresh session per batch (no long-lived connections), and
        releases memory between batches.
        """
        query = (
            Q.FETCH_VARIANTS_BY_CHR_GENOTYPES_BATCHED
            if need_genotypes
            else Q.FETCH_VARIANTS_BY_CHR_FAST_BATCHED
        )
        last_pos = -1
        while True:
            params = {
                "chr": chr_name,
                "last_pos": last_pos,
                "batch_size": self._BATCH_SIZE,
            }
            batch = []
            with self._conn.driver.session(fetch_size=5000) as session:
                result = session.run(query, params)
                for record in result:
                    props = dict(record)
                    if self._filter.variant_passes(props):
                        batch.append(props)
                    last_pos = record["pos"]

            if not batch:
                break

            yield from batch

            # If we got fewer than batch_size, we've exhausted this chromosome
            if len(batch) < self._BATCH_SIZE:
                break

    def _run_streaming_query(
        self, query: str, params: dict, need_genotypes: bool
    ) -> Iterator[dict]:
        """Run a single streaming query and yield filtered results."""
        fetch_size = 500 if need_genotypes else 5000
        record_key = "v" if "RETURN v\n" in query or query.strip().endswith("RETURN v") else None

        with self._conn.driver.session(fetch_size=fetch_size) as session:
            result = session.run(query, params)
            for record in result:
                props = dict(record[record_key]) if record_key else dict(record)
                if self._filter.variant_passes(props):
                    yield props

    def _iter_variants_legacy(
        self,
        chr_name: str,
        region_start: int | None,
        region_end: int | None,
        use_region: bool,
    ) -> Iterator[dict]:
        """Legacy path for annotation/CADD-filtered queries (RETURN v)."""
        if self._filter.has_annotation_filter():
            if use_region:
                query = Q.FETCH_VARIANTS_REGION_ANNOTATED
                params = {"chr": chr_name, "start": region_start, "end": region_end}
            else:
                query = Q.FETCH_VARIANTS_BY_CHR_ANNOTATED
                params = {"chr": chr_name}
            params.update(self._filter.get_annotation_filter_params())
        else:  # CADD filter
            if use_region:
                query = Q.FETCH_VARIANTS_REGION_CADD
                params = {"chr": chr_name, "start": region_start, "end": region_end}
            else:
                query = Q.FETCH_VARIANTS_BY_CHR_CADD
                params = {"chr": chr_name}

        with self._conn.driver.session(fetch_size=500) as session:
            result = session.run(query, params)
            for record in result:
                props = dict(record["v"])
                if self._filter.variant_passes(props):
                    yield props

    # Keep backward-compatible alias for exporters that already use this name
    def _iter_variants_fast(
        self, chr_name: str, *, ordered: bool = True, **kwargs
    ) -> Iterator[dict]:
        """Backward-compatible alias — delegates to _iter_variants."""
        return self._iter_variants(
            chr_name, need_genotypes=False, need_order=ordered, **kwargs
        )

    def _unpack_variant_genotypes(
        self, variant_props: dict, packed_indices: np.ndarray
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """Unpack genotypes for a specific subset of samples.

        Args:
            variant_props: Variant node properties including gt_packed,
                phase_packed, ploidy_packed.
            packed_indices: Sorted array of packed_index values for the
                samples to extract.

        Returns:
            Tuple of (gt_codes, phase_bits, ploidy_flags) arrays,
            each of length len(packed_indices).
        """
        gt_packed = variant_props.get("gt_packed")
        phase_packed = variant_props.get("phase_packed")
        ploidy_packed = variant_props.get("ploidy_packed")

        if gt_packed is None:
            n = len(packed_indices)
            return (
                np.full(n, 3, dtype=np.int8),  # all missing
                np.zeros(n, dtype=np.uint8),
                np.zeros(n, dtype=np.uint8),
            )

        # Convert Neo4j byte list to Python bytes if needed
        if isinstance(gt_packed, (list, bytearray)):
            gt_packed = bytes(gt_packed)
        if isinstance(phase_packed, (list, bytearray)):
            phase_packed = bytes(phase_packed)
        if isinstance(ploidy_packed, (list, bytearray)):
            ploidy_packed = bytes(ploidy_packed)

        # Total samples is determined by packed array size
        n_total = len(gt_packed) * 4
        # But actual n_samples may be less; we unpack all then subset
        gt_all = unpack_genotypes(gt_packed, n_total)
        phase_all = (
            unpack_phase(phase_packed, n_total)
            if phase_packed
            else np.zeros(n_total, dtype=np.uint8)
        )
        ploidy_all = unpack_ploidy(ploidy_packed, n_total)

        # Subset to requested sample indices
        max_idx = len(gt_all)
        safe_indices = packed_indices[packed_indices < max_idx]
        return (
            gt_all[safe_indices],
            phase_all[safe_indices],
            ploidy_all[safe_indices],
        )

    def _maybe_recalculate_af(
        self,
        props: dict,
        gt_codes: np.ndarray,
        ploidy_flags: np.ndarray,
    ) -> dict:
        """If recalculate_af is enabled, recompute AF stats from genotypes.

        Returns props (possibly updated with recalculated values).
        """
        if not self._recalculate_af:
            return props
        from graphmana.export.vcf_export import recalculate_af_from_genotypes

        af_info = recalculate_af_from_genotypes(gt_codes, ploidy_flags)
        return {**props, **af_info}

    def write_manifest(self, output: Path, summary: ExportSummary) -> Path:
        """Write a JSON manifest sidecar file alongside the export output.

        The manifest documents what was exported, when, with which filters,
        enabling reproducibility without forensic reconstruction.

        Args:
            output: The export output file path.
            summary: The ExportSummary returned by export().

        Returns:
            Path to the written manifest file.
        """
        from graphmana.config import GRAPHMANA_VERSION

        manifest = {
            "graphmana_version": GRAPHMANA_VERSION,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "output_file": str(output),
            "format": summary.get("format", "unknown"),
            "n_variants": summary.get("n_variants", 0),
            "n_samples": summary.get("n_samples", 0),
            "chromosomes": summary.get("chromosomes", []),
            "filters": {},
        }

        # Record active filters
        cfg = self._filter_config
        if cfg.populations:
            manifest["filters"]["populations"] = cfg.populations
        if cfg.chromosomes:
            manifest["filters"]["chromosomes"] = cfg.chromosomes
        if cfg.region:
            manifest["filters"]["region"] = cfg.region
        if cfg.variant_types:
            manifest["filters"]["variant_types"] = sorted(cfg.variant_types)
        if cfg.maf_min is not None:
            manifest["filters"]["maf_min"] = cfg.maf_min
        if cfg.maf_max is not None:
            manifest["filters"]["maf_max"] = cfg.maf_max
        if cfg.min_call_rate is not None:
            manifest["filters"]["min_call_rate"] = cfg.min_call_rate
        if cfg.cohort:
            manifest["filters"]["cohort"] = cfg.cohort
        if cfg.sample_ids:
            manifest["filters"]["n_sample_ids"] = len(cfg.sample_ids)
        if cfg.consequences:
            manifest["filters"]["consequences"] = cfg.consequences
        if cfg.impacts:
            manifest["filters"]["impacts"] = cfg.impacts
        if cfg.genes:
            manifest["filters"]["genes"] = cfg.genes
        if cfg.cadd_min is not None:
            manifest["filters"]["cadd_min"] = cfg.cadd_min
        if cfg.annotation_version:
            manifest["filters"]["annotation_version"] = cfg.annotation_version
        if cfg.sv_types:
            manifest["filters"]["sv_types"] = sorted(cfg.sv_types)
        if cfg.liftover_status:
            manifest["filters"]["liftover_status"] = cfg.liftover_status
        if cfg.cadd_max is not None:
            manifest["filters"]["cadd_max"] = cfg.cadd_max

        manifest["recalculate_af"] = self._recalculate_af
        manifest["threads"] = self._threads

        manifest_path = Path(str(output) + ".manifest.json")
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2, default=str)

        logger.info("Manifest written: %s", manifest_path)
        return manifest_path

    @abstractmethod
    def export(self, output: Path, **kwargs) -> ExportSummary:
        """Export data to the given output path.

        Returns:
            Summary dict with export statistics.
        """
