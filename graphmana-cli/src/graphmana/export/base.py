"""Base exporter with shared sample/variant loading and filtering."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Iterator, TypedDict

import numpy as np

from graphmana.db.connection import GraphManaConnection
from graphmana.db.queries import (
    ACTIVE_SAMPLE_FILTER,
    FETCH_CHROMOSOMES,
    FETCH_SAMPLES,
    FETCH_SAMPLES_BY_POPULATION,
    FETCH_VARIANTS_BY_CHR,
    FETCH_VARIANTS_BY_CHR_ANNOTATED,
    FETCH_VARIANTS_BY_CHR_CADD,
    FETCH_VARIANTS_REGION,
    FETCH_VARIANTS_REGION_ANNOTATED,
    FETCH_VARIANTS_REGION_CADD,
)
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
                f"WHERE ({ACTIVE_SAMPLE_FILTER}) "
                "AND p.populationId IN $populations "
                "RETURN count(s) AS n"
            )
            params = {"populations": self._filter.populations}
        else:
            query = "MATCH (s:Sample) " f"WHERE {ACTIVE_SAMPLE_FILTER} " "RETURN count(s) AS n"
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
            query = FETCH_SAMPLES_BY_POPULATION
            params = {"populations": self._filter.populations}
        else:
            query = FETCH_SAMPLES
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
            result = session.run(FETCH_CHROMOSOMES)
            self._chromosomes = [dict(record) for record in result]

        logger.info("Loaded %d chromosomes", len(self._chromosomes))
        return self._chromosomes

    def _get_target_chromosomes(self) -> list[str]:
        """Return ordered list of chromosomes to export after filtering."""
        chroms = self._load_chromosomes()
        available = [c["chr"] for c in chroms]
        return self._filter.get_target_chromosomes(available)

    def _iter_variants(
        self, chr_name: str, *, start: int | None = None, end: int | None = None
    ) -> Iterator[dict]:
        """Stream variant nodes for a chromosome from Neo4j.

        Uses driver.session() directly to support lazy iteration over
        potentially millions of variants. Applies post-query filters.

        Args:
            chr_name: Chromosome name.
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

        # Three-way routing: annotation filter > CADD-only filter > plain
        if self._filter.has_annotation_filter():
            if use_region:
                query = FETCH_VARIANTS_REGION_ANNOTATED
                params = {
                    "chr": chr_name,
                    "start": region_start,
                    "end": region_end,
                }
            else:
                query = FETCH_VARIANTS_BY_CHR_ANNOTATED
                params = {"chr": chr_name}
            params.update(self._filter.get_annotation_filter_params())
        elif self._filter.has_cadd_filter():
            if use_region:
                query = FETCH_VARIANTS_REGION_CADD
                params = {
                    "chr": chr_name,
                    "start": region_start,
                    "end": region_end,
                }
            else:
                query = FETCH_VARIANTS_BY_CHR_CADD
                params = {"chr": chr_name}
        else:
            if use_region:
                query = FETCH_VARIANTS_REGION
                params = {
                    "chr": chr_name,
                    "start": region_start,
                    "end": region_end,
                }
            else:
                query = FETCH_VARIANTS_BY_CHR
                params = {"chr": chr_name}

        # fetch_size=500 ensures Neo4j pushes records in small batches, keeping
        # the Bolt TCP connection active during long per-chromosome scans.
        with self._conn.driver.session(fetch_size=500) as session:
            result = session.run(query, params)
            for record in result:
                props = dict(record["v"])
                if self._filter.variant_passes(props):
                    yield props

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

    @abstractmethod
    def export(self, output: Path, **kwargs) -> ExportSummary:
        """Export data to the given output path.

        Returns:
            Summary dict with export statistics.
        """
