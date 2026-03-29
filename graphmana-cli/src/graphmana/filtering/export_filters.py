"""Export-time variant filters applied after Neo4j query.

Filters split into two tiers:
- Cypher-level: chromosome, region (pushed into WHERE clauses via query selection)
- Python-level: MAF, call_rate, variant_type (post-query filtering)
"""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(slots=True)
class ExportFilterConfig:
    """Configuration for export-time variant filters.

    All fields default to ``None`` (disabled).
    """

    populations: list[str] | None = None
    chromosomes: list[str] | None = None
    region: str | None = None  # "chr1:1000-2000"
    variant_types: set[str] | None = None  # {"SNP", "INDEL"}
    maf_min: float | None = None
    maf_max: float | None = None
    min_call_rate: float | None = None
    cohort: str | None = None
    sample_ids: list[str] | None = None
    # Annotation-based filters
    consequences: list[str] | None = None  # e.g. ["missense_variant", "stop_gained"]
    impacts: list[str] | None = None  # e.g. ["HIGH", "MODERATE"]
    genes: list[str] | None = None  # gene symbols or Ensembl IDs
    cadd_min: float | None = None
    cadd_max: float | None = None
    annotation_version: str | None = None
    sv_types: set[str] | None = None  # {"DEL", "DUP", "INV", "INS", "BND", "CNV"}
    liftover_status: str | None = None  # e.g. "mapped", "unmapped", "collision"


_REGION_RE = re.compile(r"^([^:]+):(\d+)-(\d+)$")


class ExportFilter:
    """Apply export filters to variant property dicts."""

    def __init__(self, config: ExportFilterConfig) -> None:
        self._cfg = config

    def variant_passes(self, variant_props: dict) -> bool:
        """Return True if variant passes all active post-query filters."""
        if not self._check_variant_type(variant_props):
            return False
        if not self._check_maf(variant_props):
            return False
        if not self._check_call_rate(variant_props):
            return False
        if not self._check_cadd(variant_props):
            return False
        if not self._check_sv_type(variant_props):
            return False
        if not self._check_liftover_status(variant_props):
            return False
        return True

    def parse_region(self) -> tuple[str, int, int] | None:
        """Parse region string into (chr, start, end). Returns None if unset."""
        if self._cfg.region is None:
            return None
        m = _REGION_RE.match(self._cfg.region)
        if not m:
            raise ValueError(
                f"Invalid region format: {self._cfg.region!r}. "
                f"Expected 'chr:start-end' (e.g. 'chr1:1000-2000')."
            )
        return m.group(1), int(m.group(2)), int(m.group(3))

    def get_target_chromosomes(self, available: list[str]) -> list[str]:
        """Return chromosomes to export.

        If a region is set, returns only that region's chromosome.
        If chromosomes are set, returns the intersection with available.
        Otherwise returns all available chromosomes.
        """
        region = self.parse_region()
        if region is not None:
            chr_name = region[0]
            if chr_name in available:
                return [chr_name]
            return []

        if self._cfg.chromosomes is not None:
            return [c for c in available if c in self._cfg.chromosomes]

        return list(available)

    def has_population_filter(self) -> bool:
        """Return True if population filtering is active."""
        return self._cfg.populations is not None and len(self._cfg.populations) > 0

    @property
    def populations(self) -> list[str] | None:
        return self._cfg.populations

    def has_cohort_filter(self) -> bool:
        """Return True if cohort filtering is active."""
        return self._cfg.cohort is not None and len(self._cfg.cohort) > 0

    @property
    def cohort(self) -> str | None:
        return self._cfg.cohort

    def has_sample_id_filter(self) -> bool:
        """Return True if sample ID filtering is active."""
        return self._cfg.sample_ids is not None and len(self._cfg.sample_ids) > 0

    @property
    def sample_ids(self) -> list[str] | None:
        return self._cfg.sample_ids

    def has_annotation_filter(self) -> bool:
        """Return True if any annotation-based filter is active."""
        cfg = self._cfg
        return (
            (cfg.consequences is not None and len(cfg.consequences) > 0)
            or (cfg.impacts is not None and len(cfg.impacts) > 0)
            or (cfg.genes is not None and len(cfg.genes) > 0)
            or (cfg.annotation_version is not None and len(cfg.annotation_version) > 0)
        )

    def has_cadd_filter(self) -> bool:
        """Return True if CADD score filtering is active."""
        return self._cfg.cadd_min is not None or self._cfg.cadd_max is not None

    def get_annotation_filter_params(self) -> dict:
        """Build Cypher parameter dict for annotation-filtered queries.

        Uses None for inactive filters so Cypher can use the
        ``$param IS NULL OR ...`` pattern.
        """
        cfg = self._cfg
        return {
            "consequences": cfg.consequences,
            "impacts": cfg.impacts,
            "genes": cfg.genes,
            "annotation_version": cfg.annotation_version,
        }

    # -- Private checks -----------------------------------------------------

    def _check_variant_type(self, props: dict) -> bool:
        if self._cfg.variant_types is None:
            return True
        vtype = props.get("variant_type")
        return vtype in self._cfg.variant_types

    def _check_maf(self, props: dict) -> bool:
        cfg = self._cfg
        if cfg.maf_min is None and cfg.maf_max is None:
            return True
        af = props.get("af_total", 0.0)
        if af is None:
            af = 0.0
        maf = min(af, 1.0 - af)
        if cfg.maf_min is not None and maf < cfg.maf_min:
            return False
        if cfg.maf_max is not None and maf > cfg.maf_max:
            return False
        return True

    def _check_call_rate(self, props: dict) -> bool:
        if self._cfg.min_call_rate is None:
            return True
        cr = props.get("call_rate", 1.0)
        if cr is None:
            cr = 1.0
        return cr >= self._cfg.min_call_rate

    def _check_cadd(self, props: dict) -> bool:
        cfg = self._cfg
        if cfg.cadd_min is None and cfg.cadd_max is None:
            return True
        cadd = props.get("cadd_phred")
        if cadd is None:
            return False  # No CADD score → fail if filter is active
        if cfg.cadd_min is not None and cadd < cfg.cadd_min:
            return False
        if cfg.cadd_max is not None and cadd > cfg.cadd_max:
            return False
        return True

    def _check_sv_type(self, props: dict) -> bool:
        if self._cfg.sv_types is None:
            return True
        sv_type = props.get("sv_type")
        return sv_type in self._cfg.sv_types

    def _check_liftover_status(self, props: dict) -> bool:
        if self._cfg.liftover_status is None:
            return True
        return props.get("liftover_status") == self._cfg.liftover_status
