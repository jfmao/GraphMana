"""Import-time variant filters applied after VCF parsing.

Filters operate on VariantRecord objects yielded by VCFParser. Each filter
is a simple predicate; the chain short-circuits on the first rejection.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Iterator

from graphmana.ingest.vcf_parser import VariantRecord

logger = logging.getLogger(__name__)

_REGION_RE = re.compile(r"^([^:]+):(\d+)-(\d+)$")


@dataclass(slots=True)
class ImportFilterConfig:
    """Configuration for import-time variant filters.

    All fields default to ``None`` (disabled).  Only non-None filters
    are applied.
    """

    min_qual: float | None = None
    min_call_rate: float | None = None
    maf_min: float | None = None
    maf_max: float | None = None
    variant_types: set[str] | None = None  # e.g. {"SNP", "INDEL"}
    region: str | None = None  # e.g. "chr1:1000-2000"
    contigs: list[str] | None = None  # e.g. ["chr1", "chr22"]


class ImportFilterChain:
    """Apply a sequence of filters to a VariantRecord stream.

    Usage::

        cfg = ImportFilterConfig(min_qual=30, maf_min=0.01)
        chain = ImportFilterChain(cfg)
        for rec in chain.filter(parser):
            ...
        print(chain.summary())
    """

    def __init__(self, config: ImportFilterConfig) -> None:
        self._cfg = config
        self._counts: dict[str, int] = {
            "total": 0,
            "passed": 0,
            "rejected_qual": 0,
            "rejected_call_rate": 0,
            "rejected_maf": 0,
            "rejected_variant_type": 0,
            "rejected_region": 0,
            "rejected_contig": 0,
        }
        # Parse region once
        self._region_parsed: tuple[str, int, int] | None = None
        if config.region:
            m = _REGION_RE.match(config.region)
            if not m:
                raise ValueError(
                    f"Invalid region format: {config.region!r}. "
                    "Expected 'chr:start-end' (e.g. 'chr1:1000-2000')."
                )
            self._region_parsed = (m.group(1), int(m.group(2)), int(m.group(3)))
        self._contigs_set: set[str] | None = set(config.contigs) if config.contigs else None

    def accepts(self, record: VariantRecord) -> bool:
        """Return True if the record passes all active filters."""
        self._counts["total"] += 1

        if not self._check_contig(record):
            self._counts["rejected_contig"] += 1
            return False

        if not self._check_region(record):
            self._counts["rejected_region"] += 1
            return False

        reason = self._check_qual(record)
        if reason:
            self._counts["rejected_qual"] += 1
            return False

        reason = self._check_call_rate(record)
        if reason:
            self._counts["rejected_call_rate"] += 1
            return False

        reason = self._check_maf(record)
        if reason:
            self._counts["rejected_maf"] += 1
            return False

        reason = self._check_variant_type(record)
        if reason:
            self._counts["rejected_variant_type"] += 1
            return False

        self._counts["passed"] += 1
        return True

    def filter(self, records: Iterator[VariantRecord]) -> Iterator[VariantRecord]:
        """Yield only records that pass all active filters."""
        for rec in records:
            if self.accepts(rec):
                yield rec

    def summary(self) -> dict[str, int]:
        """Return a copy of filter rejection counts."""
        return dict(self._counts)

    # -- Individual checks --------------------------------------------------

    def _check_qual(self, rec: VariantRecord) -> str | None:
        if self._cfg.min_qual is None:
            return None
        # Missing QUAL passes (we don't reject unknowns)
        if rec.qual is None:
            return None
        if rec.qual < self._cfg.min_qual:
            return "qual"
        return None

    def _check_call_rate(self, rec: VariantRecord) -> str | None:
        if self._cfg.min_call_rate is None:
            return None
        if rec.call_rate < self._cfg.min_call_rate:
            return "call_rate"
        return None

    def _check_maf(self, rec: VariantRecord) -> str | None:
        cfg = self._cfg
        if cfg.maf_min is None and cfg.maf_max is None:
            return None
        maf = min(rec.af_total, 1.0 - rec.af_total) if rec.af_total > 0 else 0.0
        if cfg.maf_min is not None and maf < cfg.maf_min:
            return "maf"
        if cfg.maf_max is not None and maf > cfg.maf_max:
            return "maf"
        return None

    def _check_variant_type(self, rec: VariantRecord) -> str | None:
        if self._cfg.variant_types is None:
            return None
        if rec.variant_type not in self._cfg.variant_types:
            return "variant_type"
        return None

    def _check_contig(self, rec: VariantRecord) -> bool:
        if self._contigs_set is None:
            return True
        return rec.chr in self._contigs_set

    def _check_region(self, rec: VariantRecord) -> bool:
        if self._region_parsed is None:
            return True
        r_chr, r_start, r_end = self._region_parsed
        if rec.chr != r_chr:
            return False
        return r_start <= rec.pos <= r_end
