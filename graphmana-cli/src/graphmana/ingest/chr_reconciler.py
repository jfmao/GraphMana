"""Chromosome naming reconciliation.

Handles conversion between UCSC (chr1, chrX, chrM), Ensembl (1, X, MT),
and custom naming styles. Supports auto-detection, explicit style selection,
and user-provided mapping files.
"""

from __future__ import annotations

import csv
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# Canonical mappings between UCSC and Ensembl chromosome names.
_UCSC_TO_ENSEMBL: dict[str, str] = {}
_ENSEMBL_TO_UCSC: dict[str, str] = {}

# Build standard autosome + sex + mito mappings
for _i in range(1, 100):
    _UCSC_TO_ENSEMBL[f"chr{_i}"] = str(_i)
    _ENSEMBL_TO_UCSC[str(_i)] = f"chr{_i}"
for _s in ("X", "Y"):
    _UCSC_TO_ENSEMBL[f"chr{_s}"] = _s
    _ENSEMBL_TO_UCSC[_s] = f"chr{_s}"

# Mitochondrial aliases
_MITO_ALIASES = {"chrM", "MT", "chrMT", "M", "mitochondrion"}
_UCSC_TO_ENSEMBL["chrM"] = "MT"
_UCSC_TO_ENSEMBL["chrMT"] = "MT"
_ENSEMBL_TO_UCSC["MT"] = "chrM"
_ENSEMBL_TO_UCSC["M"] = "chrM"


class ChrReconciler:
    """Reconcile chromosome names between different naming conventions.

    Args:
        chr_style: Target naming style. One of 'auto', 'ucsc', 'ensembl',
            or 'original'. Default 'auto' detects from input contig names.
        chr_map_path: Optional path to a two-column TSV file mapping
            source names to target names.
    """

    def __init__(
        self,
        chr_style: str = "auto",
        chr_map_path: str | Path | None = None,
    ) -> None:
        if chr_style not in ("auto", "ucsc", "ensembl", "original"):
            raise ValueError(
                f"Invalid chr_style '{chr_style}'. "
                f"Must be one of: auto, ucsc, ensembl, original"
            )
        self._chr_style = chr_style
        self._detected_style: str | None = None
        self._custom_map: dict[str, str] = {}
        self._aliases: dict[str, list[str]] = {}

        if chr_map_path is not None:
            self._load_chr_map(Path(chr_map_path))

    def _load_chr_map(self, path: Path) -> None:
        """Load a two-column TSV/CSV chr mapping file.

        Format: source_name<tab>target_name (one pair per line).
        Lines starting with '#' are comments. An optional header line
        is skipped if its first field contains 'source' (case-insensitive).
        """
        with open(path) as f:
            reader = csv.reader(f, delimiter="\t")
            for row in reader:
                if not row or row[0].startswith("#"):
                    continue
                if row[0].lower().startswith("source"):
                    continue  # skip header
                if len(row) >= 2:
                    self._custom_map[row[0].strip()] = row[1].strip()
        logger.info("Loaded %d chromosome mappings from %s", len(self._custom_map), path)

    def detect_style(self, contig_names: list[str]) -> str:
        """Detect the chromosome naming style from a list of contig names.

        Counts contigs matching 'chr' prefix (UCSC) vs bare numeric/letter (Ensembl).
        Stores the result for subsequent normalize() calls.

        Args:
            contig_names: list of contig/chromosome names from the VCF header.

        Returns:
            Detected style: 'ucsc', 'ensembl', or 'unknown'.
        """
        if not contig_names:
            self._detected_style = "unknown"
            return "unknown"

        n_ucsc = 0
        n_ensembl = 0
        for name in contig_names:
            name_stripped = name.strip()
            if name_stripped.startswith("chr") or name_stripped.startswith("Chr"):
                n_ucsc += 1
            elif name_stripped.isdigit() or name_stripped in ("X", "Y", "MT", "M", "W", "Z"):
                n_ensembl += 1

        if n_ucsc > n_ensembl:
            self._detected_style = "ucsc"
        elif n_ensembl > n_ucsc:
            self._detected_style = "ensembl"
        else:
            self._detected_style = "unknown"

        logger.info(
            "Detected chromosome style: %s (ucsc=%d, ensembl=%d contigs)",
            self._detected_style,
            n_ucsc,
            n_ensembl,
        )
        return self._detected_style

    def normalize(self, chrom: str) -> str:
        """Normalize a chromosome name to the target style.

        Args:
            chrom: raw chromosome name from VCF.

        Returns:
            Normalized chromosome name.
        """
        # Custom map takes priority
        if chrom in self._custom_map:
            target = self._custom_map[chrom]
            self._track_alias(target, chrom)
            return target

        # 'original' mode: pass through unchanged
        if self._chr_style == "original":
            return chrom

        target_style = self._chr_style
        if target_style == "auto":
            target_style = self._detected_style or "unknown"

        if target_style == "ucsc":
            target = self._to_ucsc(chrom)
        elif target_style == "ensembl":
            target = self._to_ensembl(chrom)
        else:
            # unknown or unresolved auto — pass through
            return chrom

        if target != chrom:
            self._track_alias(target, chrom)
        return target

    def _to_ucsc(self, chrom: str) -> str:
        """Convert to UCSC style (chr-prefixed)."""
        if chrom in _ENSEMBL_TO_UCSC:
            return _ENSEMBL_TO_UCSC[chrom]
        # Handle mito aliases
        if chrom in _MITO_ALIASES:
            return "chrM"
        # Already has chr prefix
        if chrom.startswith("chr"):
            return chrom
        # Bare number or letter — add prefix
        if chrom.isdigit() or chrom in ("X", "Y", "W", "Z"):
            return f"chr{chrom}"
        return chrom

    def _to_ensembl(self, chrom: str) -> str:
        """Convert to Ensembl style (bare)."""
        if chrom in _UCSC_TO_ENSEMBL:
            return _UCSC_TO_ENSEMBL[chrom]
        # Handle mito aliases
        if chrom in _MITO_ALIASES:
            return "MT"
        # Strip chr prefix
        if chrom.startswith("chr"):
            return chrom[3:]
        return chrom

    def _track_alias(self, canonical: str, alias: str) -> None:
        """Record that *alias* was mapped to *canonical*."""
        if canonical not in self._aliases:
            self._aliases[canonical] = []
        if alias not in self._aliases[canonical]:
            self._aliases[canonical].append(alias)

    @property
    def aliases(self) -> dict[str, list[str]]:
        """Return mapping of canonical names to their observed aliases."""
        return dict(self._aliases)

    @property
    def detected_style(self) -> str | None:
        """Return the auto-detected style, or None if not yet detected."""
        return self._detected_style
