"""JSON variant exporter (FAST PATH or FULL PATH).

Exports variant data as JSON Lines (one JSON object per line) or
optionally as a single JSON array. Supports both population-level
stats (FAST PATH) and per-sample genotypes (FULL PATH).
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import numpy as np

from graphmana.export.base import BaseExporter

logger = logging.getLogger(__name__)

DEFAULT_FIELDS = [
    "variantId",
    "chr",
    "pos",
    "ref",
    "alt",
    "variant_type",
    "af_total",
    "ac_total",
    "an_total",
]


class JSONExporter(BaseExporter):
    """Export variant data as JSON Lines (FAST or FULL PATH)."""

    def export(
        self,
        output: Path,
        *,
        fields: list[str] | None = None,
        pretty: bool = False,
        include_genotypes: bool = False,
    ) -> dict:
        """Export JSON format.

        Args:
            output: Output file path.
            fields: List of variant property fields to include.
                Defaults to DEFAULT_FIELDS.
            pretty: If True, use indented JSON formatting.
            include_genotypes: If True, include per-sample genotype
                arrays (FULL PATH). If False, only variant properties
                (FAST PATH).

        Returns:
            Summary dict with n_variants, fields, format.
        """
        cols = fields if fields else list(DEFAULT_FIELDS)
        target_chroms = self._get_target_chromosomes()

        samples = None
        packed_indices = None
        if include_genotypes:
            samples = self._load_samples()
            packed_indices = np.array(sorted(s["packed_index"] for s in samples), dtype=np.int64)
            sample_ids = []
            idx_to_sample = {s["packed_index"]: s["sampleId"] for s in samples}
            for idx in packed_indices:
                sample_ids.append(idx_to_sample[idx])

        n_variants = 0
        output = Path(output)
        output.parent.mkdir(parents=True, exist_ok=True)

        indent = 2 if pretty else None

        with open(output, "w") as f:
            for chrom in target_chroms:
                for props in self._iter_variants(chrom):
                    obj = {}
                    for col in cols:
                        val = props.get(col)
                        # Convert numpy/Neo4j types to JSON-serializable
                        if isinstance(val, (np.integer,)):
                            val = int(val)
                        elif isinstance(val, (np.floating,)):
                            val = float(val)
                        elif isinstance(val, (bytes, bytearray)):
                            val = None  # Skip raw packed arrays
                        elif isinstance(val, list):
                            val = [
                                (
                                    int(x)
                                    if isinstance(x, np.integer)
                                    else float(x) if isinstance(x, np.floating) else x
                                )
                                for x in val
                            ]
                        obj[col] = val

                    if include_genotypes and packed_indices is not None:
                        gt_codes, phase_bits, ploidy_flags = self._unpack_variant_genotypes(
                            props, packed_indices
                        )
                        props = self._maybe_recalculate_af(props, gt_codes, ploidy_flags)
                        genotypes = {}
                        for i, sid in enumerate(sample_ids):
                            gt = int(gt_codes[i])
                            ph = int(phase_bits[i])
                            genotypes[sid] = {"gt": gt, "phase": ph}
                        obj["genotypes"] = genotypes

                    f.write(json.dumps(obj, indent=indent))
                    f.write("\n")
                    n_variants += 1

        n_samples = self._get_sample_count()
        logger.info("JSON export: %d variants", n_variants)
        return {
            "n_variants": n_variants,
            "n_samples": n_samples,
            "fields": cols,
            "include_genotypes": include_genotypes,
            "chromosomes": target_chroms,
            "format": "json",
        }
