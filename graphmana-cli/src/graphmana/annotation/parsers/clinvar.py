"""ClinVar VCF importer — loads clinical significance onto Variant nodes."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterator

from graphmana.annotation.parsers.base import BaseAnnotationParser
from graphmana.db.queries import UPDATE_VARIANT_CLINVAR_BATCH

logger = logging.getLogger(__name__)


class ClinVarParser(BaseAnnotationParser):
    """Parse ClinVar VCF and set clinvar properties on Variant nodes.

    Uses cyvcf2 to parse the VCF. Extracts from INFO fields:
        - CLNSIG (clinical significance)
        - CLNDN (disease name)
        - CLNREVSTAT (review status)
        - ID field as clinvar_id
    """

    @property
    def source_name(self) -> str:
        return "ClinVar"

    def _parse_file(self, input_path: Path, *, chr_prefix: str = "") -> Iterator[dict]:
        """Yield {variantId, clinvar_id, clinvar_sig, clinvar_review, clinvar_disease}.

        Args:
            input_path: Path to ClinVar VCF (plain or gzipped).
            chr_prefix: Prefix to prepend to chromosome (e.g. 'chr').
        """
        from cyvcf2 import VCF

        vcf = VCF(str(input_path))
        for variant in vcf:
            chrom = variant.CHROM
            if chr_prefix and not chrom.startswith(chr_prefix):
                chrom = chr_prefix + chrom
            pos = variant.POS
            ref = variant.REF
            # ClinVar VCFs can have multiple alts; take first
            alt = variant.ALT[0] if variant.ALT else ""
            if not alt:
                continue

            variant_id = f"{chrom}:{pos}:{ref}:{alt}"
            clinvar_id = variant.ID if variant.ID and variant.ID != "." else None

            # Extract INFO fields
            clinvar_sig = _get_info_str(variant, "CLNSIG")
            clinvar_review = _get_info_str(variant, "CLNREVSTAT")
            clinvar_disease = _get_info_str(variant, "CLNDN")

            yield {
                "variantId": variant_id,
                "clinvar_id": clinvar_id,
                "clinvar_sig": clinvar_sig,
                "clinvar_review": clinvar_review,
                "clinvar_disease": clinvar_disease,
            }

        vcf.close()

    def _load_batch(self, batch: list[dict]) -> int:
        with self._conn.driver.session() as session:
            result = session.run(UPDATE_VARIANT_CLINVAR_BATCH, {"updates": batch})
            record = result.single()
            return record["matched"] if record else 0


def _get_info_str(variant, field: str) -> str | None:
    """Safely extract a string INFO field from a cyvcf2 variant."""
    try:
        val = variant.INFO.get(field)
        if val is None:
            return None
        return str(val)
    except (KeyError, AttributeError):
        return None
