"""Parse VEP CSQ / SnpEff ANN annotations from a VCF and emit Gene/HAS_CONSEQUENCE CSVs.

Adapted from GraphPop vep_parser.py with GraphMana-specific enhancements:
- Enhanced HAS_CONSEQUENCE_HEADER with annotation_source and annotation_version
- Optional ChrReconciler integration (replaces GraphPop's simpler chrom_map dict)
"""

from __future__ import annotations

import csv
import logging
import re
from pathlib import Path
from typing import TYPE_CHECKING

from cyvcf2 import VCF

if TYPE_CHECKING:
    from graphmana.ingest.chr_reconciler import ChrReconciler

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CSV headers (neo4j-admin import format)
# ---------------------------------------------------------------------------

GENE_HEADER = [
    "geneId:ID(Gene)",
    ":LABEL",
    "symbol",
    "biotype",
]

HAS_CONSEQUENCE_HEADER = [
    ":START_ID(Variant)",
    ":END_ID(Gene)",
    ":TYPE",
    "consequence",
    "impact",
    "feature",
    "feature_type",
    "sift_score:float",
    "sift_pred",
    "polyphen_score:float",
    "polyphen_pred",
    "cadd_phred:float",
    "revel:float",
    "annotation_source",
    "annotation_version",
]

# Regex for SIFT/PolyPhen format: "prediction(score)"
_PRED_SCORE_RE = re.compile(r"^(.+?)\(([0-9.]+)\)$")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_pred_score(value: str) -> tuple[str, str]:
    """Parse 'deleterious(0.02)' -> ('deleterious', '0.02').

    Returns ('', '') if value is empty or unparseable.
    """
    if not value:
        return ("", "")
    m = _PRED_SCORE_RE.match(value)
    if m:
        return (m.group(1), m.group(2))
    return (value, "")


def _load_variant_id_set(variant_csv_path: str | Path) -> set[str]:
    """Read the first column of variant_nodes.csv to build a variant ID set."""
    ids: set[str] = set()
    path = Path(variant_csv_path)
    with open(path) as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        for row in reader:
            if row:
                ids.add(row[0])
    logger.info("Loaded %d variant IDs from %s", len(ids), path)
    return ids


# ---------------------------------------------------------------------------
# Annotation format descriptors
# ---------------------------------------------------------------------------

# Unified field names used internally (mapped from CSQ or ANN)
_FIELD_ALLELE = "allele"
_FIELD_CONSEQUENCE = "consequence"
_FIELD_IMPACT = "impact"
_FIELD_SYMBOL = "symbol"
_FIELD_GENE = "gene"
_FIELD_FEATURE_TYPE = "feature_type"
_FIELD_FEATURE = "feature"
_FIELD_BIOTYPE = "biotype"
_FIELD_SIFT = "sift"
_FIELD_POLYPHEN = "polyphen"
_FIELD_CADD = "cadd"
_FIELD_REVEL = "revel"

# CSQ field name -> internal name
_CSQ_FIELD_MAP = {
    "Allele": _FIELD_ALLELE,
    "Consequence": _FIELD_CONSEQUENCE,
    "IMPACT": _FIELD_IMPACT,
    "SYMBOL": _FIELD_SYMBOL,
    "Gene": _FIELD_GENE,
    "Feature_type": _FIELD_FEATURE_TYPE,
    "Feature": _FIELD_FEATURE,
    "BIOTYPE": _FIELD_BIOTYPE,
    "SIFT": _FIELD_SIFT,
    "PolyPhen": _FIELD_POLYPHEN,
    "CADD_PHRED": _FIELD_CADD,
    "REVEL": _FIELD_REVEL,
}

# ANN field index -> internal name (SnpEff standard 16-field layout)
_ANN_INDEX_MAP = {
    0: _FIELD_ALLELE,  # Allele
    1: _FIELD_CONSEQUENCE,  # Annotation
    2: _FIELD_IMPACT,  # Annotation_Impact
    3: _FIELD_SYMBOL,  # Gene_Name
    4: _FIELD_GENE,  # Gene_ID
    5: _FIELD_FEATURE_TYPE,  # Feature_Type
    6: _FIELD_FEATURE,  # Feature_ID
    7: _FIELD_BIOTYPE,  # Transcript_BioType
    # Fields 8-15: Rank, HGVS.c, HGVS.p, cDNA, CDS, AA, Distance, Errors
}


# ---------------------------------------------------------------------------
# VEPParser
# ---------------------------------------------------------------------------


class VEPParser:
    """Parse VEP CSQ or SnpEff ANN annotations from a VCF.

    Auto-detects CSQ (VEP/gnomAD) vs ANN (SnpEff) format from VCF header.

    Emits two CSV files:
    - gene_nodes.csv: unique Gene nodes (geneId, symbol, biotype)
    - has_consequence_edges.csv: HAS_CONSEQUENCE edges from Variant->Gene
    """

    def __init__(
        self,
        vep_vcf_path: str | Path,
        out_dir: str | Path,
        *,
        variant_id_set: set[str] | None = None,
        variant_csv_path: str | Path | None = None,
        chr_reconciler: ChrReconciler | None = None,
        chrom_map: dict[str, str] | None = None,
        annotation_source: str = "VEP",
        annotation_version: str = "unknown",
    ) -> None:
        self._vcf_path = str(vep_vcf_path)
        self._out_dir = Path(out_dir)
        self._out_dir.mkdir(parents=True, exist_ok=True)
        self._chr_reconciler = chr_reconciler
        self._chrom_map = chrom_map or {}
        self._annotation_source = annotation_source
        self._annotation_version = annotation_version

        if variant_id_set is not None:
            self._variant_ids = variant_id_set
        elif variant_csv_path is not None:
            self._variant_ids = _load_variant_id_set(variant_csv_path)
        else:
            self._variant_ids = None

        # Stats
        self._n_variants_seen = 0
        self._n_variants_matched = 0
        self._n_variants_skipped = 0
        self._n_edges = 0
        self._n_genes = 0
        self._ann_format: str = ""

    # -- Public API ---------------------------------------------------------

    @property
    def n_variants_seen(self) -> int:
        return self._n_variants_seen

    @property
    def n_variants_matched(self) -> int:
        return self._n_variants_matched

    @property
    def n_edges(self) -> int:
        return self._n_edges

    @property
    def n_genes(self) -> int:
        return self._n_genes

    @property
    def ann_format(self) -> str:
        return self._ann_format

    def run(self) -> None:
        """Parse the VCF and write gene_nodes.csv + has_consequence_edges.csv."""
        vcf = VCF(self._vcf_path, lazy=True, threads=2)

        info_key, field_indices = self._detect_format(vcf)
        self._ann_format = info_key
        logger.info("Detected annotation format: %s", info_key)

        i_allele = field_indices[_FIELD_ALLELE]
        i_consequence = field_indices[_FIELD_CONSEQUENCE]
        i_impact = field_indices[_FIELD_IMPACT]
        i_symbol = field_indices[_FIELD_SYMBOL]
        i_gene = field_indices[_FIELD_GENE]
        i_feature_type = field_indices[_FIELD_FEATURE_TYPE]
        i_feature = field_indices[_FIELD_FEATURE]
        i_biotype = field_indices[_FIELD_BIOTYPE]
        i_sift = field_indices.get(_FIELD_SIFT)
        i_polyphen = field_indices.get(_FIELD_POLYPHEN)
        i_cadd = field_indices.get(_FIELD_CADD)
        i_revel = field_indices.get(_FIELD_REVEL)

        is_ann = info_key == "ANN"

        genes: dict[str, tuple[str, str]] = {}

        edge_path = self._out_dir / "has_consequence_edges.csv"
        edge_fh = open(edge_path, "w", newline="")
        edge_writer = csv.writer(edge_fh)
        edge_writer.writerow(HAS_CONSEQUENCE_HEADER)

        try:
            vcf_iter = iter(vcf)
            while True:
                try:
                    v = next(vcf_iter)
                except StopIteration:
                    break
                except Exception as exc:
                    if "bcf_read" in str(exc) or "htslib" in str(exc):
                        logger.warning(
                            "VCF read error after %d records (truncated file?): %s",
                            self._n_variants_seen,
                            exc,
                        )
                        break
                    raise

                self._n_variants_seen += 1

                chrom = v.CHROM
                pos = v.POS
                ref = v.REF
                alts = v.ALT

                if not alts:
                    continue

                ann_raw = v.INFO.get(info_key)
                if ann_raw is None:
                    continue

                # Map chrom for variant ID construction
                if self._chr_reconciler is not None:
                    vid_chrom = self._chr_reconciler.normalize(chrom)
                else:
                    vid_chrom = self._chrom_map.get(chrom, chrom)

                alt_to_vid: dict[str, str] = {}
                for alt in alts:
                    vid = f"{vid_chrom}:{pos}:{ref}:{alt}"
                    alt_to_vid[alt] = vid

                if self._variant_ids is not None:
                    matched_alts = {
                        alt for alt, vid in alt_to_vid.items() if vid in self._variant_ids
                    }
                    if not matched_alts:
                        self._n_variants_skipped += 1
                        continue
                else:
                    matched_alts = set(alts)

                self._n_variants_matched += 1

                for entry in ann_raw.split(","):
                    fields = entry.split("|")

                    allele = fields[i_allele] if i_allele < len(fields) else ""
                    gene_id = fields[i_gene] if i_gene < len(fields) else ""

                    if not gene_id:
                        continue

                    if is_ann:
                        variant_id = self._resolve_ann_variant_id(
                            allele,
                            alt_to_vid,
                            matched_alts,
                        )
                    else:
                        variant_id = self._resolve_variant_id(
                            allele,
                            ref,
                            alts,
                            alt_to_vid,
                            matched_alts,
                        )
                    if variant_id is None:
                        continue

                    symbol = fields[i_symbol] if i_symbol < len(fields) else ""
                    biotype = fields[i_biotype] if i_biotype < len(fields) else ""
                    if gene_id not in genes:
                        genes[gene_id] = (symbol, biotype)

                    sift_pred, sift_score = _parse_pred_score(
                        fields[i_sift] if i_sift is not None and i_sift < len(fields) else ""
                    )
                    pp_pred, pp_score = _parse_pred_score(
                        fields[i_polyphen]
                        if i_polyphen is not None and i_polyphen < len(fields)
                        else ""
                    )
                    cadd = fields[i_cadd] if i_cadd is not None and i_cadd < len(fields) else ""
                    revel = fields[i_revel] if i_revel is not None and i_revel < len(fields) else ""

                    consequence = fields[i_consequence] if i_consequence < len(fields) else ""
                    impact = fields[i_impact] if i_impact < len(fields) else ""
                    feature = fields[i_feature] if i_feature < len(fields) else ""
                    feature_type = fields[i_feature_type] if i_feature_type < len(fields) else ""

                    edge_writer.writerow(
                        [
                            variant_id,
                            gene_id,
                            "HAS_CONSEQUENCE",
                            consequence,
                            impact,
                            feature,
                            feature_type,
                            sift_score,
                            sift_pred,
                            pp_score,
                            pp_pred,
                            cadd,
                            revel,
                            self._annotation_source,
                            self._annotation_version,
                        ]
                    )
                    self._n_edges += 1

                if self._n_variants_seen % 500_000 == 0:
                    logger.info(
                        "Processed %d records (%d matched, %d edges so far)",
                        self._n_variants_seen,
                        self._n_variants_matched,
                        self._n_edges,
                    )

        finally:
            edge_fh.close()
            vcf.close()

        gene_path = self._out_dir / "gene_nodes.csv"
        with open(gene_path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(GENE_HEADER)
            for gene_id, (symbol, biotype) in sorted(genes.items()):
                w.writerow([gene_id, "Gene", symbol, biotype])

        self._n_genes = len(genes)

        logger.info(
            "%s parsing complete: %d VCF records seen, %d matched, "
            "%d skipped, %d genes, %d HAS_CONSEQUENCE edges",
            info_key,
            self._n_variants_seen,
            self._n_variants_matched,
            self._n_variants_skipped,
            self._n_genes,
            self._n_edges,
        )

    # -- Internals ----------------------------------------------------------

    @staticmethod
    def _detect_format(vcf: VCF) -> tuple[str, dict[str, int]]:
        """Auto-detect CSQ (VEP) or ANN (SnpEff) from VCF header."""
        csq_desc = None
        ann_desc = None

        for header_item in vcf.header_iter():
            info = header_item.info()
            hid = info.get("ID")
            if hid == "CSQ":
                csq_desc = info.get("Description", "")
            elif hid == "ANN":
                ann_desc = info.get("Description", "")

        if csq_desc is not None:
            return ("CSQ", VEPParser._parse_csq_format_from_desc(csq_desc))

        if ann_desc is not None:
            return ("ANN", VEPParser._parse_ann_format(ann_desc))

        raise ValueError("No CSQ or ANN INFO field found in VCF header")

    @staticmethod
    def _parse_csq_format_from_desc(desc: str) -> dict[str, int]:
        """Parse CSQ Description -> internal field index map."""
        idx = desc.find("Format: ")
        if idx == -1:
            raise ValueError(f"CSQ header found but no 'Format: ' in Description: {desc}")
        fmt_str = desc[idx + len("Format: ") :].rstrip('"').rstrip("'")
        csq_fields = fmt_str.split("|")

        field_idx: dict[str, int] = {}
        name_to_pos = {name: i for i, name in enumerate(csq_fields)}

        for csq_name, internal_name in _CSQ_FIELD_MAP.items():
            if csq_name in name_to_pos:
                field_idx[internal_name] = name_to_pos[csq_name]

        for req in (_FIELD_ALLELE, _FIELD_CONSEQUENCE, _FIELD_GENE):
            if req not in field_idx:
                raise ValueError(f"CSQ header missing required field: {req}")

        return field_idx

    @staticmethod
    def _parse_ann_format(desc: str) -> dict[str, int]:
        """Parse ANN Description -> internal field index map."""
        field_idx: dict[str, int] = {}
        for pos, internal_name in _ANN_INDEX_MAP.items():
            field_idx[internal_name] = pos
        return field_idx

    @staticmethod
    def _resolve_ann_variant_id(
        ann_allele: str,
        alt_to_vid: dict[str, str],
        matched_alts: set[str],
    ) -> str | None:
        """Map SnpEff ANN Allele field to a variant ID."""
        if ann_allele in alt_to_vid and ann_allele in matched_alts:
            return alt_to_vid[ann_allele]
        return None

    @staticmethod
    def _resolve_variant_id(
        vep_allele: str,
        ref: str,
        alts: list[str],
        alt_to_vid: dict[str, str],
        matched_alts: set[str],
    ) -> str | None:
        """Map VEP's Allele field back to a variant ID."""
        if vep_allele in alt_to_vid and vep_allele in matched_alts:
            return alt_to_vid[vep_allele]

        for alt in matched_alts:
            if alt not in alt_to_vid:
                continue
            if len(ref) > len(alt):
                if vep_allele == "-":
                    return alt_to_vid[alt]
            elif len(alt) > len(ref):
                inserted = alt[len(ref) :]
                if vep_allele == inserted:
                    return alt_to_vid[alt]

        return None
