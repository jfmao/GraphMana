"""Parse VCF files and extract variant/genotype data for Neo4j import.

Streaming parser that yields VariantRecord objects with per-population
allele counts, packed genotype arrays, and optional ancestral allele
annotation. Supports ploidy-aware counting (diploid, haploid, mixed).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

import numpy as np
from cyvcf2 import VCF

from graphmana.ingest.chr_reconciler import ChrReconciler
from graphmana.ingest.genotype_packer import (
    build_ploidy_packed,
    pack_phase,
    vectorized_gt_pack,
)
from graphmana.ingest.ploidy_detector import detect_ploidy
from graphmana.ingest.population_map import (
    PopulationMap,
    build_pop_map,
    load_panel,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

_SV_PREFIXES = ("<DEL", "<DUP", "<INV", "<INS", "<CNV", "<BND")


@dataclass(slots=True)
class VariantRecord:
    """Summary for a single biallelic variant site."""

    # Identity
    id: str  # "chr22:12345:A:T"
    chr: str
    pos: int
    ref: str
    alt: str
    variant_type: str  # SNP, INDEL, SV

    # Per-population arrays (indexed parallel to PopulationMap.pop_ids)
    ac: list[int]
    an: list[int]
    af: list[float]
    het_count: list[int]
    hom_alt_count: list[int]
    het_exp: list[float]

    # Global summaries
    ac_total: int = 0
    an_total: int = 0
    af_total: float = 0.0
    call_rate: float = 0.0

    # Ancestral allele annotation (optional, from Ensembl EPO FASTA)
    ancestral_allele: str | None = None  # "REF", "ALT", or None
    is_polarized: bool = False  # True if high-confidence EPO alignment

    # VCF site-level fields (for import filters)
    qual: float | None = None  # QUAL column; None if missing
    filter_status: str | None = None  # None means PASS; otherwise the FILTER string

    # Packed genotype arrays
    gt_packed: bytes = b""
    phase_packed: bytes = b""
    ploidy_packed: bytes = b""

    # Structural variant fields (optional, from INFO)
    sv_type: str | None = None  # DEL, DUP, INV, INS, BND, CNV
    sv_len: int | None = None
    sv_end: int | None = None

    # Multi-allelic tracking (set when variant was split from a multi-allelic site)
    multiallelic_site: str | None = None  # "chr:pos:ref" if split from multi-allelic
    allele_index: int | None = None  # 1-based allele index within the site


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def classify_variant(ref: str, alt: str) -> str:
    """Return 'SNP', 'INDEL', or 'SV'.

    Args:
        ref: reference allele string.
        alt: alternate allele string.
    """
    if alt.startswith("<") or any(alt.startswith(p) for p in _SV_PREFIXES):
        return "SV"
    if len(ref) == 1 and len(alt) == 1:
        return "SNP"
    return "INDEL"


def load_ancestral_fasta(fasta_path: str | Path) -> str:
    """Load an Ensembl ancestral allele FASTA into a single sequence string.

    Uppercase = high-confidence EPO alignment, lowercase = low-confidence.
    '.' or 'N' = unknown. Returns 0-indexed sequence.

    Args:
        fasta_path: path to the ancestral FASTA file.
    """
    lines: list[str] = []
    with open(fasta_path) as f:
        for line in f:
            if line.startswith(">"):
                continue
            lines.append(line.strip())
    seq = "".join(lines)
    logger.info("Loaded ancestral FASTA: %d positions from %s", len(seq), fasta_path)
    return seq


# ---------------------------------------------------------------------------
# Main parser
# ---------------------------------------------------------------------------


class VCFParser:
    """Streaming VCF reader that yields per-variant records with population stats.

    Uses cyvcf2 for fast, low-memory access to BCF/VCF files.

    Args:
        vcf_path: path to VCF/BCF file.
        panel_path: path to population panel/PED file.
        stratify_by: 'population' or 'superpopulation'.
        region: optional genomic region string (e.g. 'chr22:1-1000000').
        include_filtered: if True, include FILTER!=PASS variants.
        ancestral_fasta: optional path to ancestral allele FASTA.
        ploidy: 'auto' or 'diploid'.
        contigs: optional whitelist of contig names to process.
        chr_style: chromosome naming style ('auto', 'ucsc', 'ensembl', 'original').
        chr_map_path: optional path to custom chromosome name mapping file.
    """

    def __init__(
        self,
        vcf_path: str | Path,
        panel_path: str | Path,
        *,
        stratify_by: str = "superpopulation",
        region: str | None = None,
        include_filtered: bool = False,
        ancestral_fasta: str | Path | None = None,
        ploidy: str = "auto",
        contigs: list[str] | None = None,
        chr_style: str = "auto",
        chr_map_path: str | Path | None = None,
    ) -> None:
        self._vcf_path = str(vcf_path)
        self._region = region
        self._include_filtered = include_filtered
        self._n_variants_processed = 0
        self._n_multiallelic_skipped = 0
        self._ploidy_mode_setting = ploidy
        self._contig_whitelist = set(contigs) if contigs else None

        # Chromosome reconciler
        self._chr_reconciler = ChrReconciler(
            chr_style=chr_style,
            chr_map_path=chr_map_path,
        )

        # Load optional ancestral FASTA for allele polarization
        self._ancestral_seq: str | None = None
        if ancestral_fasta is not None:
            self._ancestral_seq = load_ancestral_fasta(Path(ancestral_fasta))

        # Load panel and build population map
        sample_to_pop, sample_to_sex = load_panel(panel_path, stratify_by)

        vcf_tmp = VCF(self._vcf_path, lazy=True)
        vcf_samples = list(vcf_tmp.samples)

        # Capture raw VCF meta-information lines (## only, no #CHROM)
        raw = vcf_tmp.raw_header or ""
        meta_lines = [line for line in raw.strip().split("\n") if line.startswith("##")]
        self._raw_header = "\n".join(meta_lines)

        # Extract contig lengths and auto-detect chr style
        self._contig_lengths: dict[str, int] = {}
        seqnames = vcf_tmp.seqnames or []
        try:
            seqlens = vcf_tmp.seqlens or []
        except AttributeError:
            # cyvcf2 raises AttributeError when no sequence lengths in header
            seqlens = []
        for name, length in zip(seqnames, seqlens):
            if length > 0:
                self._contig_lengths[name] = length
        # If contig names exist but no lengths, still record them (length 0)
        for name in seqnames:
            if name not in self._contig_lengths:
                self._contig_lengths[name] = 0
        if chr_style == "auto":
            self._chr_reconciler.detect_style(list(self._contig_lengths.keys()))

        vcf_tmp.close()

        self._pop_map = build_pop_map(vcf_samples, sample_to_pop, sample_to_sex)

        logger.info(
            "VCFParser initialised: %d samples, %d populations, %d contigs",
            len(self._pop_map.sample_ids),
            len(self._pop_map.pop_ids),
            len(self._contig_lengths),
        )

    # -- Public API ---------------------------------------------------------

    @property
    def pop_map(self) -> PopulationMap:
        """The population map built from VCF samples intersected with panel."""
        return self._pop_map

    @property
    def contig_lengths(self) -> dict[str, int]:
        """Chromosome lengths from VCF ##contig headers."""
        return self._contig_lengths

    @property
    def raw_header(self) -> str:
        """VCF meta-information lines (## only, no #CHROM)."""
        return self._raw_header

    @property
    def chr_reconciler(self) -> ChrReconciler:
        """The chromosome name reconciler."""
        return self._chr_reconciler

    @property
    def n_variants_processed(self) -> int:
        """Number of variants processed so far."""
        return self._n_variants_processed

    @property
    def n_multiallelic_skipped(self) -> int:
        """Number of multiallelic sites skipped."""
        return self._n_multiallelic_skipped

    def __iter__(self) -> Iterator[VariantRecord]:
        """Yield one VariantRecord per qualifying variant site."""
        vcf = VCF(self._vcf_path, lazy=True, threads=2)
        try:
            yield from self._stream(vcf)
        finally:
            vcf.close()

    def iter_chunks(self, chunk_size: int = 100_000) -> Iterator[list[VariantRecord]]:
        """Yield lists of up to *chunk_size* VariantRecords."""
        chunk: list[VariantRecord] = []
        for rec in self:
            chunk.append(rec)
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk

    # -- Internals ----------------------------------------------------------

    def _stream(self, vcf: VCF) -> Iterator[VariantRecord]:
        """Core streaming loop over VCF records.

        Uses a site-level buffer to detect split multi-allelic variants.
        Consecutive records sharing the same (chrom, pos, ref) are tagged with
        multiallelic_site and allele_index before being yielded.
        """
        pop_map = self._pop_map
        pop_ids = pop_map.pop_ids
        pop_indices = [pop_map.pop_to_indices[p] for p in pop_ids]
        n_pops = len(pop_ids)
        n_total_samples = len(vcf.samples)

        # Ploidy cache: chr → (mode, haploid_flags, ploidy_packed_bytes)
        chr_ploidy_cache: dict[str, tuple[str, np.ndarray, bytes]] = {}
        force_diploid = self._ploidy_mode_setting == "diploid"

        iterator = vcf(self._region) if self._region else vcf

        # Site buffer for multi-allelic detection
        site_buffer: list[VariantRecord] = []
        current_site_key: tuple[str, int, str] | None = None

        for v in iterator:
            # --- Filter checks ---
            if not self._include_filtered and v.FILTER is not None:
                continue

            alts = v.ALT
            if not alts or alts[0] in (".", "<*>", "*"):
                continue

            if len(alts) > 1:
                self._n_multiallelic_skipped += 1
                continue

            # --- Identity ---
            chrom = self._chr_reconciler.normalize(v.CHROM)
            pos = v.POS
            ref = v.REF
            alt = alts[0]

            # Contig whitelist filter
            if self._contig_whitelist and chrom not in self._contig_whitelist:
                continue

            variant_id = f"{chrom}:{pos}:{ref}:{alt}"
            variant_type = classify_variant(ref, alt)

            # --- Genotypes (explicit copy from C memory) ---
            gt_types = np.array(v.gt_types, copy=True)

            # --- Ploidy detection (once per chromosome) ---
            if force_diploid:
                ploidy_mode = "all_diploid"
                haploid_flags = None
                ploidy_packed_bytes = b""
            elif chrom in chr_ploidy_cache:
                ploidy_mode, haploid_flags, ploidy_packed_bytes = chr_ploidy_cache[chrom]
            else:
                ploidy_mode, haploid_flags = detect_ploidy(v)
                if ploidy_mode == "all_diploid":
                    ploidy_packed_bytes = b""
                    haploid_flags = None
                else:
                    ploidy_packed_bytes = build_ploidy_packed(haploid_flags)
                chr_ploidy_cache[chrom] = (ploidy_mode, haploid_flags, ploidy_packed_bytes)
                if ploidy_mode != "all_diploid":
                    logger.info(
                        "Chromosome %s: ploidy mode=%s (%d haploid samples)",
                        chrom,
                        ploidy_mode,
                        int(haploid_flags.sum()) if haploid_flags is not None else 0,
                    )

            # --- Per-population stats ---
            ac = [0] * n_pops
            an = [0] * n_pops
            af = [0.0] * n_pops
            het_count = [0] * n_pops
            hom_alt_count = [0] * n_pops
            het_exp = [0.0] * n_pops

            if ploidy_mode == "all_diploid":
                for k in range(n_pops):
                    idx = pop_indices[k]
                    gt_k = gt_types[idx]

                    n_miss = int(np.sum(gt_k == 2))
                    n_het = int(np.sum(gt_k == 1))
                    n_hom_alt = int(np.sum(gt_k == 3))
                    n_called = len(idx) - n_miss

                    ac_k = n_het + 2 * n_hom_alt
                    an_k = 2 * n_called

                    ac[k] = ac_k
                    an[k] = an_k
                    af_k = ac_k / an_k if an_k > 0 else 0.0
                    af[k] = af_k
                    het_count[k] = n_het
                    hom_alt_count[k] = n_hom_alt
                    het_exp[k] = 2.0 * af_k * (1.0 - af_k)

            elif ploidy_mode == "all_haploid":
                for k in range(n_pops):
                    idx = pop_indices[k]
                    gt_k = gt_types[idx]

                    n_miss = int(np.sum(gt_k == 2))
                    n_hom_alt = int(np.sum(gt_k == 3))
                    n_called = len(idx) - n_miss

                    ac_k = n_hom_alt
                    an_k = n_called

                    ac[k] = ac_k
                    an[k] = an_k
                    af_k = ac_k / an_k if an_k > 0 else 0.0
                    af[k] = af_k
                    het_count[k] = 0
                    hom_alt_count[k] = n_hom_alt
                    het_exp[k] = 2.0 * af_k * (1.0 - af_k)

            else:
                # MIXED: per-sample diploid/haploid split (chrX, chrY)
                for k in range(n_pops):
                    idx = pop_indices[k]
                    gt_k = gt_types[idx]
                    hap_k = haploid_flags[idx]

                    # Diploid subset
                    dip_mask = ~hap_k
                    gt_dip = gt_k[dip_mask]
                    n_miss_dip = int(np.sum(gt_dip == 2))
                    n_het_dip = int(np.sum(gt_dip == 1))
                    n_hom_alt_dip = int(np.sum(gt_dip == 3))
                    n_called_dip = len(gt_dip) - n_miss_dip
                    ac_dip = n_het_dip + 2 * n_hom_alt_dip
                    an_dip = 2 * n_called_dip

                    # Haploid subset
                    gt_hap = gt_k[hap_k]
                    n_miss_hap = int(np.sum(gt_hap == 2))
                    n_hom_alt_hap = int(np.sum(gt_hap == 3))
                    n_called_hap = len(gt_hap) - n_miss_hap
                    ac_hap = n_hom_alt_hap
                    an_hap = n_called_hap

                    ac_k = ac_dip + ac_hap
                    an_k = an_dip + an_hap

                    ac[k] = ac_k
                    an[k] = an_k
                    af_k = ac_k / an_k if an_k > 0 else 0.0
                    af[k] = af_k
                    het_count[k] = n_het_dip
                    hom_alt_count[k] = n_hom_alt_dip + n_hom_alt_hap
                    het_exp[k] = 2.0 * af_k * (1.0 - af_k)

            ac_total = sum(ac)
            an_total = sum(an)
            af_total = ac_total / an_total if an_total > 0 else 0.0

            n_missing_total = int(np.sum(gt_types == 2))
            call_rate = (
                (n_total_samples - n_missing_total) / n_total_samples
                if n_total_samples > 0
                else 0.0
            )

            # --- Packed genotype arrays ---
            gt_packed_data = vectorized_gt_pack(gt_types)

            # --- Phase packing (for het sites) ---
            het_idx = np.flatnonzero(gt_types == 1)
            if len(het_idx) > 0:
                phase_packed_data = pack_phase(len(gt_types), het_idx, v.genotypes)
            else:
                phase_packed_len = (len(gt_types) + 7) >> 3
                phase_packed_data = bytes(phase_packed_len)

            # --- Ancestral allele annotation (optional) ---
            ancestral_allele: str | None = None
            is_polarized = False
            if self._ancestral_seq is not None:
                fasta_idx = pos - 1  # 0-indexed
                if 0 <= fasta_idx < len(self._ancestral_seq):
                    anc_char = self._ancestral_seq[fasta_idx]
                    anc_upper = anc_char.upper()
                    if anc_upper not in (".", "N", "-"):
                        if anc_upper == ref[0].upper():
                            ancestral_allele = "REF"
                            is_polarized = anc_char.isupper()
                        elif anc_upper == alt[0].upper():
                            ancestral_allele = "ALT"
                            is_polarized = anc_char.isupper()

            self._n_variants_processed += 1
            if self._n_variants_processed % 100_000 == 0:
                logger.info(
                    "Processed %d variants (current: %s)",
                    self._n_variants_processed,
                    variant_id,
                )

            # Site-level quality and filter
            raw_qual = v.QUAL
            qual_val = float(raw_qual) if raw_qual is not None else None
            filter_val = v.FILTER  # None = PASS

            # Structural variant INFO fields
            sv_type_val: str | None = None
            sv_len_val: int | None = None
            sv_end_val: int | None = None
            if variant_type == "SV":
                try:
                    sv_type_val = v.INFO.get("SVTYPE")
                except (KeyError, TypeError):
                    pass
                try:
                    raw_svlen = v.INFO.get("SVLEN")
                    if raw_svlen is not None:
                        sv_len_val = (
                            int(raw_svlen)
                            if not isinstance(raw_svlen, tuple)
                            else int(raw_svlen[0])
                        )
                except (KeyError, TypeError, ValueError):
                    pass
                try:
                    raw_end = v.INFO.get("END")
                    if raw_end is not None:
                        sv_end_val = int(raw_end)
                except (KeyError, TypeError, ValueError):
                    pass

            rec = VariantRecord(
                id=variant_id,
                chr=chrom,
                pos=pos,
                ref=ref,
                alt=alt,
                variant_type=variant_type,
                ac=ac,
                an=an,
                af=af,
                het_count=het_count,
                hom_alt_count=hom_alt_count,
                het_exp=het_exp,
                ac_total=ac_total,
                an_total=an_total,
                af_total=af_total,
                call_rate=call_rate,
                ancestral_allele=ancestral_allele,
                is_polarized=is_polarized,
                qual=qual_val,
                filter_status=filter_val,
                gt_packed=gt_packed_data,
                phase_packed=phase_packed_data,
                ploidy_packed=ploidy_packed_bytes,
                sv_type=sv_type_val,
                sv_len=sv_len_val,
                sv_end=sv_end_val,
            )

            # Buffer records to detect split multi-allelic sites
            site_key = (chrom, pos, ref)
            if current_site_key is not None and site_key != current_site_key:
                yield from self._flush_site_buffer(site_buffer)
                site_buffer = []
            current_site_key = site_key
            site_buffer.append(rec)

        # Flush remaining buffer
        if site_buffer:
            yield from self._flush_site_buffer(site_buffer)

        logger.info(
            "VCF parsing complete: %d variants processed",
            self._n_variants_processed,
        )
        if self._n_multiallelic_skipped > 0:
            logger.warning(
                "%d multi-allelic sites skipped (%.1f%% of encountered sites). "
                "To retain these variants, decompose before import with: "
                "bcftools norm -m -both INPUT.vcf.gz -Oz -o OUTPUT.vcf.gz",
                self._n_multiallelic_skipped,
                100.0
                * self._n_multiallelic_skipped
                / (self._n_variants_processed + self._n_multiallelic_skipped),
            )

    @staticmethod
    def _flush_site_buffer(buffer: list[VariantRecord]) -> Iterator[VariantRecord]:
        """Tag and yield buffered records, marking multi-allelic groups."""
        if len(buffer) > 1:
            site_key = f"{buffer[0].chr}:{buffer[0].pos}:{buffer[0].ref}"
            for idx, rec in enumerate(buffer, start=1):
                rec.multiallelic_site = site_key
                rec.allele_index = idx
        # Single-record buffers: fields remain None (native biallelic)
        yield from buffer
