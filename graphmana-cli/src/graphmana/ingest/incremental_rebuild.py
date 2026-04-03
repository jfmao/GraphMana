"""Incremental sample addition via export-extend-reimport.

Bypasses Neo4j's transaction engine for packed array updates by:
1. Reading existing variant data from Neo4j (read-only, batched)
2. Parsing new VCF for incoming samples
3. Extending packed arrays and merging pop stats in Python (numpy)
4. Writing merged CSV files in neo4j-admin import format
5. Rebuilding the database via neo4j-admin import (direct store writes)

Optimizations:
- Fast HomRef extension: appends zero bytes instead of unpack/repack (~95% of variants)
- Precomputed HomRef pop stats: merge computed once, reused for all HomRef variants
- Multi-chromosome parallelism: ProcessPoolExecutor when threads > 1
"""

from __future__ import annotations

import csv
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from graphmana.db.queries import ACTIVE_SAMPLE_FILTER
from graphmana.ingest.array_ops import (
    extend_gt_packed,
    extend_phase_packed,
    extend_ploidy_packed,
    merge_pop_stats,
    pad_gt_for_new_variant,
    pad_phase_for_new_variant,
)
from graphmana.ingest.csv_emitter import (
    CHROMOSOME_HEADER,
    CHR_LENGTHS,
    IN_POPULATION_HEADER,
    NEXT_HEADER,
    ON_CHROMOSOME_HEADER,
    POPULATION_HEADER,
    SAMPLE_HEADER,
    VARIANT_HEADER,
    _fmt_float,
    _harmonic,
    _harmonic2,
)

logger = logging.getLogger(__name__)

DELIM = ";"
# Read batch for paged variant queries during rebuild. Smaller than
# DEFAULT_BATCH_SIZE (100K) to limit peak memory when each row carries
# gt_packed + phase_packed byte arrays (~1.6 KB/variant at 3K samples).
BATCH_READ_SIZE = 50_000

# Cypher queries -------------------------------------------------------

FETCH_VARIANTS_PAGED = """
MATCH (v:Variant) WHERE v.chr = $chr AND v.pos > $lastPos
RETURN v.variantId AS variantId, v.chr AS chr, v.pos AS pos,
       v.ref AS ref, v.alt AS alt, v.variant_type AS variant_type,
       v.gt_packed AS gt_packed, v.phase_packed AS phase_packed,
       v.ploidy_packed AS ploidy_packed,
       v.pop_ids AS pop_ids, v.ac AS ac, v.an AS an, v.af AS af,
       v.het_count AS het_count, v.hom_alt_count AS hom_alt_count,
       v.het_exp AS het_exp,
       v.ac_total AS ac_total, v.an_total AS an_total,
       v.af_total AS af_total, v.call_rate AS call_rate,
       v.ancestral_allele AS ancestral_allele,
       v.is_polarized AS is_polarized,
       v.qual AS qual, v.filter AS filter,
       v.sv_type AS sv_type, v.sv_len AS sv_len, v.sv_end AS sv_end,
       v.multiallelic_site AS multiallelic_site,
       v.allele_index AS allele_index
ORDER BY v.pos
LIMIT $limit
"""

FETCH_CHROMOSOMES = """
MATCH (c:Chromosome)
RETURN c.chromosomeId AS chr, c.length AS length
ORDER BY c.chromosomeId
"""

FETCH_SAMPLES = f"""
MATCH (s:Sample)-[:IN_POPULATION]->(p:Population)
WHERE {ACTIVE_SAMPLE_FILTER}
RETURN s.sampleId AS sampleId, s.packed_index AS packed_index,
       s.population AS population, s.sex AS sex,
       s.source_dataset AS source_dataset, s.source_file AS source_file,
       s.ingestion_date AS ingestion_date
ORDER BY s.packed_index
"""

FETCH_POPULATIONS = """
MATCH (p:Population)
RETURN p.populationId AS populationId, p.name AS name,
       p.n_samples AS n_samples, p.a_n AS a_n, p.a_n2 AS a_n2
ORDER BY p.populationId
"""


# Helpers --------------------------------------------------------------


def _bytes_to_csv(data: bytes | None) -> str:
    if not data:
        return ""
    return DELIM.join(str(b if b < 128 else b - 256) for b in data)


def _arr_int(arr) -> list[int]:
    if arr is None:
        return []
    return [int(x) for x in arr]


def _arr_str(arr) -> list[str]:
    if arr is None:
        return []
    return [str(x) for x in arr]


def _neo4j_bytes(val) -> bytes:
    if val is None:
        return b""
    if isinstance(val, (bytes, bytearray)):
        return bytes(val)
    if isinstance(val, list):
        return bytes(b & 0xFF for b in val)
    return bytes(val)


def _gt_packed_length(n_samples: int) -> int:
    return (n_samples + 3) >> 2


def _phase_packed_length(n_samples: int) -> int:
    return (n_samples + 7) >> 3


def _extend_homref_fast(old_bytes: bytes, n_existing: int, n_total: int, is_gt: bool) -> bytes:
    """Extend packed array with zeros (HomRef/unphased/diploid) by appending zero bytes.

    For HomRef genotypes (code 0), unphased (bit 0), and diploid (bit 0),
    the new bits are all zero. Since zero bytes are the default, we just
    need to extend the array to the new length with zero padding.
    """
    new_len = _gt_packed_length(n_total) if is_gt else _phase_packed_length(n_total)
    if len(old_bytes) >= new_len:
        return old_bytes[:new_len]
    return old_bytes + b"\x00" * (new_len - len(old_bytes))


def _write_variant_csv_row(
    writer, vid, chrom, pos, ref, alt, vtype,
    gt_packed, phase_packed, ploidy_packed,
    pop_ids, ac, an, af, het_count, hom_alt_count, het_exp,
    ac_total, an_total, af_total, call_rate,
    anc_allele, is_polarized, qual, filt,
    sv_type, sv_len, sv_end, multiallelic, allele_idx,
):
    writer.writerow([
        vid, "Variant", chrom, pos, ref, alt, vtype or "",
        DELIM.join(pop_ids),
        DELIM.join(str(x) for x in ac),
        DELIM.join(str(x) for x in an),
        DELIM.join(_fmt_float(x) for x in af),
        DELIM.join(str(x) for x in het_count),
        DELIM.join(str(x) for x in hom_alt_count),
        ac_total, an_total,
        _fmt_float(af_total), _fmt_float(call_rate),
        DELIM.join(_fmt_float(x) for x in het_exp),
        anc_allele or "",
        str(is_polarized).lower() if is_polarized is not None else "",
        _fmt_float(qual) if qual is not None else "",
        filt or "PASS",
        _bytes_to_csv(gt_packed), _bytes_to_csv(phase_packed), _bytes_to_csv(ploidy_packed),
        sv_type or "",
        sv_len if sv_len is not None else "",
        sv_end if sv_end is not None else "",
        multiallelic or "",
        allele_idx if allele_idx is not None else "",
    ])


# CSV column indices (matching VARIANT_HEADER order)
_COL_VID = 0
_COL_LABEL = 1
_COL_CHR = 2
_COL_POS = 3
_COL_REF = 4
_COL_ALT = 5
_COL_VTYPE = 6
_COL_POPIDS = 7
_COL_AC = 8
_COL_AN = 9
_COL_AF = 10
_COL_HET = 11
_COL_HOM = 12
_COL_AC_TOTAL = 13
_COL_AN_TOTAL = 14
_COL_AF_TOTAL = 15
_COL_CALLRATE = 16
_COL_HETEXP = 17
_COL_ANC = 18
_COL_POLAR = 19
_COL_QUAL = 20
_COL_FILTER = 21
_COL_GT = 22
_COL_PHASE = 23
_COL_PLOIDY = 24
_COL_SVTYPE = 25
_COL_SVLEN = 26
_COL_SVEND = 27
_COL_MULTI = 28
_COL_ALLIDX = 29


def _csv_bytes_to_python(csv_str: str) -> bytes:
    """Parse semicolon-delimited signed Java bytes back to Python bytes."""
    if not csv_str:
        return b""
    return bytes(int(x) & 0xFF for x in csv_str.split(DELIM))


def _csv_int_list(csv_str: str) -> list[int]:
    if not csv_str:
        return []
    return [int(x) for x in csv_str.split(DELIM)]


def _csv_str_list(csv_str: str) -> list[str]:
    if not csv_str:
        return []
    return csv_str.split(DELIM)


def run_incremental_from_csv(
    existing_csv_dir: str | Path,
    vcf_path: str | Path,
    panel_path: str | Path,
    output_csv_dir: str | Path,
    *,
    neo4j_home: str | Path,
    n_existing: int,
    stratify_by: str = "superpopulation",
    include_filtered: bool = False,
    region: str | None = None,
    dataset_id: str = "",
    source_file: str = "",
    database: str = "neo4j",
    threads: int = 1,
) -> dict:
    """Incremental import via CSV-to-CSV: reads existing CSV directly, no Neo4j.

    This is the fastest path for whole-genome incremental addition. Instead
    of querying 70M+ variants through Bolt, it reads the existing
    variant_nodes.csv (the checkpoint from prepare-csv) at NVMe speed,
    extends packed arrays, and writes a new CSV for neo4j-admin import.

    Args:
        existing_csv_dir: directory containing the base CSV files (checkpoint).
        vcf_path: new VCF file with additional samples.
        panel_path: population map for new samples.
        output_csv_dir: directory for the merged CSV output.
        neo4j_home: Neo4j installation directory (for import + restart).
        n_existing: number of samples in the base CSV.
        stratify_by: population column in the panel.
        dataset_id: provenance identifier.
        source_file: VCF source path string.
        database: Neo4j database name.
        threads: number of parallel workers (future use).

    Returns:
        Summary dict with counts.
    """
    from graphmana.ingest.genotype_packer import unpack_genotypes, unpack_phase
    from graphmana.ingest.loader import run_load_csv
    from graphmana.ingest.vcf_parser import VCFParser

    existing_csv_dir = Path(existing_csv_dir)
    output_csv_dir = Path(output_csv_dir)
    output_csv_dir.mkdir(parents=True, exist_ok=True)

    existing_var_csv = existing_csv_dir / "variant_nodes.csv"
    if not existing_var_csv.exists():
        raise FileNotFoundError(
            f"No variant_nodes.csv in {existing_csv_dir}. "
            "Run prepare-csv first, or use the Neo4j-based rebuild."
        )

    # ------------------------------------------------------------------
    # 1. Parse new VCF
    # ------------------------------------------------------------------
    logger.info("Parsing new VCF: %s", vcf_path)
    parser = VCFParser(
        vcf_path, panel_path,
        stratify_by=stratify_by,
        region=region,
        include_filtered=include_filtered,
    )
    pop_map_new = parser.pop_map
    n_new = len(pop_map_new.sample_ids)
    n_total = n_existing + n_new
    logger.info("  %d new samples, %d total after merge", n_new, n_total)

    reverse_remap = np.array([0, 1, 3, 2], dtype=np.int8)
    new_by_vid: dict[str, dict] = {}
    new_chroms: set[str] = set()
    for rec in parser:
        packed_codes = unpack_genotypes(rec.gt_packed, n_new)
        cyvcf2_codes = reverse_remap[packed_codes]
        phase_bits = unpack_phase(rec.phase_packed, n_new)
        ploidy_bits = np.zeros(n_new, dtype=np.uint8)
        if rec.ploidy_packed:
            from graphmana.ingest.genotype_packer import unpack_ploidy
            ploidy_bits = unpack_ploidy(rec.ploidy_packed, n_new)

        new_by_vid[rec.id] = {
            "gt_types": cyvcf2_codes,
            "phase_bits": phase_bits,
            "ploidy_bits": ploidy_bits,
            "pop_ids": pop_map_new.pop_ids,
            "ac": list(rec.ac), "an": list(rec.an),
            "het_count": list(rec.het_count), "hom_alt_count": list(rec.hom_alt_count),
            "pos": rec.pos, "ref": rec.ref, "alt": rec.alt,
            "chr": rec.chr, "variant_type": rec.variant_type,
            "multiallelic_site": rec.multiallelic_site,
            "allele_index": rec.allele_index,
        }
        new_chroms.add(rec.chr)
    logger.info("  %d new VCF variants on %d chromosomes", len(new_by_vid), len(new_chroms))

    # Precompute HomRef pop stats for new populations
    new_pop_ids = pop_map_new.pop_ids
    homref_new_ac = [0] * len(new_pop_ids)
    homref_new_an = [2 * pop_map_new.n_samples_per_pop[pid] for pid in new_pop_ids]
    homref_new_het = [0] * len(new_pop_ids)
    homref_new_hom = [0] * len(new_pop_ids)

    # ------------------------------------------------------------------
    # 2. Stream existing CSV, extend, write new CSV
    # ------------------------------------------------------------------
    logger.info("Reading existing CSV: %s", existing_var_csv)

    f_out = open(output_csv_dir / "variant_nodes.csv", "w", newline="")
    f_next = open(output_csv_dir / "next_edges.csv", "w", newline="")
    f_onchr = open(output_csv_dir / "on_chromosome_edges.csv", "w", newline="")
    w_out = csv.writer(f_out)
    w_next = csv.writer(f_next)
    w_onchr = csv.writer(f_onchr)

    w_out.writerow(VARIANT_HEADER)
    w_next.writerow(NEXT_HEADER)
    w_onchr.writerow(ON_CHROMOSOME_HEADER)

    n_extended = 0
    n_homref = 0
    n_total_variants = 0
    prev_vid = None
    prev_pos = None
    prev_chr = None
    all_chroms: set[str] = set()
    new_vids_used: set[str] = set()

    with open(existing_var_csv, "r") as fin:
        reader = csv.reader(fin)
        header = next(reader)  # skip header

        for row in reader:
            vid = row[_COL_VID]
            chrom = row[_COL_CHR]
            pos = int(row[_COL_POS])
            all_chroms.add(chrom)

            if vid in new_by_vid:
                # EXTEND: variant has actual genotypes in new VCF
                nv = new_by_vid[vid]
                new_vids_used.add(vid)

                old_gt = _csv_bytes_to_python(row[_COL_GT])
                old_phase = _csv_bytes_to_python(row[_COL_PHASE])
                old_ploidy = _csv_bytes_to_python(row[_COL_PLOIDY])

                gt_packed = extend_gt_packed(old_gt, n_existing, nv["gt_types"])
                phase_packed = extend_phase_packed(old_phase, n_existing, nv["phase_bits"])
                ploidy_packed = extend_ploidy_packed(old_ploidy, n_existing, nv["ploidy_bits"])

                old_pop_ids = _csv_str_list(row[_COL_POPIDS])
                merged = merge_pop_stats(
                    old_pop_ids,
                    _csv_int_list(row[_COL_AC]), _csv_int_list(row[_COL_AN]),
                    _csv_int_list(row[_COL_HET]), _csv_int_list(row[_COL_HOM]),
                    nv["pop_ids"], nv["ac"], nv["an"], nv["het_count"], nv["hom_alt_count"],
                )
                call_rate = merged["an_total"] / (2 * n_total) if n_total > 0 else 0.0
                n_extended += 1

                _write_variant_csv_row(
                    w_out, vid, chrom, pos, row[_COL_REF], row[_COL_ALT], row[_COL_VTYPE],
                    gt_packed, phase_packed, ploidy_packed,
                    merged["pop_ids"], merged["ac"], merged["an"], merged["af"],
                    merged["het_count"], merged["hom_alt_count"], merged["het_exp"],
                    merged["ac_total"], merged["an_total"], merged["af_total"], call_rate,
                    row[_COL_ANC] or None, row[_COL_POLAR] or None,
                    float(row[_COL_QUAL]) if row[_COL_QUAL] else None,
                    row[_COL_FILTER],
                    row[_COL_SVTYPE] or None,
                    int(row[_COL_SVLEN]) if row[_COL_SVLEN] else None,
                    int(row[_COL_SVEND]) if row[_COL_SVEND] else None,
                    row[_COL_MULTI] or None,
                    int(row[_COL_ALLIDX]) if row[_COL_ALLIDX] else None,
                )
            else:
                # HOMREF: extend with zero bytes (fast path)
                old_gt_str = row[_COL_GT]
                old_phase_str = row[_COL_PHASE]
                old_ploidy_str = row[_COL_PLOIDY]

                # Fast HomRef: just append zero bytes to the CSV representation
                old_gt = _csv_bytes_to_python(old_gt_str)
                gt_packed = _extend_homref_fast(old_gt, n_existing, n_total, is_gt=True)
                old_phase = _csv_bytes_to_python(old_phase_str)
                phase_packed = _extend_homref_fast(old_phase, n_existing, n_total, is_gt=False)
                if old_ploidy_str:
                    old_ploidy = _csv_bytes_to_python(old_ploidy_str)
                    ploidy_packed = _extend_homref_fast(old_ploidy, n_existing, n_total, is_gt=False)
                else:
                    ploidy_packed = b""

                # HomRef pop stats merge (per-variant since ac/an differ)
                old_pop_ids = _csv_str_list(row[_COL_POPIDS])
                old_ac = _csv_int_list(row[_COL_AC])
                old_an = _csv_int_list(row[_COL_AN])
                old_het = _csv_int_list(row[_COL_HET])
                old_hom = _csv_int_list(row[_COL_HOM])

                old_map = {pid: i for i, pid in enumerate(old_pop_ids)}
                new_map = {pid: i for i, pid in enumerate(new_pop_ids)}
                all_pids = sorted(set(old_pop_ids) | set(new_pop_ids))

                m_ac, m_an, m_af, m_het, m_hom, m_he = [], [], [], [], [], []
                for pid in all_pids:
                    ac = old_ac[old_map[pid]] if pid in old_map else 0
                    an = (old_an[old_map[pid]] if pid in old_map else 0) + \
                         (homref_new_an[new_map[pid]] if pid in new_map else 0)
                    het = old_het[old_map[pid]] if pid in old_map else 0
                    hom = old_hom[old_map[pid]] if pid in old_map else 0
                    af = ac / an if an > 0 else 0.0
                    m_ac.append(ac); m_an.append(an); m_af.append(af)
                    m_het.append(het); m_hom.append(hom)
                    m_he.append(2.0 * af * (1.0 - af))
                ac_total = sum(m_ac)
                an_total = sum(m_an)
                af_total = ac_total / an_total if an_total > 0 else 0.0
                call_rate = an_total / (2 * n_total) if n_total > 0 else 0.0
                n_homref += 1

                _write_variant_csv_row(
                    w_out, vid, chrom, pos, row[_COL_REF], row[_COL_ALT], row[_COL_VTYPE],
                    gt_packed, phase_packed, ploidy_packed,
                    all_pids, m_ac, m_an, m_af, m_het, m_hom, m_he,
                    ac_total, an_total, af_total, call_rate,
                    row[_COL_ANC] or None, row[_COL_POLAR] or None,
                    float(row[_COL_QUAL]) if row[_COL_QUAL] else None,
                    row[_COL_FILTER],
                    row[_COL_SVTYPE] or None,
                    int(row[_COL_SVLEN]) if row[_COL_SVLEN] else None,
                    int(row[_COL_SVEND]) if row[_COL_SVEND] else None,
                    row[_COL_MULTI] or None,
                    int(row[_COL_ALLIDX]) if row[_COL_ALLIDX] else None,
                )

            # NEXT edge (within same chromosome)
            if prev_vid is not None and prev_chr == chrom:
                w_next.writerow([prev_vid, vid, "NEXT", pos - prev_pos])
            w_onchr.writerow([vid, chrom, "ON_CHROMOSOME"])
            prev_vid = vid
            prev_pos = pos
            prev_chr = chrom
            n_total_variants += 1

            if n_total_variants % 1_000_000 == 0:
                logger.info("  Processed %dM variants (%d extended, %d homref)",
                            n_total_variants // 1_000_000, n_extended, n_homref)

    # CREATE: variants in new VCF but not in existing CSV
    n_created = 0
    new_only = set(new_by_vid.keys()) - new_vids_used
    if new_only:
        # Read existing population info from population_nodes.csv for HomRef padding
        existing_pops_csv = existing_csv_dir / "population_nodes.csv"
        existing_pop_info = {}
        if existing_pops_csv.exists():
            with open(existing_pops_csv) as f:
                pr = csv.reader(f)
                next(pr)  # skip header
                for prow in pr:
                    existing_pop_info[prow[0]] = {"n_samples": int(prow[3])}

        existing_pids = sorted(existing_pop_info.keys())
        existing_an_homref = [2 * existing_pop_info[pid]["n_samples"] for pid in existing_pids]
        existing_zeros = [0] * len(existing_pids)

        for vid in sorted(new_only, key=lambda v: (new_by_vid[v]["chr"], new_by_vid[v]["pos"])):
            nv = new_by_vid[vid]
            gt_packed = pad_gt_for_new_variant(n_existing, nv["gt_types"])
            phase_packed = pad_phase_for_new_variant(n_existing, nv["phase_bits"])
            ploidy_packed = extend_ploidy_packed(None, n_existing, nv["ploidy_bits"])

            merged = merge_pop_stats(
                existing_pids, existing_zeros, existing_an_homref,
                existing_zeros, existing_zeros,
                nv["pop_ids"], nv["ac"], nv["an"], nv["het_count"], nv["hom_alt_count"],
            )
            call_rate = merged["an_total"] / (2 * n_total) if n_total > 0 else 0.0

            _write_variant_csv_row(
                w_out, vid, nv["chr"], nv["pos"], nv["ref"], nv["alt"], nv["variant_type"],
                gt_packed, phase_packed, ploidy_packed,
                merged["pop_ids"], merged["ac"], merged["an"], merged["af"],
                merged["het_count"], merged["hom_alt_count"], merged["het_exp"],
                merged["ac_total"], merged["an_total"], merged["af_total"], call_rate,
                None, None, None, "PASS", None, None, None,
                nv.get("multiallelic_site"), nv.get("allele_index"),
            )
            if prev_vid is not None and prev_chr == nv["chr"]:
                w_next.writerow([prev_vid, vid, "NEXT", nv["pos"] - prev_pos])
            w_onchr.writerow([vid, nv["chr"], "ON_CHROMOSOME"])
            prev_vid = vid
            prev_pos = nv["pos"]
            prev_chr = nv["chr"]
            all_chroms.add(nv["chr"])
            n_created += 1

    f_out.close()
    f_next.close()
    f_onchr.close()

    logger.info(
        "CSV-to-CSV complete: %d extended, %d homref, %d new, %d total",
        n_extended, n_homref, n_created, n_total_variants + n_created,
    )

    # ------------------------------------------------------------------
    # 3. Copy/generate non-variant CSVs
    # ------------------------------------------------------------------
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Read existing samples from CSV
    existing_samples = []
    with open(existing_csv_dir / "sample_nodes.csv") as f:
        sr = csv.reader(f)
        hdr = next(sr)
        for srow in sr:
            existing_samples.append(srow)

    with open(output_csv_dir / "sample_nodes.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(SAMPLE_HEADER)
        for srow in existing_samples:
            w.writerow(srow)
        for i, sid in enumerate(pop_map_new.sample_ids):
            pop = pop_map_new.sample_to_pop[sid]
            sex = pop_map_new.sample_to_sex.get(sid, 0)
            w.writerow([sid, "Sample", pop, n_existing + i, sex, dataset_id, source_file, now])

    # Read existing populations, merge with new
    existing_pop_map = {}
    with open(existing_csv_dir / "population_nodes.csv") as f:
        pr = csv.reader(f)
        next(pr)
        for prow in pr:
            existing_pop_map[prow[0]] = {"n_samples": int(prow[3])}

    merged_pops = dict(existing_pop_map)
    for pid in pop_map_new.pop_ids:
        n = pop_map_new.n_samples_per_pop[pid]
        if pid in merged_pops:
            merged_pops[pid]["n_samples"] += n
        else:
            merged_pops[pid] = {"n_samples": n}

    with open(output_csv_dir / "population_nodes.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(POPULATION_HEADER)
        for pid in sorted(merged_pops.keys()):
            ns = merged_pops[pid]["n_samples"]
            an = _harmonic(2 * ns - 1) if ns > 0 else 0.0
            an2 = _harmonic2(2 * ns - 1) if ns > 0 else 0.0
            w.writerow([pid, "Population", pid, ns, _fmt_float(an), _fmt_float(an2)])

    with open(output_csv_dir / "chromosome_nodes.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(CHROMOSOME_HEADER)
        for chrom in sorted(all_chroms):
            w.writerow([chrom, "Chromosome", CHR_LENGTHS.get(chrom, 0)])

    with open(output_csv_dir / "in_population_edges.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(IN_POPULATION_HEADER)
        for srow in existing_samples:
            w.writerow([srow[0], srow[2], "IN_POPULATION"])
        for sid in pop_map_new.sample_ids:
            w.writerow([sid, pop_map_new.sample_to_pop[sid], "IN_POPULATION"])

    # ------------------------------------------------------------------
    # 4. Stop Neo4j + import + restart
    # ------------------------------------------------------------------
    from graphmana.cluster.neo4j_lifecycle import start_neo4j, stop_neo4j

    logger.info("Stopping Neo4j for reimport...")
    stop_neo4j(neo4j_home)

    logger.info("Running neo4j-admin import...")
    run_load_csv(csv_dir=output_csv_dir, neo4j_home=neo4j_home, database=database, overwrite=True)

    logger.info("Restarting Neo4j...")
    start_neo4j(neo4j_home, wait=True)

    return {
        "n_variants_extended": n_extended,
        "n_variants_homref_extended": n_homref,
        "n_variants_created": n_created,
        "n_samples_created": n_new,
        "n_total_samples": n_total,
        "mode": "incremental_csv",
    }


# Per-chromosome worker ------------------------------------------------


def _process_one_chromosome(
    chrom: str,
    new_variants: dict,
    conn_params: dict,
    n_existing: int,
    n_new: int,
    n_total: int,
    new_pop_ids: list[str],
    homref_new_an: list[int],
    existing_pop_map: dict,
    output_dir: Path,
) -> dict:
    """Process one chromosome: fetch from DB, extend, write CSV files.

    This function is designed to run in a subprocess (ProcessPoolExecutor).
    It opens its own Neo4j connection.

    Returns dict with counts and file paths.
    """
    from graphmana.db.connection import GraphManaConnection

    output_dir.mkdir(parents=True, exist_ok=True)
    var_path = output_dir / f"variant_nodes_{chrom}.csv"
    next_path = output_dir / f"next_edges_{chrom}.csv"
    onchr_path = output_dir / f"on_chromosome_edges_{chrom}.csv"

    f_var = open(var_path, "w", newline="")
    f_next = open(next_path, "w", newline="")
    f_onchr = open(onchr_path, "w", newline="")
    w_var = csv.writer(f_var)
    w_next = csv.writer(f_next)
    w_onchr = csv.writer(f_onchr)

    # No header — headers written in the merge step
    new_ids_remaining = set(new_variants.keys())
    n_extended = 0
    n_homref = 0
    n_created = 0

    # Precompute HomRef pop stats merge inputs (constant for all HomRef variants)
    homref_new_ac = [0] * len(new_pop_ids)
    homref_new_het = [0] * len(new_pop_ids)
    homref_new_hom = [0] * len(new_pop_ids)

    # Precompute HomRef merged pop stats for each unique existing pop_ids combo.
    # For most datasets, all variants share the same pop_ids, so cache hit rate is ~100%.
    _homref_pop_cache: dict[tuple, dict] = {}

    prev_vid = None
    prev_pos = None

    conn = GraphManaConnection(**conn_params)
    conn.__enter__()
    try:
        last_pos = -1
        while True:
            rows = conn.execute_read(
                FETCH_VARIANTS_PAGED,
                {"chr": chrom, "lastPos": last_pos, "limit": BATCH_READ_SIZE},
            )
            if not rows:
                break

            for row in rows:
                vid = row["variantId"]
                pos = int(row["pos"])
                last_pos = pos

                old_gt = _neo4j_bytes(row["gt_packed"])
                old_phase = _neo4j_bytes(row["phase_packed"])
                old_ploidy = _neo4j_bytes(row["ploidy_packed"])

                if vid in new_variants:
                    # EXTEND: variant in both DB and new VCF
                    nv = new_variants[vid]
                    new_ids_remaining.discard(vid)

                    gt_packed = extend_gt_packed(old_gt, n_existing, nv["gt_types"])
                    phase_packed = extend_phase_packed(old_phase, n_existing, nv["phase_bits"])
                    ploidy_packed = extend_ploidy_packed(old_ploidy, n_existing, nv["ploidy_bits"])

                    old_pop_ids = _arr_str(row["pop_ids"])
                    merged = merge_pop_stats(
                        old_pop_ids,
                        _arr_int(row["ac"]), _arr_int(row["an"]),
                        _arr_int(row["het_count"]), _arr_int(row["hom_alt_count"]),
                        nv["pop_ids"],
                        _arr_int(nv["ac"]), _arr_int(nv["an"]),
                        _arr_int(nv["het_count"]), _arr_int(nv["hom_alt_count"]),
                    )
                    call_rate = merged["an_total"] / (2 * n_total) if n_total > 0 else 0.0
                    n_extended += 1
                else:
                    # HOMREF: variant only in DB — fast zero-byte extension
                    gt_packed = _extend_homref_fast(old_gt, n_existing, n_total, is_gt=True)
                    phase_packed = _extend_homref_fast(old_phase, n_existing, n_total, is_gt=False)
                    if old_ploidy:
                        ploidy_packed = _extend_homref_fast(old_ploidy, n_existing, n_total, is_gt=False)
                    else:
                        ploidy_packed = b""

                    # Cached pop stats merge (same for all HomRef variants with same pop_ids)
                    old_pop_ids = _arr_str(row["pop_ids"])
                    cache_key = tuple(old_pop_ids)
                    if cache_key not in _homref_pop_cache:
                        _homref_pop_cache[cache_key] = merge_pop_stats(
                            old_pop_ids,
                            _arr_int(row["ac"]), _arr_int(row["an"]),
                            _arr_int(row["het_count"]), _arr_int(row["hom_alt_count"]),
                            new_pop_ids, homref_new_ac, homref_new_an,
                            homref_new_het, homref_new_hom,
                        )
                    # Use cached merged pop_ids/af/het_exp, but per-variant ac/an/het/hom
                    cached = _homref_pop_cache[cache_key]
                    # For HomRef, new ac=0 so merged ac = old ac, but an changes.
                    # We must recompute per variant since old ac/an differ per variant.
                    old_ac = _arr_int(row["ac"])
                    old_an = _arr_int(row["an"])
                    old_het = _arr_int(row["het_count"])
                    old_hom = _arr_int(row["hom_alt_count"])
                    m_ac = []
                    m_an = []
                    m_af = []
                    m_het = []
                    m_hom = []
                    m_het_exp = []
                    old_map = {pid: i for i, pid in enumerate(old_pop_ids)}
                    new_map = {pid: i for i, pid in enumerate(new_pop_ids)}
                    for idx, pid in enumerate(cached["pop_ids"]):
                        ac_val = old_ac[old_map[pid]] if pid in old_map else 0
                        an_val = (old_an[old_map[pid]] if pid in old_map else 0) + \
                                 (homref_new_an[new_map[pid]] if pid in new_map else 0)
                        het_val = old_het[old_map[pid]] if pid in old_map else 0
                        hom_val = old_hom[old_map[pid]] if pid in old_map else 0
                        af_val = ac_val / an_val if an_val > 0 else 0.0
                        m_ac.append(ac_val)
                        m_an.append(an_val)
                        m_af.append(af_val)
                        m_het.append(het_val)
                        m_hom.append(hom_val)
                        m_het_exp.append(2.0 * af_val * (1.0 - af_val))
                    ac_total = sum(m_ac)
                    an_total = sum(m_an)
                    af_total = ac_total / an_total if an_total > 0 else 0.0
                    call_rate = an_total / (2 * n_total) if n_total > 0 else 0.0

                    merged = {
                        "pop_ids": cached["pop_ids"],
                        "ac": m_ac, "an": m_an, "af": m_af,
                        "het_count": m_het, "hom_alt_count": m_hom,
                        "het_exp": m_het_exp,
                        "ac_total": ac_total, "an_total": an_total, "af_total": af_total,
                    }
                    n_homref += 1

                _write_variant_csv_row(
                    w_var, vid, chrom, pos, row["ref"], row["alt"], row["variant_type"],
                    gt_packed, phase_packed, ploidy_packed,
                    merged["pop_ids"], merged["ac"], merged["an"], merged["af"],
                    merged["het_count"], merged["hom_alt_count"], merged["het_exp"],
                    merged["ac_total"], merged["an_total"], merged["af_total"], call_rate,
                    row.get("ancestral_allele"), row.get("is_polarized"),
                    row.get("qual"), row.get("filter"),
                    row.get("sv_type"), row.get("sv_len"), row.get("sv_end"),
                    row.get("multiallelic_site"), row.get("allele_index"),
                )

                if prev_vid is not None:
                    w_next.writerow([prev_vid, vid, "NEXT", pos - prev_pos])
                w_onchr.writerow([vid, chrom, "ON_CHROMOSOME"])
                prev_vid = vid
                prev_pos = pos

    finally:
        try:
            conn.__exit__(None, None, None)
        except Exception:
            pass

    # CREATE: new-only variants (in VCF but not in DB)
    existing_pids = sorted(existing_pop_map.keys())
    existing_an_homref = [2 * existing_pop_map[pid]["n_samples"] for pid in existing_pids]
    existing_zeros = [0] * len(existing_pids)

    for vid in sorted(new_ids_remaining, key=lambda v: new_variants[v]["pos"]):
        nv = new_variants[vid]
        gt_packed = pad_gt_for_new_variant(n_existing, nv["gt_types"])
        phase_packed = pad_phase_for_new_variant(n_existing, nv["phase_bits"])
        ploidy_packed = extend_ploidy_packed(None, n_existing, nv["ploidy_bits"])

        merged = merge_pop_stats(
            existing_pids, existing_zeros, existing_an_homref,
            existing_zeros, existing_zeros,
            nv["pop_ids"], _arr_int(nv["ac"]), _arr_int(nv["an"]),
            _arr_int(nv["het_count"]), _arr_int(nv["hom_alt_count"]),
        )
        call_rate = merged["an_total"] / (2 * n_total) if n_total > 0 else 0.0

        _write_variant_csv_row(
            w_var, vid, chrom, nv["pos"], nv["ref"], nv["alt"], nv["variant_type"],
            gt_packed, phase_packed, ploidy_packed,
            merged["pop_ids"], merged["ac"], merged["an"], merged["af"],
            merged["het_count"], merged["hom_alt_count"], merged["het_exp"],
            merged["ac_total"], merged["an_total"], merged["af_total"], call_rate,
            None, None, None, "PASS", None, None, None,
            nv.get("multiallelic_site"), nv.get("allele_index"),
        )
        if prev_vid is not None:
            w_next.writerow([prev_vid, vid, "NEXT", nv["pos"] - prev_pos])
        w_onchr.writerow([vid, chrom, "ON_CHROMOSOME"])
        prev_vid = vid
        prev_pos = nv["pos"]
        n_created += 1

    f_var.close()
    f_next.close()
    f_onchr.close()

    return {
        "chrom": chrom,
        "n_extended": n_extended,
        "n_homref": n_homref,
        "n_created": n_created,
        "var_csv": str(var_path),
        "next_csv": str(next_path),
        "onchr_csv": str(onchr_path),
    }


# Main function --------------------------------------------------------


def run_incremental_rebuild(
    conn,
    vcf_path: str | Path,
    panel_path: str | Path,
    output_csv_dir: str | Path,
    *,
    neo4j_home: str | Path,
    stratify_by: str = "superpopulation",
    include_filtered: bool = False,
    region: str | None = None,
    dataset_id: str = "",
    source_file: str = "",
    database: str = "neo4j",
    threads: int = 1,
    filter_config: dict | None = None,
) -> dict:
    """Run incremental import via export-extend-reimport.

    Supports multi-chromosome parallelism via ``threads`` parameter.
    """
    from graphmana.ingest.genotype_packer import unpack_genotypes, unpack_phase
    from graphmana.ingest.loader import run_load_csv
    from graphmana.ingest.vcf_parser import VCFParser

    output_csv_dir = Path(output_csv_dir)
    output_csv_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # 1. Read existing database state
    # ------------------------------------------------------------------
    logger.info("Reading existing database state...")

    existing_samples = conn.execute_read(FETCH_SAMPLES)
    n_existing = len(existing_samples)
    existing_sample_ids = {r["sampleId"] for r in existing_samples}
    logger.info("  %d existing samples", n_existing)

    existing_pops = conn.execute_read(FETCH_POPULATIONS)
    existing_pop_map = {r["populationId"]: dict(r) for r in existing_pops}
    logger.info("  %d existing populations", len(existing_pops))

    existing_chroms = conn.execute_read(FETCH_CHROMOSOMES)
    existing_chrom_map = {r["chr"]: r.get("length", 0) for r in existing_chroms}
    logger.info("  %d existing chromosomes", len(existing_chroms))

    # Build connection params for workers (pickle-safe, no driver objects)
    conn_params = {
        "uri": conn._uri if hasattr(conn, "_uri") else "bolt://localhost:7687",
        "user": conn._user if hasattr(conn, "_user") else "neo4j",
        "password": conn._password if hasattr(conn, "_password") else "graphmana",
        "database": conn._database if hasattr(conn, "_database") else database,
    }

    # ------------------------------------------------------------------
    # 2. Parse new VCF
    # ------------------------------------------------------------------
    logger.info("Parsing new VCF: %s", vcf_path)
    parser = VCFParser(
        vcf_path, panel_path,
        stratify_by=stratify_by,
        region=region,
        include_filtered=include_filtered,
    )
    pop_map_new = parser.pop_map
    n_new = len(pop_map_new.sample_ids)

    dupes = existing_sample_ids & set(pop_map_new.sample_ids)
    if dupes:
        raise ValueError(
            f"{len(dupes)} duplicate sample(s) found: {list(dupes)[:5]}... "
            "Use --on-duplicate skip or remove duplicates."
        )

    n_total = n_existing + n_new
    logger.info("  %d new samples, %d total after merge", n_new, n_total)

    reverse_remap = np.array([0, 1, 3, 2], dtype=np.int8)
    new_by_chr: dict[str, dict] = {}
    for rec in parser:
        packed_codes = unpack_genotypes(rec.gt_packed, n_new)
        cyvcf2_codes = reverse_remap[packed_codes]
        phase_bits = unpack_phase(rec.phase_packed, n_new)
        ploidy_bits = np.zeros(n_new, dtype=np.uint8)
        if rec.ploidy_packed:
            from graphmana.ingest.genotype_packer import unpack_ploidy
            ploidy_bits = unpack_ploidy(rec.ploidy_packed, n_new)

        new_by_chr.setdefault(rec.chr, {})[rec.id] = {
            "gt_types": cyvcf2_codes,
            "phase_bits": phase_bits,
            "ploidy_bits": ploidy_bits,
            "pop_ids": pop_map_new.pop_ids,
            "ac": list(rec.ac), "an": list(rec.an),
            "het_count": list(rec.het_count), "hom_alt_count": list(rec.hom_alt_count),
            "pos": rec.pos, "ref": rec.ref, "alt": rec.alt,
            "variant_type": rec.variant_type,
            "multiallelic_site": rec.multiallelic_site,
            "allele_index": rec.allele_index,
        }
    logger.info("  Parsed %d chromosomes from new VCF", len(new_by_chr))

    # Close the main connection — workers open their own
    try:
        conn.close()
    except Exception:
        pass

    # ------------------------------------------------------------------
    # 3. Process chromosomes (parallel or sequential)
    # ------------------------------------------------------------------
    all_chrom_ids = sorted(set(existing_chrom_map.keys()) | set(new_by_chr.keys()))
    new_pop_ids = pop_map_new.pop_ids
    homref_new_an = [2 * pop_map_new.n_samples_per_pop[pid] for pid in new_pop_ids]

    chrom_dir = output_csv_dir / "_chrom_parts"
    chrom_dir.mkdir(exist_ok=True)

    chrom_results = []

    if threads > 1 and len(all_chrom_ids) > 1:
        n_workers = min(threads, len(all_chrom_ids))
        logger.info("Processing %d chromosomes with %d workers", len(all_chrom_ids), n_workers)
        with ProcessPoolExecutor(max_workers=n_workers) as executor:
            futures = {}
            for chrom in all_chrom_ids:
                new_variants = new_by_chr.pop(chrom, {})
                fut = executor.submit(
                    _process_one_chromosome,
                    chrom, new_variants, conn_params,
                    n_existing, n_new, n_total,
                    new_pop_ids, homref_new_an, existing_pop_map,
                    chrom_dir,
                )
                futures[fut] = chrom

            for fut in as_completed(futures):
                chrom = futures[fut]
                result = fut.result()
                logger.info(
                    "  %s: %d extended, %d homref, %d new",
                    chrom, result["n_extended"], result["n_homref"], result["n_created"],
                )
                chrom_results.append(result)
    else:
        logger.info("Processing %d chromosomes sequentially", len(all_chrom_ids))
        for chrom in all_chrom_ids:
            new_variants = new_by_chr.pop(chrom, {})
            result = _process_one_chromosome(
                chrom, new_variants, conn_params,
                n_existing, n_new, n_total,
                new_pop_ids, homref_new_an, existing_pop_map,
                chrom_dir,
            )
            logger.info(
                "  %s: %d extended, %d homref, %d new",
                chrom, result["n_extended"], result["n_homref"], result["n_created"],
            )
            chrom_results.append(result)

    # ------------------------------------------------------------------
    # 4. Merge per-chromosome CSVs into final files
    # ------------------------------------------------------------------
    chrom_results.sort(key=lambda r: r["chrom"])
    n_extended = sum(r["n_extended"] for r in chrom_results)
    n_homref = sum(r["n_homref"] for r in chrom_results)
    n_created = sum(r["n_created"] for r in chrom_results)

    # Concatenate variant/next/onchr CSVs with a single header
    for csv_name, header in [
        ("variant_nodes.csv", VARIANT_HEADER),
        ("next_edges.csv", NEXT_HEADER),
        ("on_chromosome_edges.csv", ON_CHROMOSOME_HEADER),
    ]:
        prefix = csv_name.replace(".csv", "")
        with open(output_csv_dir / csv_name, "w") as fout:
            fout.write(",".join(header) + "\n")
            for r in chrom_results:
                part_key = f"{prefix}_{r['chrom']}.csv"
                part_path = chrom_dir / part_key
                if part_path.exists():
                    with open(part_path) as fin:
                        fout.write(fin.read())

    # ------------------------------------------------------------------
    # 5. Write sample, population, chromosome, in_population CSVs
    # ------------------------------------------------------------------
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    with open(output_csv_dir / "sample_nodes.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(SAMPLE_HEADER)
        for s in existing_samples:
            w.writerow([
                s["sampleId"], "Sample", s["population"],
                s["packed_index"], s.get("sex", 0),
                s.get("source_dataset", ""), s.get("source_file", ""),
                s.get("ingestion_date", now),
            ])
        for i, sid in enumerate(pop_map_new.sample_ids):
            pop = pop_map_new.sample_to_pop[sid]
            sex = pop_map_new.sample_to_sex.get(sid, 0)
            w.writerow([sid, "Sample", pop, n_existing + i, sex, dataset_id, source_file, now])

    merged_pops = {}
    for pid, pdata in existing_pop_map.items():
        merged_pops[pid] = {"n_samples": pdata["n_samples"]}
    for pid in pop_map_new.pop_ids:
        n = pop_map_new.n_samples_per_pop[pid]
        if pid in merged_pops:
            merged_pops[pid]["n_samples"] += n
        else:
            merged_pops[pid] = {"n_samples": n}

    with open(output_csv_dir / "population_nodes.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(POPULATION_HEADER)
        for pid in sorted(merged_pops.keys()):
            ns = merged_pops[pid]["n_samples"]
            an = _harmonic(2 * ns - 1) if ns > 0 else 0.0
            an2 = _harmonic2(2 * ns - 1) if ns > 0 else 0.0
            w.writerow([pid, "Population", pid, ns, _fmt_float(an), _fmt_float(an2)])

    all_chroms = set(existing_chrom_map.keys()) | {r["chrom"] for r in chrom_results}
    with open(output_csv_dir / "chromosome_nodes.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(CHROMOSOME_HEADER)
        for chrom in sorted(all_chroms):
            length = existing_chrom_map.get(chrom, CHR_LENGTHS.get(chrom, 0))
            w.writerow([chrom, "Chromosome", length])

    with open(output_csv_dir / "in_population_edges.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(IN_POPULATION_HEADER)
        for s in existing_samples:
            w.writerow([s["sampleId"], s["population"], "IN_POPULATION"])
        for sid in pop_map_new.sample_ids:
            pop = pop_map_new.sample_to_pop[sid]
            w.writerow([sid, pop, "IN_POPULATION"])

    logger.info(
        "CSV generation complete: %d extended, %d homref, %d new, %d total variants",
        n_extended, n_homref, n_created, n_extended + n_homref + n_created,
    )

    # ------------------------------------------------------------------
    # 6. Stop Neo4j + neo4j-admin import + restart
    # ------------------------------------------------------------------
    from graphmana.cluster.neo4j_lifecycle import start_neo4j, stop_neo4j

    logger.info("Stopping Neo4j for reimport...")
    stop_neo4j(neo4j_home)

    logger.info("Running neo4j-admin import...")
    run_load_csv(csv_dir=output_csv_dir, neo4j_home=neo4j_home, database=database, overwrite=True)

    logger.info("Restarting Neo4j...")
    start_neo4j(neo4j_home, wait=True)

    return {
        "n_variants_extended": n_extended,
        "n_variants_homref_extended": n_homref,
        "n_variants_created": n_created,
        "n_samples_created": n_new,
        "n_total_samples": n_total,
    }
