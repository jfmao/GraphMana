"""Parallel CSV generation by chromosome and within-chromosome regions.

Uses ProcessPoolExecutor to run one worker per chromosome (or per sub-region
when threads exceed chromosome count). Results are merged into final output
with bridge NEXT edges connecting adjacent regions.
"""

from __future__ import annotations

import csv
import logging
import shutil
import tempfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from graphmana.filtering.import_filters import ImportFilterConfig

logger = logging.getLogger(__name__)

# CSV files that are identical across workers (static nodes)
_STATIC_FILES = frozenset(
    {
        "sample_nodes.csv",
        "population_nodes.csv",
        "in_population_edges.csv",
        "vcf_header_nodes.csv",
    }
)

# CSV files that are per-chromosome and need concatenation
_STREAMING_FILES = [
    "variant_nodes.csv",
    "next_edges.csv",
    "on_chromosome_edges.csv",
]


def _get_vcf_chromosomes(vcf_path: str | Path) -> list[str]:
    """Extract chromosome names from VCF header (via cyvcf2)."""
    from cyvcf2 import VCF

    vcf = VCF(str(vcf_path), lazy=True)
    chroms = list(vcf.seqnames)
    vcf.close()
    return chroms


def _get_vcf_contig_lengths(vcf_path: str | Path) -> dict[str, int]:
    """Extract contig lengths from VCF header, falling back to GRCh38 defaults."""
    from cyvcf2 import VCF

    from graphmana.ingest.csv_emitter import CHR_LENGTHS

    vcf = VCF(str(vcf_path), lazy=True)
    seqnames = vcf.seqnames or []
    try:
        seqlens = vcf.seqlens or []
    except AttributeError:
        seqlens = []

    lengths = {}
    for name, length in zip(seqnames, seqlens):
        if length > 0:
            lengths[name] = length
    # Fill in missing from GRCh38 defaults
    for name in seqnames:
        if name not in lengths:
            lengths[name] = CHR_LENGTHS.get(name, 0)
    vcf.close()
    return lengths


def _has_tabix_index(vcf_path: str | Path) -> bool:
    """Check whether a .tbi or .csi index file exists for the VCF/BCF."""
    p = Path(vcf_path)
    return (p.with_suffix(p.suffix + ".tbi")).exists() or (
        p.with_suffix(p.suffix + ".csi")
    ).exists()


def _resolve_requested_contigs(
    contigs: list[str] | None,
    filter_config: "ImportFilterConfig | None",
) -> set[str] | None:
    """Merge contig restrictions from explicit param and filter_config.

    Returns a set of requested contig names, or None if unrestricted.
    """
    sets: list[set[str]] = []
    if contigs:
        sets.append(set(contigs))
    if filter_config is not None and filter_config.contigs:
        sets.append(set(filter_config.contigs))
    if not sets:
        return None
    # Intersection: both constraints must be satisfied
    result = sets[0]
    for s in sets[1:]:
        result = result & s
    return result


def _split_chromosome_regions(
    chrom: str, length: int, n_parts: int
) -> list[str]:
    """Split a chromosome into n_parts region strings for tabix seek.

    Returns list of region strings like "chr22:1-6352308".
    Regions are 1-based inclusive, non-overlapping, covering the full length.
    """
    if length <= 0 or n_parts <= 1:
        return [chrom]

    chunk = length // n_parts
    if chunk < 1000:
        # Too small to split meaningfully
        return [chrom]

    regions = []
    for i in range(n_parts):
        start = i * chunk + 1  # 1-based
        end = (i + 1) * chunk if i < n_parts - 1 else length
        regions.append(f"{chrom}:{start}-{end}")
    return regions


def _allocate_regions(
    chromosomes: list[str],
    contig_lengths: dict[str, int],
    threads: int,
) -> list[tuple[str, str]]:
    """Allocate threads across chromosomes, splitting large ones into regions.

    Returns list of (chromosome_name, region_string) tuples in order.
    When a chromosome gets multiple workers, the region_string is a sub-range.
    When it gets one worker, the region_string is just the chromosome name.
    """
    n_chroms = len(chromosomes)

    if n_chroms >= threads:
        # Original behavior: one worker per chromosome, no splitting
        return [(c, c) for c in chromosomes]

    # Distribute threads across chromosomes proportionally to length
    total_length = sum(contig_lengths.get(c, 0) for c in chromosomes)

    all_regions: list[tuple[str, str]] = []

    if total_length == 0:
        # No length info: distribute threads evenly
        workers_per = threads // n_chroms
        remainder = threads % n_chroms
        for i, chrom in enumerate(chromosomes):
            n = workers_per + (1 if i < remainder else 0)
            # Without length info, can't split by region
            all_regions.append((chrom, chrom))
            # Extra workers would be wasted — just one per chromosome
        return all_regions

    # Proportional allocation: each chrom gets workers ~ its fraction of total
    workers_allocated = 0
    for i, chrom in enumerate(chromosomes):
        length = contig_lengths.get(chrom, 0)
        if i == n_chroms - 1:
            # Last chromosome gets remaining workers
            n_workers = threads - workers_allocated
        else:
            n_workers = max(1, round(threads * length / total_length))
            n_workers = min(n_workers, threads - workers_allocated - (n_chroms - i - 1))
        workers_allocated += n_workers

        regions = _split_chromosome_regions(chrom, length, n_workers)
        for region in regions:
            all_regions.append((chrom, region))

    return all_regions


def _worker_prepare_csv_region(
    vcf_path: str,
    panel_path: str,
    output_dir: str,
    chromosome: str,
    kwargs: dict,
) -> dict:
    """Worker function: generate CSVs for a single chromosome/region using tabix.

    The chromosome parameter can be a full chromosome name (e.g., "chr22")
    or a region string (e.g., "chr22:1-25000000").
    """
    from graphmana.ingest.pipeline import run_prepare_csv

    # Remove region/threads from kwargs to avoid conflict with explicit args
    worker_kwargs = {k: v for k, v in kwargs.items() if k not in ("region", "threads")}

    return run_prepare_csv(
        vcf_path,
        panel_path,
        output_dir,
        region=chromosome,
        **worker_kwargs,
    )


def _worker_prepare_csv(
    vcf_path: str,
    panel_path: str,
    output_dir: str,
    chromosome: str,
    kwargs: dict,
) -> dict:
    """Worker function: generate CSVs for a single chromosome.

    Runs in a subprocess. Creates VCFParser with contigs=[chromosome],
    CSVEmitter writing to output_dir. Returns summary dict.
    """
    from graphmana.ingest.pipeline import run_prepare_csv

    worker_kwargs = {k: v for k, v in kwargs.items() if k not in ("region", "threads")}

    return run_prepare_csv(
        vcf_path,
        panel_path,
        output_dir,
        contigs=[chromosome],
        **worker_kwargs,
    )


def _merge_csv_dirs(
    chr_dirs: list[tuple[str, Path]],
    final_dir: Path,
    bridge_next_edges: list[tuple[str, str, int]] | None = None,
) -> dict:
    """Merge per-region CSV directories into final output.

    Args:
        chr_dirs: List of (region_key, dir_path) in region order.
        final_dir: Destination directory.
        bridge_next_edges: Optional list of (start_id, end_id, distance_bp)
            for NEXT edges bridging adjacent within-chromosome regions.

    Returns:
        Merge summary with combined counts.
    """
    final_dir.mkdir(parents=True, exist_ok=True)

    # Copy static files from the first worker directory
    if chr_dirs:
        first_dir = chr_dirs[0][1]
        for fname in _STATIC_FILES:
            src = first_dir / fname
            if src.exists():
                shutil.copy2(src, final_dir / fname)

    # Concatenate streaming files: header from first, data from all
    for fname in _STREAMING_FILES:
        dst = final_dir / fname
        header_written = False
        with open(dst, "w", newline="") as out_f:
            writer = csv.writer(out_f)
            for _key, chr_dir in chr_dirs:
                src = chr_dir / fname
                if not src.exists():
                    continue
                with open(src, "r") as in_f:
                    reader = csv.reader(in_f)
                    header = next(reader, None)
                    if header is not None and not header_written:
                        writer.writerow(header)
                        header_written = True
                    for row in reader:
                        writer.writerow(row)

            # Append bridge NEXT edges after all worker edges
            if fname == "next_edges.csv" and bridge_next_edges:
                if not header_written:
                    writer.writerow(
                        [":START_ID(Variant)", ":END_ID(Variant)", ":TYPE", "distance_bp:long"]
                    )
                for start_id, end_id, distance in bridge_next_edges:
                    writer.writerow([start_id, end_id, "NEXT", distance])

    # Merge chromosome_nodes.csv (union of all workers, deduplicate)
    all_chroms: dict[str, list[str]] = {}
    for _key, chr_dir in chr_dirs:
        src = chr_dir / "chromosome_nodes.csv"
        if not src.exists():
            continue
        with open(src, "r") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            for row in reader:
                if row and row[0] not in all_chroms:
                    all_chroms[row[0]] = row

    chrom_dst = final_dir / "chromosome_nodes.csv"
    with open(chrom_dst, "w", newline="") as f:
        writer = csv.writer(f)
        from graphmana.ingest.csv_emitter import CHROMOSOME_HEADER

        writer.writerow(CHROMOSOME_HEADER)
        for key in sorted(all_chroms):
            writer.writerow(all_chroms[key])

    return {"n_chromosome_files_merged": len(chr_dirs)}


def _build_bridge_next_edges(
    region_summaries: list[tuple[str, str, dict]],
) -> list[tuple[str, str, int]]:
    """Build NEXT edges that bridge adjacent within-chromosome regions.

    Args:
        region_summaries: List of (chromosome, region, summary_dict) in order.

    Returns:
        List of (start_variant_id, end_variant_id, distance_bp) tuples.
    """
    bridges = []
    for i in range(len(region_summaries) - 1):
        chrom_a, _region_a, summary_a = region_summaries[i]
        chrom_b, _region_b, summary_b = region_summaries[i + 1]

        # Only bridge regions on the same chromosome
        if chrom_a != chrom_b:
            continue

        last = summary_a.get("last_variant", {}).get(chrom_a)
        first = summary_b.get("first_variant", {}).get(chrom_b)

        if last is not None and first is not None:
            last_id, last_pos = last
            first_id, first_pos = first
            distance = first_pos - last_pos
            bridges.append((last_id, first_id, distance))
            logger.debug(
                "Bridge NEXT: %s (pos %d) → %s (pos %d), distance %d",
                last_id, last_pos, first_id, first_pos, distance,
            )

    if bridges:
        logger.info("Created %d bridge NEXT edges between regions", len(bridges))
    return bridges


def run_prepare_csv_parallel(
    vcf_path: str | Path,
    panel_path: str | Path,
    output_dir: str | Path,
    *,
    threads: int,
    contigs: list[str] | None = None,
    **kwargs,
) -> dict:
    """Parallel CSV generation by chromosome and within-chromosome regions.

    Strategy decision tree:
    1. If --filter-region specified → sequential (sub-chromosomal range)
    2. Resolve requested contigs (from contigs param + filter_config.contigs)
    3. Restrict chromosome list to only requested chromosomes
    4. If tabix index exists → parallel with region seek (fast)
       - If threads > chromosomes → split chromosomes into sub-regions
    5. No index → sequential single-pass fallback
    """
    from graphmana.ingest.pipeline import run_prepare_csv

    vcf_path = str(vcf_path)
    panel_path = str(panel_path)
    output_dir = Path(output_dir)

    filter_config = kwargs.get("filter_config")

    # 1. If region specified, use sequential — region already restricts scope
    region = kwargs.get("region")
    if region is not None:
        logger.info("Region filter specified (%s); using sequential mode", region)
        return run_prepare_csv(vcf_path, panel_path, output_dir, contigs=contigs, **kwargs)

    # 2. Get chromosomes from VCF header
    chromosomes = _get_vcf_chromosomes(vcf_path)
    if not chromosomes:
        logger.warning("No chromosomes found in VCF header; falling back to sequential")
        return run_prepare_csv(vcf_path, panel_path, output_dir, contigs=contigs, **kwargs)

    # 3. Resolve requested contigs and restrict chromosome list
    requested = _resolve_requested_contigs(contigs, filter_config)
    if requested is not None:
        chromosomes = [c for c in chromosomes if c in requested]
        if not chromosomes:
            logger.warning(
                "No VCF chromosomes match requested contigs %s; falling back to sequential",
                requested,
            )
            return run_prepare_csv(vcf_path, panel_path, output_dir, contigs=contigs, **kwargs)

    # 4. Choose worker strategy based on tabix index availability
    has_index = _has_tabix_index(vcf_path)
    if not has_index:
        # No index → each worker would do a full file scan. Use sequential.
        logger.info(
            "No tabix index for %s; using sequential single-pass mode "
            "(index with 'tabix -p vcf' for parallel speedup)",
            vcf_path,
        )
        return run_prepare_csv(vcf_path, panel_path, output_dir, contigs=contigs, **kwargs)

    # 5. Allocate regions (may split chromosomes if threads > n_chroms)
    contig_lengths = _get_vcf_contig_lengths(vcf_path)
    work_units = _allocate_regions(chromosomes, contig_lengths, threads)

    using_subregions = any(
        ":" in region for _chrom, region in work_units
    )
    if using_subregions:
        logger.info(
            "Parallel CSV generation (within-chromosome splitting): "
            "%d chromosomes → %d regions, %d workers",
            len(chromosomes),
            len(work_units),
            threads,
        )
    else:
        logger.info(
            "Parallel CSV generation (tabix region seek): %d chromosomes, %d workers",
            len(chromosomes),
            threads,
        )

    tmp_base = tempfile.mkdtemp(prefix="graphmana_parallel_")
    # (chrom, region, dir) — preserves order for bridge edge construction
    region_results: list[tuple[str, str, Path, dict]] = []

    try:
        futures = {}
        with ProcessPoolExecutor(max_workers=threads) as executor:
            for idx, (chrom, region_str) in enumerate(work_units):
                work_dir = Path(tmp_base) / f"region_{idx:04d}"
                work_dir.mkdir(parents=True, exist_ok=True)
                future = executor.submit(
                    _worker_prepare_csv_region,
                    vcf_path,
                    panel_path,
                    str(work_dir),
                    region_str,
                    kwargs,
                )
                futures[future] = (chrom, region_str, work_dir, idx)

            for future in as_completed(futures):
                chrom, region_str, work_dir, idx = futures[future]
                try:
                    summary = future.result()
                    region_results.append((chrom, region_str, work_dir, summary))
                    logger.info(
                        "Region %s: %d variants",
                        region_str,
                        summary.get("n_variants", 0),
                    )
                except Exception:
                    logger.exception("Worker failed for region %s", region_str)
                    raise

        # Sort by original submission order (preserves chromosome + region order)
        work_order = {(c, r): i for i, (c, r) in enumerate(work_units)}
        region_results.sort(key=lambda x: work_order.get((x[0], x[1]), 999))

        # Build bridge NEXT edges between adjacent same-chromosome regions
        bridge_summaries = [
            (chrom, region_str, summary)
            for chrom, region_str, _dir, summary in region_results
        ]
        bridge_edges = _build_bridge_next_edges(bridge_summaries)

        # Merge CSV dirs
        chr_dirs = [(region_str, work_dir) for _chrom, region_str, work_dir, _summary in region_results]
        _merge_csv_dirs(chr_dirs, output_dir, bridge_next_edges=bridge_edges)

        # Combine summaries
        summaries = [s for _, _, _, s in region_results]
        total_variants = sum(s.get("n_variants", 0) for s in summaries)
        total_next = sum(s.get("n_next_edges", 0) for s in summaries) + len(bridge_edges)
        all_chroms_seen = sorted(set().union(*(set(s.get("chromosomes", [])) for s in summaries)))

        # Get sample/population counts from first summary
        first = summaries[0] if summaries else {}
        combined = {
            "output_dir": str(output_dir),
            "n_variants": total_variants,
            "n_samples": first.get("n_samples", 0),
            "n_populations": first.get("n_populations", 0),
            "chromosomes": all_chroms_seen,
            "n_next_edges": total_next,
            "reference": first.get("reference", "unknown"),
            "parallel": True,
            "threads": threads,
            "strategy": "within_chromosome" if using_subregions else "tabix_region",
            "n_regions": len(work_units),
            "n_bridge_edges": len(bridge_edges),
        }
        logger.info(
            "Parallel CSV generation complete: %d variants across %d chromosomes "
            "(%d regions, %d bridge edges)",
            total_variants,
            len(all_chroms_seen),
            len(work_units),
            len(bridge_edges),
        )
        return combined

    finally:
        shutil.rmtree(tmp_base, ignore_errors=True)


# ---------------------------------------------------------------------------
# Multi-file parallel processing
# ---------------------------------------------------------------------------


def _worker_prepare_csv_file(
    vcf_path: str,
    panel_path: str,
    output_dir: str,
    kwargs: dict,
) -> dict:
    """Worker: process one VCF file end-to-end (single thread per file)."""
    from graphmana.ingest.pipeline import run_prepare_csv

    # Each file worker uses 1 thread (no within-chromosome splitting)
    worker_kwargs = {k: v for k, v in kwargs.items() if k != "threads"}

    return run_prepare_csv(
        vcf_path,
        panel_path,
        output_dir,
        threads=1,
        **worker_kwargs,
    )


def run_prepare_csv_multifile(
    vcf_paths: list[str],
    panel_path: str | Path,
    output_dir: str | Path,
    *,
    threads: int = 4,
    **kwargs,
) -> dict:
    """Process multiple VCF files in parallel, then merge CSVs.

    Each VCF file is processed by one worker (single thread per file).
    Up to `threads` files are processed concurrently.
    Results are merged into a single set of CSV files in output_dir.

    This is optimal for per-chromosome VCFs (e.g., 22 files from 1KGP),
    where cross-file parallelism is more efficient than within-chromosome
    splitting.
    """
    from graphmana.ingest.pipeline import run_prepare_csv

    output_dir = Path(output_dir)
    panel_path = str(panel_path)
    n_files = len(vcf_paths)

    if n_files == 0:
        raise ValueError("No VCF files provided")
    if n_files == 1:
        return run_prepare_csv(
            vcf_paths[0], panel_path, str(output_dir), threads=threads, **kwargs,
        )

    max_workers = min(threads, n_files)
    logger.info(
        "Multi-file parallel CSV generation: %d files, %d workers",
        n_files,
        max_workers,
    )

    tmp_base = tempfile.mkdtemp(prefix="graphmana_multifile_")
    file_results: list[tuple[int, Path, dict]] = []  # (index, dir, summary)

    try:
        futures = {}
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            for i, vcf_path in enumerate(vcf_paths):
                file_dir = Path(tmp_base) / f"file_{i:04d}"
                file_dir.mkdir(parents=True, exist_ok=True)
                future = executor.submit(
                    _worker_prepare_csv_file,
                    str(vcf_path),
                    panel_path,
                    str(file_dir),
                    kwargs,
                )
                futures[future] = (i, file_dir, Path(vcf_path).name)

            for future in as_completed(futures):
                idx, file_dir, fname = futures[future]
                try:
                    summary = future.result()
                    file_results.append((idx, file_dir, summary))
                    logger.info(
                        "File %s: %d variants, %s",
                        fname,
                        summary.get("n_variants", 0),
                        summary.get("chromosomes", []),
                    )
                except Exception:
                    logger.exception("Worker failed for file %s", fname)
                    raise

        # Sort by original file order
        file_results.sort(key=lambda x: x[0])

        # Merge all per-file CSV dirs
        chr_dirs = [
            (f"file_{idx:04d}", fdir)
            for idx, fdir, _summary in file_results
        ]
        output_dir.mkdir(parents=True, exist_ok=True)
        _merge_csv_dirs(chr_dirs, output_dir)

        # Combine summaries
        summaries = [s for _, _, s in file_results]
        total_variants = sum(s.get("n_variants", 0) for s in summaries)
        total_next = sum(s.get("n_next_edges", 0) for s in summaries)
        all_chroms = sorted(
            set().union(*(set(s.get("chromosomes", [])) for s in summaries))
        )
        first = summaries[0]

        combined = {
            "output_dir": str(output_dir),
            "n_variants": total_variants,
            "n_samples": first.get("n_samples", 0),
            "n_populations": first.get("n_populations", 0),
            "chromosomes": all_chroms,
            "n_next_edges": total_next,
            "reference": first.get("reference", "unknown"),
            "parallel": True,
            "threads": threads,
            "strategy": "multifile",
            "n_files": n_files,
        }
        logger.info(
            "Multi-file CSV generation complete: %d variants across %d "
            "chromosomes from %d files",
            total_variants,
            len(all_chroms),
            n_files,
        )
        return combined

    finally:
        shutil.rmtree(tmp_base, ignore_errors=True)
