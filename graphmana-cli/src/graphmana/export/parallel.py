"""Parallel export by chromosome.

Uses ProcessPoolExecutor to export one chromosome per worker to a temp file,
then concatenates results in chromosome order.
"""

from __future__ import annotations

import logging
import shutil
import tempfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

logger = logging.getLogger(__name__)


def _worker_export_chromosome(
    exporter_cls_name: str,
    exporter_module: str,
    conn_args: tuple[str, str, str, str | None],
    filter_config_dict: dict | None,
    chromosome: str,
    output_path: str,
    export_kwargs: dict,
    recalculate_af: bool = False,
) -> dict:
    """Worker: export a single chromosome to a temp file.

    Creates a new Neo4j connection and exporter instance per worker.
    Returns summary dict with temp file path and variant count.
    """
    import importlib

    from graphmana.db.connection import GraphManaConnection
    from graphmana.filtering.export_filters import ExportFilterConfig

    # Reconstruct filter config with single chromosome
    fc_dict = dict(filter_config_dict) if filter_config_dict else {}
    fc_dict["chromosomes"] = [chromosome]
    filter_config = ExportFilterConfig(**fc_dict)

    # Import the exporter class
    mod = importlib.import_module(exporter_module)
    exporter_cls = getattr(mod, exporter_cls_name)

    uri, user, password, database = conn_args
    with GraphManaConnection(uri, user, password, database=database) as conn:
        exporter = exporter_cls(
            conn,
            filter_config=filter_config,
            recalculate_af=recalculate_af,
        )
        summary = exporter.export(Path(output_path), **export_kwargs)

    return summary


def _get_filter_config_dict(filter_config) -> dict | None:
    """Serialize ExportFilterConfig to a dict for pickling across processes."""
    if filter_config is None:
        return None
    return {
        "populations": filter_config.populations,
        "chromosomes": None,  # overridden per worker
        "region": filter_config.region,
        "variant_types": filter_config.variant_types,
        "maf_min": filter_config.maf_min,
        "maf_max": filter_config.maf_max,
        "min_call_rate": filter_config.min_call_rate,
        "cohort": filter_config.cohort,
        "sample_ids": filter_config.sample_ids,
        # Annotation-based filters
        "consequences": filter_config.consequences,
        "impacts": filter_config.impacts,
        "genes": filter_config.genes,
        "cadd_min": filter_config.cadd_min,
        "cadd_max": filter_config.cadd_max,
        "annotation_version": filter_config.annotation_version,
        "sv_types": filter_config.sv_types,
        "liftover_status": filter_config.liftover_status,
    }


def run_export_parallel(
    exporter_cls,
    conn,
    *,
    threads: int,
    output: Path,
    filter_config,
    target_chroms: list[str],
    export_kwargs: dict,
    header_writer=None,
    merge_func=None,
    recalculate_af: bool = False,
) -> dict:
    """Parallel export by chromosome.

    Args:
        exporter_cls: The exporter class (e.g., VCFExporter).
        conn: GraphManaConnection (used to extract connection args).
        threads: Number of parallel workers.
        output: Final output path.
        filter_config: ExportFilterConfig instance.
        target_chroms: Ordered list of chromosomes to export.
        export_kwargs: Kwargs passed to exporter.export().
        header_writer: Callable(output_path, conn) that writes the
            file header/preamble. Called once before workers start.
        merge_func: Callable(output_path, chr_tmp_files) that merges
            per-chromosome temp files into the final output. If None,
            simple text concatenation is used.
        recalculate_af: Whether to recalculate allele frequencies from
            genotypes instead of using stored population arrays.

    Returns:
        Merged summary dict.
    """
    conn_args = (conn._uri, conn._user, conn._password, conn._database)
    fc_dict = _get_filter_config_dict(filter_config)

    exporter_module = exporter_cls.__module__
    exporter_cls_name = exporter_cls.__name__

    tmp_dir = tempfile.mkdtemp(prefix="graphmana_export_")
    chr_files: list[tuple[str, Path]] = []
    summaries: list[dict] = []

    try:
        # Write header if provided
        if header_writer is not None:
            header_writer(output, conn)

        futures = {}
        with ProcessPoolExecutor(max_workers=threads) as executor:
            for chrom in target_chroms:
                tmp_path = Path(tmp_dir) / f"{chrom}_export.tmp"
                future = executor.submit(
                    _worker_export_chromosome,
                    exporter_cls_name,
                    exporter_module,
                    conn_args,
                    fc_dict,
                    chrom,
                    str(tmp_path),
                    export_kwargs,
                    recalculate_af,
                )
                futures[future] = (chrom, tmp_path)

            for future in as_completed(futures):
                chrom, tmp_path = futures[future]
                try:
                    summary = future.result()
                    summaries.append(summary)
                    chr_files.append((chrom, tmp_path))
                    logger.info(
                        "Exported chromosome %s: %d variants",
                        chrom,
                        summary.get("n_variants", 0),
                    )
                except Exception:
                    logger.exception("Export worker failed for chromosome %s", chrom)
                    raise

        # Sort by original chromosome order
        chrom_order = {c: i for i, c in enumerate(target_chroms)}
        chr_files.sort(key=lambda x: chrom_order.get(x[0], 999))

        # Merge
        if merge_func is not None:
            merge_func(output, chr_files)
        else:
            _default_text_merge(output, chr_files, has_header=header_writer is not None)

        # Combine summaries
        total_variants = sum(s.get("n_variants", 0) for s in summaries)
        total_skipped = sum(s.get("n_skipped", 0) for s in summaries)

        first = summaries[0] if summaries else {}
        combined = {
            "n_variants": total_variants,
            "n_samples": first.get("n_samples", 0),
            "chromosomes": target_chroms,
            "format": first.get("format", "unknown"),
            "parallel": True,
            "threads": threads,
        }
        if total_skipped > 0:
            combined["n_skipped"] = total_skipped

        return combined

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def _default_text_merge(
    output: Path, chr_files: list[tuple[str, Path]], *, has_header: bool
) -> None:
    """Concatenate per-chromosome text files, skipping headers from workers.

    If has_header is True, the main output already has a header written
    by header_writer, so we append data lines and skip each worker's header.
    If has_header is False, we keep the first worker's header and skip the rest.
    """
    mode = "a" if has_header else "w"
    first_file = True
    with open(output, mode) as out_f:
        for _chrom, tmp_path in chr_files:
            if not tmp_path.exists():
                continue
            with open(tmp_path, "r") as in_f:
                for i, line in enumerate(in_f):
                    # Skip header line from worker output
                    # Worker output always has a header as first line
                    if i == 0:
                        if not has_header and first_file:
                            # Keep header from first worker
                            out_f.write(line)
                        # Otherwise skip header
                        continue
                    out_f.write(line)
            first_file = False
