"""Import pipeline orchestration: prepare-csv, ingest, and incremental workflows.

Connects VCFParser -> ImportFilterChain -> CSVEmitter -> VEPParser -> Loader.
For incremental mode: VCFParser -> IncrementalIngester (Cypher on live Neo4j).
"""

from __future__ import annotations

import logging
import tempfile
from pathlib import Path

from graphmana.config import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_DATABASE,
    DEFAULT_NEO4J_PASSWORD,
    DEFAULT_NEO4J_URI,
    DEFAULT_NEO4J_USER,
)
from graphmana.db.queries import FETCH_EXISTING_SAMPLE_IDS, FETCH_MAX_PACKED_INDEX
from graphmana.filtering.import_filters import ImportFilterChain, ImportFilterConfig
from graphmana.ingest.csv_emitter import CSVEmitter
from graphmana.ingest.loader import run_load_csv
from graphmana.ingest.vcf_parser import VCFParser
from graphmana.ingest.vep_parser import VEPParser

logger = logging.getLogger(__name__)


def run_prepare_csv(
    vcf_path: str | Path,
    panel_path: str | Path,
    output_dir: str | Path,
    *,
    stratify_by: str = "superpopulation",
    reference: str = "unknown",
    ancestral_fasta: str | Path | None = None,
    chr_style: str = "auto",
    chr_map_path: str | Path | None = None,
    chunk_size: int = DEFAULT_BATCH_SIZE,
    filter_config: ImportFilterConfig | None = None,
    vep_vcf_path: str | Path | None = None,
    annotation_source: str = "VEP",
    annotation_version: str = "unknown",
    dataset_id: str = "",
    include_filtered: bool = False,
    contigs: list[str] | None = None,
    region: str | None = None,
    ploidy: str = "auto",
    verbose: bool = False,
    threads: int = 1,
) -> dict:
    """Generate CSV files from VCF (no Neo4j needed).

    Returns a summary dict with counts and paths.
    """
    if threads > 1:
        from graphmana.ingest.parallel import run_prepare_csv_parallel

        return run_prepare_csv_parallel(
            vcf_path,
            panel_path,
            output_dir,
            threads=threads,
            contigs=contigs,
            stratify_by=stratify_by,
            reference=reference,
            ancestral_fasta=ancestral_fasta,
            chr_style=chr_style,
            chr_map_path=chr_map_path,
            chunk_size=chunk_size,
            filter_config=filter_config,
            vep_vcf_path=vep_vcf_path,
            annotation_source=annotation_source,
            annotation_version=annotation_version,
            dataset_id=dataset_id,
            include_filtered=include_filtered,
            region=region,
            ploidy=ploidy,
            verbose=verbose,
        )

    output_dir = Path(output_dir)

    # 1. Create VCFParser
    parser = VCFParser(
        vcf_path,
        panel_path,
        stratify_by=stratify_by,
        region=region,
        include_filtered=include_filtered,
        ancestral_fasta=ancestral_fasta,
        ploidy=ploidy,
        contigs=contigs,
        chr_style=chr_style,
        chr_map_path=chr_map_path,
    )

    # 2. Optionally create ImportFilterChain
    filter_chain = None
    if filter_config is not None:
        filter_chain = ImportFilterChain(filter_config)

    # 3. Run CSVEmitter
    source_file = str(vcf_path)
    emitter = CSVEmitter.run(
        parser,
        output_dir,
        chunk_size=chunk_size,
        filter_chain=filter_chain,
        dataset_id=dataset_id,
        source_file=source_file,
    )

    # 3b. Write VCFHeader node CSV (if raw header available)
    if parser.raw_header:
        emitter.write_vcf_header_node(parser.raw_header)

    summary: dict = {
        "output_dir": str(output_dir),
        "n_variants": emitter.n_variants,
        "n_samples": len(parser.pop_map.sample_ids),
        "n_populations": len(parser.pop_map.pop_ids),
        "chromosomes": sorted(emitter.chromosomes_seen),
        "n_next_edges": emitter.n_next,
        "reference": reference,
        "first_variant": dict(emitter.first_variant),
        "last_variant": dict(emitter.last_variant),
    }

    # 4. Filter summary
    if filter_chain is not None:
        summary["filter_summary"] = filter_chain.summary()

    # 5. Optionally run VEPParser
    if vep_vcf_path is not None:
        vep = VEPParser(
            vep_vcf_path,
            output_dir,
            variant_csv_path=output_dir / "variant_nodes.csv",
            chr_reconciler=parser.chr_reconciler,
            annotation_source=annotation_source,
            annotation_version=annotation_version,
        )
        vep.run()
        summary["vep"] = {
            "format": vep.ann_format,
            "n_genes": vep.n_genes,
            "n_edges": vep.n_edges,
            "n_variants_matched": vep.n_variants_matched,
        }

    logger.info(
        "CSV generation complete: %d variants, %d samples, %d populations",
        summary["n_variants"],
        summary["n_samples"],
        summary["n_populations"],
    )
    return summary


def _detect_import_mode(
    neo4j_uri: str, neo4j_user: str, neo4j_password: str, *, database: str | None = None
) -> str:
    """Auto-detect import mode by checking if Variant nodes exist.

    Returns:
        'initial' if database is empty, 'incremental' if variants exist.
    """
    from graphmana.db.connection import GraphManaConnection

    try:
        with GraphManaConnection(neo4j_uri, neo4j_user, neo4j_password, database=database) as conn:
            result = conn.execute_read("MATCH (v:Variant) RETURN count(v) AS c")
            record = result.single()
            n = record["c"] if record else 0
            return "incremental" if n > 0 else "initial"
    except Exception:
        return "initial"


def run_ingest(
    vcf_path: str | Path,
    panel_path: str | Path,
    *,
    output_csv_dir: str | Path | None = None,
    neo4j_home: str | Path | None = None,
    database: str = DEFAULT_DATABASE,
    mode: str = "auto",
    on_duplicate: str = "error",
    assume_homref_on_missing: bool = False,
    # prepare-csv options
    stratify_by: str = "superpopulation",
    reference: str = "unknown",
    ancestral_fasta: str | Path | None = None,
    chr_style: str = "auto",
    chr_map_path: str | Path | None = None,
    chunk_size: int = DEFAULT_BATCH_SIZE,
    filter_config: ImportFilterConfig | None = None,
    vep_vcf_path: str | Path | None = None,
    annotation_source: str = "VEP",
    annotation_version: str = "unknown",
    dataset_id: str = "",
    include_filtered: bool = False,
    contigs: list[str] | None = None,
    region: str | None = None,
    ploidy: str = "auto",
    verbose: bool = False,
    threads: int = 1,
    # load-csv options
    neo4j_uri: str | None = None,
    neo4j_user: str = DEFAULT_NEO4J_USER,
    neo4j_password: str = DEFAULT_NEO4J_PASSWORD,
) -> dict:
    """Combined: generate CSVs + load into Neo4j, or incremental import.

    When mode='auto', checks Neo4j for existing variants to decide.
    When mode='incremental', uses Cypher on a running Neo4j instance.
    When mode='initial', uses neo4j-admin import full (destructive).

    Returns combined summary stats.
    """
    effective_uri = neo4j_uri or DEFAULT_NEO4J_URI

    # Auto-detect mode
    if mode == "auto":
        if neo4j_uri is not None:
            mode = _detect_import_mode(effective_uri, neo4j_user, neo4j_password, database=database)
            logger.info("Auto-detected import mode: %s", mode)
        else:
            mode = "initial"

    # Route to incremental if needed
    if mode == "incremental":
        # Use rebuild strategy (export-extend-reimport) when neo4j_home is
        # provided — this is 5-10x faster than the Cypher transaction approach
        # because it bypasses Neo4j's transaction engine entirely.
        if neo4j_home is not None:
            rebuild_csv_dir = Path(output_csv_dir) if output_csv_dir else Path(
                tempfile.mkdtemp(prefix="graphmana_incr_rebuild_")
            )

            # Check for CSV checkpoint: if the output dir already contains
            # variant_nodes.csv from a prior prepare-csv, use the fast
            # CSV-to-CSV path (no Neo4j reads needed).
            existing_checkpoint = rebuild_csv_dir / "variant_nodes.csv"
            if existing_checkpoint.exists():
                from graphmana.ingest.incremental_rebuild import run_incremental_from_csv

                logger.info(
                    "CSV checkpoint found at %s — using CSV-to-CSV fast path",
                    rebuild_csv_dir,
                )
                # Count existing samples from the checkpoint
                with open(rebuild_csv_dir / "sample_nodes.csv") as _sf:
                    _n_existing = sum(1 for _ in _sf) - 1  # minus header

                merged_csv_dir = Path(
                    tempfile.mkdtemp(prefix="graphmana_incr_merged_")
                )
                summary = run_incremental_from_csv(
                    existing_csv_dir=rebuild_csv_dir,
                    vcf_path=vcf_path,
                    panel_path=panel_path,
                    output_csv_dir=merged_csv_dir,
                    neo4j_home=neo4j_home,
                    n_existing=_n_existing,
                    stratify_by=stratify_by,
                    include_filtered=include_filtered,
                    region=region,
                    dataset_id=dataset_id,
                    source_file=str(vcf_path),
                    database=database or DEFAULT_DATABASE,
                    threads=threads,
                )

                # Update checkpoint: replace old CSVs with merged ones
                import shutil
                for csv_file in merged_csv_dir.glob("*.csv"):
                    shutil.move(str(csv_file), str(rebuild_csv_dir / csv_file.name))
                shutil.rmtree(merged_csv_dir, ignore_errors=True)

            else:
                # No checkpoint — use Neo4j-based rebuild
                from graphmana.db.connection import GraphManaConnection
                from graphmana.ingest.incremental_rebuild import run_incremental_rebuild

                conn = GraphManaConnection(
                    effective_uri, neo4j_user, neo4j_password, database=database
                )
                conn.__enter__()
                try:
                    summary = run_incremental_rebuild(
                        conn,
                        vcf_path,
                        panel_path,
                        rebuild_csv_dir,
                        neo4j_home=neo4j_home,
                        stratify_by=stratify_by,
                        include_filtered=include_filtered,
                        region=region,
                        dataset_id=dataset_id,
                        source_file=str(vcf_path),
                        database=database or DEFAULT_DATABASE,
                        threads=threads,
                    )
                except Exception:
                    try:
                        conn.__exit__(None, None, None)
                    except Exception:
                        pass
                    raise

            # Apply post-import indexes
            from graphmana.ingest.loader import apply_post_import_indexes

            apply_post_import_indexes(
                effective_uri,
                neo4j_user,
                neo4j_password,
                database=database,
                reference_genome=reference,
                chr_naming_style=chr_style,
            )

            summary["mode"] = "incremental_rebuild"
            return summary

        # Fallback: Cypher transaction approach (when neo4j_home not available)
        return run_incremental(
            vcf_path,
            panel_path,
            neo4j_uri=effective_uri,
            neo4j_user=neo4j_user,
            neo4j_password=neo4j_password,
            database=database,
            stratify_by=stratify_by,
            reference=reference,
            ancestral_fasta=ancestral_fasta,
            chr_style=chr_style,
            chr_map_path=chr_map_path,
            chunk_size=chunk_size,
            filter_config=filter_config,
            dataset_id=dataset_id,
            include_filtered=include_filtered,
            contigs=contigs,
            region=region,
            ploidy=ploidy,
            on_duplicate=on_duplicate,
            assume_homref_on_missing=assume_homref_on_missing,
            verbose=verbose,
        )

    # Initial mode: CSV generation + neo4j-admin import full
    if neo4j_home is None:
        raise ValueError("--neo4j-home is required for initial import mode.")

    # Determine output directory
    if output_csv_dir is not None:
        csv_dir = Path(output_csv_dir)
        cleanup = False
    else:
        tmp = tempfile.mkdtemp(prefix="graphmana_csv_")
        csv_dir = Path(tmp)
        cleanup = True

    try:
        # Step 1: Generate CSVs
        summary = run_prepare_csv(
            vcf_path,
            panel_path,
            csv_dir,
            stratify_by=stratify_by,
            reference=reference,
            ancestral_fasta=ancestral_fasta,
            chr_style=chr_style,
            chr_map_path=chr_map_path,
            chunk_size=chunk_size,
            filter_config=filter_config,
            vep_vcf_path=vep_vcf_path,
            annotation_source=annotation_source,
            annotation_version=annotation_version,
            dataset_id=dataset_id,
            include_filtered=include_filtered,
            contigs=contigs,
            region=region,
            ploidy=ploidy,
            verbose=verbose,
            threads=threads,
        )

        # Step 2: Load CSVs into Neo4j
        run_load_csv(
            csv_dir,
            neo4j_home=neo4j_home,
            database=database,
        )
        summary["database"] = database
        summary["loaded"] = True
        summary["mode"] = "initial"

        # Step 3: Apply post-import indexes and record provenance (if URI provided)
        if neo4j_uri is not None:
            from graphmana.ingest.loader import apply_post_import_indexes

            apply_post_import_indexes(
                neo4j_uri,
                neo4j_user,
                neo4j_password,
                database=database,
                reference_genome=reference,
                chr_naming_style=chr_style,
            )
            summary["indexes_created"] = True

            # Record provenance
            try:
                from graphmana.db.connection import GraphManaConnection as _GConn
                from graphmana.provenance.manager import ProvenanceManager

                with _GConn(neo4j_uri, neo4j_user, neo4j_password, database=database) as prov_conn:
                    prov = ProvenanceManager(prov_conn)
                    prov.record_ingestion(
                        source_file=str(vcf_path),
                        dataset_id=dataset_id or str(vcf_path),
                        mode="initial",
                        n_samples=summary["n_samples"],
                        n_variants=summary["n_variants"],
                        filters_applied=str(summary.get("filter_summary", {})),
                        fidelity="default",
                        reference_genome=reference,
                    )
            except Exception:
                logger.warning("Failed to record provenance log", exc_info=True)

        return summary

    finally:
        if cleanup:
            import shutil

            shutil.rmtree(csv_dir, ignore_errors=True)


def run_incremental(
    vcf_path: str | Path,
    panel_path: str | Path,
    *,
    neo4j_uri: str = DEFAULT_NEO4J_URI,
    neo4j_user: str = DEFAULT_NEO4J_USER,
    neo4j_password: str = DEFAULT_NEO4J_PASSWORD,
    database: str | None = None,
    stratify_by: str = "superpopulation",
    reference: str = "unknown",
    ancestral_fasta: str | Path | None = None,
    chr_style: str = "auto",
    chr_map_path: str | Path | None = None,
    chunk_size: int = DEFAULT_BATCH_SIZE,
    filter_config: ImportFilterConfig | None = None,
    dataset_id: str = "",
    include_filtered: bool = False,
    contigs: list[str] | None = None,
    region: str | None = None,
    ploidy: str = "auto",
    on_duplicate: str = "error",
    assume_homref_on_missing: bool = False,
    verbose: bool = False,
) -> dict:
    """Incremental sample addition to an existing database via Cypher.

    Requires a running Neo4j instance. Extends packed arrays on existing
    Variant nodes and creates new Sample/Population nodes.

    Args:
        vcf_path: path to VCF/BCF file with new samples.
        panel_path: population panel/PED file for new samples.
        neo4j_uri: Bolt URI for running Neo4j.
        neo4j_user: Neo4j username.
        neo4j_password: Neo4j password.
        stratify_by: population stratification level.
        reference: reference genome identifier.
        ancestral_fasta: optional ancestral allele FASTA.
        chr_style: chromosome naming style.
        chr_map_path: optional chromosome name mapping file.
        chunk_size: variants per Cypher batch.
        filter_config: optional import filter configuration.
        dataset_id: dataset identifier for provenance.
        include_filtered: include FILTER!=PASS variants.
        contigs: optional contig whitelist.
        region: optional genomic region.
        ploidy: ploidy mode ('auto' or 'diploid').
        on_duplicate: how to handle duplicate samples ('error' or 'skip').
        verbose: enable verbose logging.

    Returns:
        Summary dict with counts.

    Raises:
        ValueError: if duplicate samples found and on_duplicate='error'.
    """
    from graphmana.db.connection import GraphManaConnection
    from graphmana.ingest.incremental import IncrementalIngester
    from graphmana.ingest.population_map import build_pop_map

    # 1. Parse new VCF to get sample list
    parser = VCFParser(
        vcf_path,
        panel_path,
        stratify_by=stratify_by,
        region=region,
        include_filtered=include_filtered,
        ancestral_fasta=ancestral_fasta,
        ploidy=ploidy,
        contigs=contigs,
        chr_style=chr_style,
        chr_map_path=chr_map_path,
    )

    # 2. Optional filter chain
    filter_chain = None
    if filter_config is not None:
        filter_chain = ImportFilterChain(filter_config)

    source_file = str(vcf_path)

    # 3. Connect to Neo4j and validate state
    with GraphManaConnection(neo4j_uri, neo4j_user, neo4j_password, database=database) as conn:
        # Fetch existing sample IDs (excludes soft-deleted)
        result = conn.execute_read(FETCH_EXISTING_SAMPLE_IDS)
        record = result.single()
        existing_ids = set(record["ids"]) if record and record["ids"] else set()

        # Check for duplicates
        new_sample_ids = set(parser.pop_map.sample_ids)
        duplicates = new_sample_ids & existing_ids
        if duplicates:
            if on_duplicate == "error":
                raise ValueError(
                    f"Duplicate samples found ({len(duplicates)}): "
                    f"{sorted(duplicates)[:5]}{'...' if len(duplicates) > 5 else ''}. "
                    f"Use --on-duplicate=skip to skip these."
                )
            elif on_duplicate == "skip":
                logger.warning(
                    "Skipping %d duplicate samples: %s",
                    len(duplicates),
                    sorted(duplicates)[:5],
                )
                # Rebuild pop_map without duplicates
                from cyvcf2 import VCF as _VCF

                vcf_tmp = _VCF(str(vcf_path), lazy=True)
                vcf_samples = list(vcf_tmp.samples)
                vcf_tmp.close()

                sample_to_pop_orig = parser.pop_map.sample_to_pop
                sample_to_sex_orig = parser.pop_map.sample_to_sex

                # Filter out duplicates
                filtered_s2p = {s: p for s, p in sample_to_pop_orig.items() if s not in duplicates}
                filtered_s2s = {s: x for s, x in sample_to_sex_orig.items() if s not in duplicates}

                if not filtered_s2p:
                    logger.warning("All new samples are duplicates. Nothing to do.")
                    return {
                        "n_variants_extended": 0,
                        "n_variants_homref_extended": 0,
                        "n_variants_created": 0,
                        "n_samples_created": 0,
                        "n_populations_created": 0,
                        "n_total_samples": len(existing_ids),
                        "skipped_duplicates": len(duplicates),
                    }

                pop_map_new = build_pop_map(vcf_samples, filtered_s2p, filtered_s2s)

                # Re-create parser with filtered pop_map samples
                # The parser already exists; we just replace its pop_map
                parser._pop_map = pop_map_new
        else:
            pop_map_new = parser.pop_map

        # Get packed_index offset (excludes soft-deleted)
        result = conn.execute_read(FETCH_MAX_PACKED_INDEX)
        record = result.single()
        max_idx = record["max_idx"] if record else None
        offset = (max_idx + 1) if max_idx is not None else 0

        # Get existing population IDs
        result = conn.execute_read("MATCH (p:Population) RETURN collect(p.populationId) AS ids")
        record = result.single()
        existing_pop_ids = sorted(record["ids"]) if record and record["ids"] else []

        n_existing = len(existing_ids)
        n_new = len(pop_map_new.sample_ids)
        n_total = n_existing + n_new

        logger.info(
            "Incremental import: %d existing samples + %d new = %d total",
            n_existing,
            n_new,
            n_total,
        )

        # 4. Run IncrementalIngester
        ingester = IncrementalIngester(
            conn=conn,
            pop_map_new=pop_map_new,
            n_existing=n_existing,
            existing_sample_ids=existing_ids,
            existing_pop_ids=existing_pop_ids,
            packed_index_offset=offset,
            dataset_id=dataset_id,
            source_file=source_file,
            n_total_samples=n_total,
            assume_homref_on_missing=assume_homref_on_missing,
        )
        if assume_homref_on_missing:
            logger.warning(
                "Legacy --assume-homref-on-missing is enabled: existing "
                "samples will be coded as HomRef at variants introduced by "
                "this batch, reproducing the v1.0 semantics. Only use this "
                "with fixed-site-list workflows."
            )
        summary = ingester.run(parser, chunk_size=chunk_size, filter_chain=filter_chain)

        if duplicates and on_duplicate == "skip":
            summary["skipped_duplicates"] = len(duplicates)

        # Record provenance
        try:
            from graphmana.provenance.manager import ProvenanceManager

            prov = ProvenanceManager(conn)
            prov.record_ingestion(
                source_file=str(vcf_path),
                dataset_id=dataset_id or str(vcf_path),
                mode="incremental",
                n_samples=n_new,
                n_variants=summary.get("n_variants_extended", 0)
                + summary.get("n_variants_created", 0),
                filters_applied=str(filter_config) if filter_config else "",
                fidelity="default",
                reference_genome=reference,
            )
        except Exception:
            logger.warning("Failed to record provenance log", exc_info=True)
            summary["provenance_recorded"] = False
        else:
            summary["provenance_recorded"] = True

        return summary
