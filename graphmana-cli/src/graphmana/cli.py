"""GraphMana CLI — Click-based command-line interface."""

import json
import logging
import sys
from contextlib import contextmanager

import click

from graphmana import __version__
from graphmana.config import (
    DEFAULT_ANNOTATION_BATCH_SIZE,
    DEFAULT_BATCH_SIZE,
    DEFAULT_DATABASE,
    DEFAULT_NEO4J_PASSWORD,
    DEFAULT_NEO4J_URI,
    DEFAULT_NEO4J_USER,
    DEFAULT_THREADS,
)
from graphmana.db.connection import GraphManaConnection
from graphmana.snapshot.manager import DEFAULT_SNAPSHOT_DIR


@click.group()
@click.version_option(version=__version__, prog_name="graphmana")
def cli():
    """GraphMana — Graph-native data management for variant genomics."""


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------


@cli.command()
@click.option("--neo4j-uri", default=DEFAULT_NEO4J_URI, help="Neo4j Bolt URI.")
@click.option("--neo4j-user", default=DEFAULT_NEO4J_USER, help="Neo4j username.")
@click.option("--neo4j-password", default=DEFAULT_NEO4J_PASSWORD, help="Neo4j password.")
@click.option("--database", default=DEFAULT_DATABASE, help="Neo4j database name.")
@click.option("--json", "output_json", is_flag=True, help="Output as JSON.")
@click.option("--detailed", is_flag=True, help="Show detailed statistics.")
def status(neo4j_uri, neo4j_user, neo4j_password, database, output_json, detailed):
    """Show database status and node counts."""
    try:
        with GraphManaConnection(neo4j_uri, neo4j_user, neo4j_password, database=database) as conn:
            counts = {}
            for label in ["Variant", "Sample", "Population", "Chromosome", "Gene", "VCFHeader"]:
                counts[label] = conn.execute_read(
                    f"MATCH (n:{label}) RETURN count(n) AS c"
                ).single()["c"]

            schema_meta = conn.execute_read("MATCH (m:SchemaMetadata) RETURN m LIMIT 1").single()

            info = {
                "version": __version__,
                "neo4j_uri": neo4j_uri,
                "nodes": counts,
            }

            if schema_meta:
                meta = dict(schema_meta["m"])
                info["schema"] = {
                    "schema_version": meta.get("schema_version"),
                    "reference_genome": meta.get("reference_genome"),
                    "graphmana_version": meta.get("graphmana_version"),
                }

            if detailed:
                info["detailed"] = {
                    "total_nodes": sum(counts.values()),
                    "relationships": conn.execute_read(
                        "MATCH ()-[r]->() RETURN count(r) AS c"
                    ).single()["c"],
                }

            if output_json:
                click.echo(json.dumps(info, indent=2))
            else:
                click.echo(f"GraphMana v{__version__}")
                click.echo(f"Connected to: {neo4j_uri}")
                click.echo()
                click.echo("Node counts:")
                for label, count in counts.items():
                    click.echo(f"  {label:15s} {count:>10,}")
                if schema_meta:
                    click.echo()
                    click.echo(f"Schema version:   {info['schema']['schema_version']}")
                    click.echo(f"Reference genome: {info['schema']['reference_genome']}")
                if detailed and "detailed" in info:
                    click.echo()
                    click.echo(f"Total nodes:      {info['detailed']['total_nodes']:,}")
                    click.echo(f"Relationships:    {info['detailed']['relationships']:,}")

    except Exception as e:
        click.echo(f"Error connecting to Neo4j: {e}", err=True)
        sys.exit(1)


# ---------------------------------------------------------------------------
# prepare-csv
# ---------------------------------------------------------------------------


@cli.command("prepare-csv")
@click.option(
    "--input",
    "input_files",
    required=True,
    multiple=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Input VCF/BCF file(s).",
)
@click.option(
    "--input-list",
    type=click.Path(exists=True, dir_okay=False),
    help="File listing input VCF paths (one per line).",
)
@click.option(
    "--population-map",
    "panel_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Population panel/PED file.",
)
@click.option(
    "--output-dir",
    required=True,
    type=click.Path(file_okay=False),
    help="Output directory for CSV files.",
)
@click.option(
    "--stratify-by",
    type=click.Choice(["population", "superpopulation"]),
    default="superpopulation",
    help="Population stratification level.",
)
@click.option("--reference", default="unknown", help="Reference genome identifier.")
@click.option(
    "--ancestral-fasta",
    type=click.Path(exists=True, dir_okay=False),
    help="Ancestral allele FASTA for polarization.",
)
@click.option(
    "--chr-style",
    type=click.Choice(["auto", "ucsc", "ensembl", "original"]),
    default="auto",
    help="Chromosome naming style.",
)
@click.option(
    "--chr-map",
    "chr_map_path",
    type=click.Path(exists=True, dir_okay=False),
    help="Custom chromosome name mapping file.",
)
@click.option("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Variants per chunk.")
@click.option("--threads", type=int, default=DEFAULT_THREADS, help="Number of threads.")
@click.option("--filter-min-qual", type=float, help="Minimum QUAL threshold.")
@click.option("--filter-min-call-rate", type=float, help="Minimum call rate threshold.")
@click.option("--filter-maf-min", type=float, help="Minimum minor allele frequency.")
@click.option("--filter-maf-max", type=float, help="Maximum minor allele frequency.")
@click.option(
    "--filter-variant-type",
    "filter_variant_types",
    multiple=True,
    type=click.Choice(["SNP", "INDEL", "SV"]),
    help="Include only these variant types.",
)
@click.option(
    "--filter-region",
    type=str,
    help="Genomic region to import (e.g. 'chr1:1000-2000').",
)
@click.option(
    "--filter-contigs",
    "filter_contigs",
    multiple=True,
    help="Import only these chromosomes/contigs (repeatable).",
)
@click.option(
    "--vep-vcf",
    type=click.Path(exists=True, dir_okay=False),
    help="VEP/SnpEff annotated VCF for consequence annotation.",
)
@click.option("--annotation-version", default="unknown", help="Annotation version label.")
@click.option("--dataset-id", default="", help="Dataset identifier.")
@click.option(
    "--include-filtered",
    is_flag=True,
    default=False,
    help="Include variants with FILTER != PASS (default: exclude).",
)
@click.option("--verbose/--quiet", default=False, help="Verbose logging.")
@click.option("--dry-run", is_flag=True, help="Show what would be done without executing.")
@click.option("--normalize", is_flag=True, help="Run bcftools norm before parsing.")
@click.option(
    "--reference-fasta",
    type=click.Path(exists=True, dir_okay=False),
    help="Reference FASTA for normalization (required with --normalize).",
)
def prepare_csv(
    input_files,
    input_list,
    panel_path,
    output_dir,
    stratify_by,
    reference,
    ancestral_fasta,
    chr_style,
    chr_map_path,
    batch_size,
    threads,
    filter_min_qual,
    filter_min_call_rate,
    filter_maf_min,
    filter_maf_max,
    filter_variant_types,
    filter_region,
    filter_contigs,
    vep_vcf,
    annotation_version,
    dataset_id,
    include_filtered,
    verbose,
    dry_run,
    normalize,
    reference_fasta,
):
    """Generate CSV files from VCF (no Neo4j needed)."""
    _setup_logging(verbose)

    # Collect input files
    all_inputs = list(input_files)
    if input_list:
        with open(input_list) as f:
            for line in f:
                line = line.strip()
                if line:
                    all_inputs.append(line)

    if not all_inputs:
        click.echo("Error: no input files specified.", err=True)
        sys.exit(1)

    # Optional normalization step
    if normalize:
        if not reference_fasta:
            click.echo("Error: --reference-fasta required with --normalize.", err=True)
            sys.exit(1)
        from graphmana.ingest.normalizer import normalize_vcf

        normalized_inputs = []
        for inp in all_inputs:
            from pathlib import Path as P

            inp_path = P(inp)
            norm_out = P(output_dir) / f"{inp_path.stem}.norm.vcf.gz"
            P(output_dir).mkdir(parents=True, exist_ok=True)
            click.echo(f"Normalizing {inp} -> {norm_out}")
            result = normalize_vcf(inp, norm_out, reference_fasta)
            click.echo(
                f"  {result.total_records} records, "
                f"{result.split_records} split, "
                f"{result.realigned_records} realigned"
            )
            normalized_inputs.append(str(norm_out))
        all_inputs = normalized_inputs

    # Build filter config
    filter_config = _build_filter_config(
        filter_min_qual,
        filter_min_call_rate,
        filter_maf_min,
        filter_maf_max,
        filter_variant_types,
        region=filter_region,
        contigs=list(filter_contigs) if filter_contigs else None,
    )

    if dry_run:
        click.echo("Dry run — would process:")
        for f in all_inputs:
            click.echo(f"  {f}")
        click.echo(f"Output: {output_dir}")
        click.echo(f"Filters: {filter_config}")
        return

    from graphmana.ingest.pipeline import run_prepare_csv

    common_kwargs = dict(
        stratify_by=stratify_by,
        reference=reference,
        ancestral_fasta=ancestral_fasta,
        chr_style=chr_style,
        chr_map_path=chr_map_path,
        chunk_size=batch_size,
        filter_config=filter_config,
        vep_vcf_path=vep_vcf,
        annotation_version=annotation_version,
        dataset_id=dataset_id,
        verbose=verbose,
        threads=threads,
        include_filtered=include_filtered,
    )

    if len(all_inputs) == 1:
        summary = run_prepare_csv(
            all_inputs[0], panel_path, output_dir, **common_kwargs,
        )
    else:
        # Multi-file parallel: process VCFs concurrently, then merge CSVs
        from graphmana.ingest.parallel import run_prepare_csv_multifile

        summary = run_prepare_csv_multifile(
            all_inputs,
            panel_path,
            output_dir,
            threads=threads,
            **{k: v for k, v in common_kwargs.items() if k != "threads"},
        )

    click.echo(
        f"CSV generation complete: {summary['n_variants']} variants, "
        f"{summary['n_samples']} samples, {summary['n_populations']} populations"
    )
    click.echo(f"Output: {output_dir}")


# ---------------------------------------------------------------------------
# load-csv
# ---------------------------------------------------------------------------


@cli.command("load-csv")
@click.option(
    "--csv-dir",
    required=True,
    type=click.Path(exists=True, file_okay=False),
    help="Directory containing CSV files from prepare-csv.",
)
@click.option(
    "--neo4j-home",
    required=True,
    type=click.Path(exists=True, file_okay=False),
    help="Neo4j installation directory.",
)
@click.option(
    "--neo4j-data-dir",
    type=click.Path(file_okay=False),
    help="Neo4j data directory.",
)
@click.option("--database", default=DEFAULT_DATABASE, help="Neo4j database name.")
@click.option("--auto-start-neo4j", is_flag=True, help="Auto start/stop Neo4j.")
@click.option(
    "--neo4j-uri", default=DEFAULT_NEO4J_URI, help="Neo4j Bolt URI (for post-import indexes)."
)
@click.option("--neo4j-user", default=DEFAULT_NEO4J_USER, help="Neo4j username.")
@click.option("--neo4j-password", default=DEFAULT_NEO4J_PASSWORD, help="Neo4j password.")
@click.option("--verbose/--quiet", default=False, help="Verbose logging.")
def load_csv(
    csv_dir,
    neo4j_home,
    neo4j_data_dir,
    database,
    auto_start_neo4j,
    neo4j_uri,
    neo4j_user,
    neo4j_password,
    verbose,
):
    """Load pre-generated CSVs into Neo4j."""
    _setup_logging(verbose)

    from graphmana.ingest.loader import run_load_csv, validate_csv_dir

    missing = validate_csv_dir(csv_dir)
    if missing:
        click.echo(f"Error: missing CSV files: {', '.join(missing)}", err=True)
        sys.exit(1)

    with _auto_neo4j_lifecycle(auto_start_neo4j, neo4j_home, neo4j_data_dir):
        try:
            run_load_csv(
                csv_dir,
                neo4j_home=neo4j_home,
                database=database,
            )
            click.echo("neo4j-admin import completed successfully.")
        except Exception as e:
            click.echo(f"Error: {e}", err=True)
            sys.exit(1)

        # Apply indexes and schema metadata if Neo4j is reachable
        try:
            from graphmana.ingest.loader import apply_post_import_indexes

            apply_post_import_indexes(neo4j_uri, neo4j_user, neo4j_password, database=database)
            click.echo("Schema metadata and indexes created.")
        except Exception:
            click.echo(
                "Note: Could not connect to Neo4j for post-import setup. "
                "Start Neo4j and run 'graphmana status' to verify, or re-run "
                "'graphmana ingest' with --neo4j-uri.",
                err=True,
            )


# ---------------------------------------------------------------------------
# ingest
# ---------------------------------------------------------------------------


@cli.command()
@click.option(
    "--input",
    "input_files",
    required=True,
    multiple=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Input VCF/BCF file(s).",
)
@click.option(
    "--input-list",
    type=click.Path(exists=True, dir_okay=False),
    help="File listing input VCF paths (one per line).",
)
@click.option(
    "--population-map",
    "panel_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Population panel/PED file.",
)
@click.option(
    "--mode",
    type=click.Choice(["auto", "initial", "incremental"]),
    default="auto",
    help="Import mode.",
)
@click.option(
    "--output-csv-dir",
    type=click.Path(file_okay=False),
    help="Keep CSVs in this directory (otherwise temp dir).",
)
@click.option(
    "--neo4j-home",
    type=click.Path(exists=True, file_okay=False),
    help="Neo4j installation directory (required for initial mode).",
)
@click.option("--neo4j-data-dir", type=click.Path(file_okay=False), help="Neo4j data directory.")
@click.option("--database", default=DEFAULT_DATABASE, help="Neo4j database name.")
@click.option("--auto-start-neo4j", is_flag=True, help="Auto start/stop Neo4j.")
@click.option("--neo4j-uri", default=DEFAULT_NEO4J_URI, help="Neo4j Bolt URI.")
@click.option("--neo4j-user", default=DEFAULT_NEO4J_USER, help="Neo4j username.")
@click.option("--neo4j-password", default=DEFAULT_NEO4J_PASSWORD, help="Neo4j password.")
@click.option(
    "--stratify-by",
    type=click.Choice(["population", "superpopulation"]),
    default="superpopulation",
    help="Population stratification level.",
)
@click.option("--reference", default="unknown", help="Reference genome identifier.")
@click.option(
    "--ancestral-fasta",
    type=click.Path(exists=True, dir_okay=False),
    help="Ancestral allele FASTA for polarization.",
)
@click.option(
    "--chr-style",
    type=click.Choice(["auto", "ucsc", "ensembl", "original"]),
    default="auto",
    help="Chromosome naming style.",
)
@click.option(
    "--chr-map",
    "chr_map_path",
    type=click.Path(exists=True, dir_okay=False),
    help="Custom chromosome name mapping file.",
)
@click.option("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Variants per chunk.")
@click.option("--threads", type=int, default=DEFAULT_THREADS, help="Number of threads.")
@click.option("--filter-min-qual", type=float, help="Minimum QUAL threshold.")
@click.option("--filter-min-call-rate", type=float, help="Minimum call rate threshold.")
@click.option("--filter-maf-min", type=float, help="Minimum minor allele frequency.")
@click.option("--filter-maf-max", type=float, help="Maximum minor allele frequency.")
@click.option(
    "--filter-variant-type",
    "filter_variant_types",
    multiple=True,
    type=click.Choice(["SNP", "INDEL", "SV"]),
    help="Include only these variant types.",
)
@click.option(
    "--filter-region",
    type=str,
    help="Genomic region to import (e.g. 'chr1:1000-2000').",
)
@click.option(
    "--filter-contigs",
    "filter_contigs",
    multiple=True,
    help="Import only these chromosomes/contigs (repeatable).",
)
@click.option(
    "--vep-vcf",
    type=click.Path(exists=True, dir_okay=False),
    help="VEP/SnpEff annotated VCF for consequence annotation.",
)
@click.option("--annotation-version", default="unknown", help="Annotation version label.")
@click.option("--dataset-id", default="", help="Dataset identifier.")
@click.option(
    "--on-duplicate",
    type=click.Choice(["error", "skip"]),
    default="error",
    help="Action when duplicate samples found (incremental mode).",
)
@click.option(
    "--include-filtered",
    is_flag=True,
    default=False,
    help="Include variants with FILTER != PASS (default: exclude).",
)
@click.option("--verbose/--quiet", default=False, help="Verbose logging.")
@click.option("--dry-run", is_flag=True, help="Show what would be done without executing.")
@click.option("--normalize", is_flag=True, help="Run bcftools norm before parsing.")
@click.option(
    "--reference-fasta",
    type=click.Path(exists=True, dir_okay=False),
    help="Reference FASTA for normalization (required with --normalize).",
)
def ingest(
    input_files,
    input_list,
    panel_path,
    mode,
    output_csv_dir,
    neo4j_home,
    neo4j_data_dir,
    database,
    auto_start_neo4j,
    neo4j_uri,
    neo4j_user,
    neo4j_password,
    stratify_by,
    reference,
    ancestral_fasta,
    chr_style,
    chr_map_path,
    batch_size,
    threads,
    filter_min_qual,
    filter_min_call_rate,
    filter_maf_min,
    filter_maf_max,
    filter_variant_types,
    filter_region,
    filter_contigs,
    vep_vcf,
    annotation_version,
    dataset_id,
    on_duplicate,
    include_filtered,
    verbose,
    dry_run,
    normalize,
    reference_fasta,
):
    """Import VCF data: generate CSVs and load into Neo4j."""
    _setup_logging(verbose)

    all_inputs = list(input_files)
    if input_list:
        with open(input_list) as f:
            for line in f:
                line = line.strip()
                if line:
                    all_inputs.append(line)

    if not all_inputs:
        click.echo("Error: no input files specified.", err=True)
        sys.exit(1)

    # Optional normalization step
    if normalize:
        if not reference_fasta:
            click.echo("Error: --reference-fasta required with --normalize.", err=True)
            sys.exit(1)
        from graphmana.ingest.normalizer import normalize_vcf

        normalized_inputs = []
        for inp in all_inputs:
            from pathlib import Path as P

            inp_path = P(inp)
            norm_dir = P(output_csv_dir) if output_csv_dir else P(inp_path.parent)
            norm_out = norm_dir / f"{inp_path.stem}.norm.vcf.gz"
            norm_dir.mkdir(parents=True, exist_ok=True)
            click.echo(f"Normalizing {inp} -> {norm_out}")
            result = normalize_vcf(inp, norm_out, reference_fasta)
            click.echo(
                f"  {result.total_records} records, "
                f"{result.split_records} split, "
                f"{result.realigned_records} realigned"
            )
            normalized_inputs.append(str(norm_out))
        all_inputs = normalized_inputs

    filter_config = _build_filter_config(
        filter_min_qual,
        filter_min_call_rate,
        filter_maf_min,
        filter_maf_max,
        filter_variant_types,
        region=filter_region,
        contigs=list(filter_contigs) if filter_contigs else None,
    )

    if dry_run:
        click.echo(f"Dry run — would ingest (mode={mode}):")
        for f in all_inputs:
            click.echo(f"  {f}")
        click.echo(f"Database: {database}")
        return

    from graphmana.ingest.pipeline import run_ingest

    vcf_path = all_inputs[0]
    if len(all_inputs) > 1:
        click.echo(
            f"Warning: {len(all_inputs)} files provided but v0.1 processes "
            f"only the first. Multi-file support in v0.5.",
            err=True,
        )

    with _auto_neo4j_lifecycle(auto_start_neo4j, neo4j_home, neo4j_data_dir):
        try:
            summary = run_ingest(
                vcf_path,
                panel_path,
                output_csv_dir=output_csv_dir,
                neo4j_home=neo4j_home,
                database=database,
                mode=mode,
                on_duplicate=on_duplicate,
                stratify_by=stratify_by,
                reference=reference,
                ancestral_fasta=ancestral_fasta,
                chr_style=chr_style,
                chr_map_path=chr_map_path,
                chunk_size=batch_size,
                filter_config=filter_config,
                vep_vcf_path=vep_vcf,
                annotation_version=annotation_version,
                dataset_id=dataset_id,
                verbose=verbose,
                threads=threads,
                neo4j_uri=neo4j_uri,
                neo4j_user=neo4j_user,
                neo4j_password=neo4j_password,
                include_filtered=include_filtered,
            )

            # Display results based on mode
            if summary.get("mode") == "initial" or "n_variants" in summary:
                n_v = summary.get("n_variants", 0)
                n_s = summary.get("n_samples", summary.get("n_samples_created", 0))
                click.echo(f"Import complete (initial): {n_v} variants, {n_s} samples")
            else:
                click.echo(
                    f"Incremental import complete: "
                    f"{summary.get('n_variants_extended', 0)} extended, "
                    f"{summary.get('n_variants_created', 0)} new variants, "
                    f"{summary.get('n_samples_created', 0)} new samples"
                )
                if summary.get("skipped_duplicates"):
                    click.echo(f"Skipped duplicates: {summary['skipped_duplicates']}")

        except Exception as e:
            click.echo(f"Error: {e}", err=True)
            sys.exit(1)


# ---------------------------------------------------------------------------
# export
# ---------------------------------------------------------------------------


@cli.command()
@click.option(
    "--output",
    required=True,
    type=click.Path(),
    help="Output file path (or stem for PLINK).",
)
@click.option(
    "--format",
    "fmt",
    required=True,
    type=click.Choice(
        [
            "vcf",
            "plink",
            "plink2",
            "eigenstrat",
            "treemix",
            "sfs-dadi",
            "sfs-fsc",
            "bed",
            "tsv",
            "beagle",
            "structure",
            "genepop",
            "hap",
            "json",
            "zarr",
            "gds",
            "bgen",
        ]
    ),
    help="Export format.",
)
@click.option("--neo4j-uri", default=DEFAULT_NEO4J_URI, help="Neo4j Bolt URI.")
@click.option("--neo4j-user", default=DEFAULT_NEO4J_USER, help="Neo4j username.")
@click.option("--neo4j-password", default=DEFAULT_NEO4J_PASSWORD, help="Neo4j password.")
@click.option("--database", default=DEFAULT_DATABASE, help="Neo4j database name.")
@click.option(
    "--populations",
    multiple=True,
    help="Export only these populations.",
)
@click.option(
    "--chromosomes",
    multiple=True,
    help="Export only these chromosomes.",
)
@click.option("--region", type=str, help="Genomic region (e.g. 'chr1:1000-2000').")
@click.option(
    "--filter-variant-type",
    "filter_variant_types",
    multiple=True,
    type=click.Choice(["SNP", "INDEL", "SV"]),
    help="Include only these variant types.",
)
@click.option("--filter-maf-min", type=float, help="Minimum minor allele frequency.")
@click.option("--filter-maf-max", type=float, help="Maximum minor allele frequency.")
@click.option("--filter-min-call-rate", type=float, help="Minimum call rate.")
@click.option(
    "--filter-consequence",
    "filter_consequences",
    multiple=True,
    help="Include variants with these consequence types (e.g. missense_variant).",
)
@click.option(
    "--filter-impact",
    "filter_impacts",
    multiple=True,
    type=click.Choice(["HIGH", "MODERATE", "LOW", "MODIFIER"]),
    help="Include variants with these impact levels.",
)
@click.option(
    "--filter-gene",
    "filter_genes",
    multiple=True,
    help="Include variants annotated to these genes (symbol or Ensembl ID).",
)
@click.option("--filter-cadd-min", type=float, help="Minimum CADD phred score.")
@click.option("--filter-cadd-max", type=float, help="Maximum CADD phred score.")
@click.option(
    "--filter-annotation-version",
    type=str,
    help="Include variants with annotations from this version.",
)
@click.option(
    "--filter-sv-type",
    "filter_sv_types",
    multiple=True,
    type=click.Choice(["DEL", "DUP", "INV", "INS", "BND", "CNV"]),
    help="Include only structural variants of these types.",
)
@click.option(
    "--filter-liftover-status",
    type=click.Choice(["mapped", "unmapped", "collision"]),
    default=None,
    help="Include only variants with this liftover status.",
)
@click.option(
    "--recalculate-af/--no-recalculate-af",
    default=None,
    help="Recalculate allele frequencies after population filtering. "
    "Default: True when --populations is set, False otherwise.",
)
@click.option(
    "--sfs-include-monomorphic",
    is_flag=True,
    default=False,
    help="Include monomorphic sites in SFS (default: exclude).",
)
@click.option(
    "--vcf-version",
    type=click.Choice(["4.1", "4.2", "4.3"]),
    default="4.3",
    help="VCF format version header. Default: 4.3.",
)
@click.option(
    "--output-type",
    "output_type",
    type=click.Choice(["v", "z", "b"]),
    default=None,
    help="VCF output type: v=VCF, z=gzipped VCF, b=BCF. Default: auto-detect from extension.",
)
@click.option("--phased", is_flag=True, help="Output phased genotypes (VCF only).")
@click.option(
    "--reconstruct-multiallelic/--no-reconstruct-multiallelic",
    default=True,
    help="Reconstruct multi-allelic VCF lines from split variants (VCF only).",
)
@click.option(
    "--tsv-columns",
    multiple=True,
    help="Columns to include (TSV only).",
)
@click.option(
    "--sfs-populations",
    multiple=True,
    help="Populations for SFS (required for sfs-dadi/sfs-fsc).",
)
@click.option(
    "--sfs-projection",
    multiple=True,
    type=int,
    help="Projection sizes per population (required for sfs-dadi/sfs-fsc).",
)
@click.option(
    "--sfs-polarized/--sfs-folded",
    default=True,
    help="Polarized (unfolded) or folded SFS. Default: polarized.",
)
@click.option(
    "--bed-extra-columns",
    multiple=True,
    help="Extra columns for BED format (e.g. variant_type, af_total).",
)
@click.option(
    "--structure-format",
    "structure_format",
    type=click.Choice(["onerow", "tworow"]),
    default="onerow",
    help="STRUCTURE output format (onerow or tworow). Default: onerow.",
)
@click.option(
    "--json-fields",
    multiple=True,
    help="Fields to include in JSON output.",
)
@click.option("--json-pretty", is_flag=True, help="Pretty-print JSON output.")
@click.option(
    "--json-include-genotypes",
    is_flag=True,
    help="Include per-sample genotypes in JSON (FULL PATH).",
)
@click.option(
    "--zarr-chunk-size",
    type=int,
    default=10000,
    help="Number of variants per chunk in Zarr output. Default: 10000.",
)
@click.option("--filter-cohort", type=str, help="Filter samples by named cohort.")
@click.option(
    "--filter-sample-list",
    type=click.Path(exists=True, dir_okay=False),
    help="File with sample IDs to include (one per line).",
)
@click.option("--threads", type=int, default=DEFAULT_THREADS, help="Number of threads.")
@click.option("--auto-start-neo4j", is_flag=True, help="Auto start/stop Neo4j around export.")
@click.option(
    "--neo4j-home",
    "export_neo4j_home",
    type=click.Path(exists=True, file_okay=False),
    default=None,
    help="Neo4j installation directory (required with --auto-start-neo4j).",
)
@click.option(
    "--neo4j-data-dir",
    "export_neo4j_data_dir",
    type=click.Path(file_okay=False),
    default=None,
    help="Neo4j data directory.",
)
@click.option("--verbose/--quiet", default=False, help="Verbose logging.")
def export(
    output,
    fmt,
    neo4j_uri,
    neo4j_user,
    neo4j_password,
    database,
    populations,
    chromosomes,
    region,
    filter_variant_types,
    filter_maf_min,
    filter_maf_max,
    filter_min_call_rate,
    filter_consequences,
    filter_impacts,
    filter_genes,
    filter_cadd_min,
    filter_cadd_max,
    filter_annotation_version,
    filter_sv_types,
    filter_liftover_status,
    recalculate_af,
    sfs_include_monomorphic,
    vcf_version,
    output_type,
    phased,
    reconstruct_multiallelic,
    tsv_columns,
    sfs_populations,
    sfs_projection,
    sfs_polarized,
    bed_extra_columns,
    structure_format,
    json_fields,
    json_pretty,
    json_include_genotypes,
    zarr_chunk_size,
    filter_cohort,
    filter_sample_list,
    threads,
    auto_start_neo4j,
    export_neo4j_home,
    export_neo4j_data_dir,
    verbose,
):
    """Export data from Neo4j to various formats."""
    _setup_logging(verbose)

    from pathlib import Path

    from graphmana.filtering.export_filters import ExportFilterConfig

    sample_ids = None
    if filter_sample_list:
        from graphmana.sample.manager import load_sample_ids_from_file

        sample_ids = load_sample_ids_from_file(filter_sample_list)

    # Resolve recalculate_af conditional default: True when populations are
    # filtered, False otherwise (user can override with the explicit flag).
    if recalculate_af is None:
        recalculate_af = bool(populations)

    filter_config = ExportFilterConfig(
        populations=list(populations) if populations else None,
        chromosomes=list(chromosomes) if chromosomes else None,
        region=region,
        variant_types=set(filter_variant_types) if filter_variant_types else None,
        maf_min=filter_maf_min,
        maf_max=filter_maf_max,
        min_call_rate=filter_min_call_rate,
        cohort=filter_cohort,
        sample_ids=sample_ids,
        consequences=list(filter_consequences) if filter_consequences else None,
        impacts=list(filter_impacts) if filter_impacts else None,
        genes=list(filter_genes) if filter_genes else None,
        cadd_min=filter_cadd_min,
        cadd_max=filter_cadd_max,
        annotation_version=filter_annotation_version,
        sv_types=set(filter_sv_types) if filter_sv_types else None,
        liftover_status=filter_liftover_status,
    )

    with _auto_neo4j_lifecycle(auto_start_neo4j, export_neo4j_home, export_neo4j_data_dir):
        try:
            with GraphManaConnection(
                neo4j_uri, neo4j_user, neo4j_password, database=database
            ) as conn:
                if fmt == "vcf":
                    from graphmana.export.vcf_export import VCFExporter

                    exporter = VCFExporter(
                        conn,
                        filter_config=filter_config,
                        threads=threads,
                        recalculate_af=recalculate_af,
                    )
                    summary = exporter.export(
                        Path(output),
                        phased=phased,
                        vcf_version=vcf_version,
                        output_type=output_type,
                        reconstruct_multiallelic=reconstruct_multiallelic,
                    )
                elif fmt == "plink":
                    from graphmana.export.plink_export import PLINKExporter

                    exporter = PLINKExporter(
                        conn,
                        filter_config=filter_config,
                        threads=threads,
                        recalculate_af=recalculate_af,
                    )
                    summary = exporter.export(Path(output))
                elif fmt == "plink2":
                    from graphmana.export.plink2_export import PLINK2Exporter

                    exporter = PLINK2Exporter(
                        conn,
                        filter_config=filter_config,
                        threads=threads,
                        recalculate_af=recalculate_af,
                    )
                    summary = exporter.export(Path(output))
                elif fmt == "eigenstrat":
                    from graphmana.export.eigenstrat_export import EIGENSTRATExporter

                    exporter = EIGENSTRATExporter(
                        conn,
                        filter_config=filter_config,
                        threads=threads,
                        recalculate_af=recalculate_af,
                    )
                    summary = exporter.export(Path(output))
                elif fmt == "treemix":
                    from graphmana.export.treemix_export import TreeMixExporter

                    exporter = TreeMixExporter(
                        conn,
                        filter_config=filter_config,
                        threads=threads,
                        recalculate_af=recalculate_af,
                    )
                    summary = exporter.export(Path(output))
                elif fmt in ("sfs-dadi", "sfs-fsc"):
                    sfs_pops = list(sfs_populations)
                    sfs_proj = list(sfs_projection)
                    if not sfs_pops:
                        click.echo(
                            "Error: --sfs-populations required for SFS formats.",
                            err=True,
                        )
                        sys.exit(1)
                    if not sfs_proj:
                        click.echo(
                            "Error: --sfs-projection required for SFS formats.",
                            err=True,
                        )
                        sys.exit(1)

                    if fmt == "sfs-dadi":
                        from graphmana.export.sfs_dadi_export import SFSDadiExporter

                        exporter = SFSDadiExporter(
                            conn,
                            filter_config=filter_config,
                            threads=threads,
                            recalculate_af=recalculate_af,
                        )
                        summary = exporter.export(
                            Path(output),
                            populations=sfs_pops,
                            projection=sfs_proj,
                            polarized=sfs_polarized,
                            include_monomorphic=sfs_include_monomorphic,
                        )
                    else:
                        from graphmana.export.sfs_fsc_export import SFSFscExporter

                        exporter = SFSFscExporter(
                            conn,
                            filter_config=filter_config,
                            threads=threads,
                            recalculate_af=recalculate_af,
                        )
                        summary = exporter.export(
                            Path(output),
                            populations=sfs_pops,
                            projection=sfs_proj,
                            polarized=sfs_polarized,
                            include_monomorphic=sfs_include_monomorphic,
                        )
                elif fmt == "bed":
                    from graphmana.export.bed_export import BEDExporter

                    exporter = BEDExporter(
                        conn,
                        filter_config=filter_config,
                        threads=threads,
                        recalculate_af=recalculate_af,
                    )
                    extras = list(bed_extra_columns) if bed_extra_columns else None
                    summary = exporter.export(Path(output), extra_columns=extras)
                elif fmt == "tsv":
                    from graphmana.export.tsv_export import TSVExporter

                    exporter = TSVExporter(
                        conn,
                        filter_config=filter_config,
                        threads=threads,
                        recalculate_af=recalculate_af,
                    )
                    cols = list(tsv_columns) if tsv_columns else None
                    summary = exporter.export(Path(output), columns=cols)
                elif fmt == "beagle":
                    from graphmana.export.beagle_export import BeagleExporter

                    exporter = BeagleExporter(
                        conn,
                        filter_config=filter_config,
                        threads=threads,
                        recalculate_af=recalculate_af,
                    )
                    summary = exporter.export(Path(output))
                elif fmt == "structure":
                    from graphmana.export.structure_export import STRUCTUREExporter

                    exporter = STRUCTUREExporter(
                        conn,
                        filter_config=filter_config,
                        threads=threads,
                        recalculate_af=recalculate_af,
                    )
                    summary = exporter.export(Path(output), output_format=structure_format)
                elif fmt == "genepop":
                    from graphmana.export.genepop_export import GenepopExporter

                    exporter = GenepopExporter(
                        conn,
                        filter_config=filter_config,
                        threads=threads,
                        recalculate_af=recalculate_af,
                    )
                    summary = exporter.export(Path(output))
                elif fmt == "hap":
                    from graphmana.export.hap_export import HAPExporter

                    exporter = HAPExporter(
                        conn,
                        filter_config=filter_config,
                        threads=threads,
                        recalculate_af=recalculate_af,
                    )
                    summary = exporter.export(Path(output))
                elif fmt == "json":
                    from graphmana.export.json_export import JSONExporter

                    exporter = JSONExporter(
                        conn,
                        filter_config=filter_config,
                        threads=threads,
                        recalculate_af=recalculate_af,
                    )
                    fields = list(json_fields) if json_fields else None
                    summary = exporter.export(
                        Path(output),
                        fields=fields,
                        pretty=json_pretty,
                        include_genotypes=json_include_genotypes,
                    )
                elif fmt == "zarr":
                    from graphmana.export.zarr_export import ZarrExporter

                    exporter = ZarrExporter(
                        conn,
                        filter_config=filter_config,
                        threads=threads,
                        recalculate_af=recalculate_af,
                    )
                    summary = exporter.export(Path(output), chunk_size=zarr_chunk_size)
                elif fmt == "gds":
                    from graphmana.export.gds_export import GDSExporter

                    exporter = GDSExporter(
                        conn,
                        filter_config=filter_config,
                        threads=threads,
                        recalculate_af=recalculate_af,
                    )
                    summary = exporter.export(Path(output))
                elif fmt == "bgen":
                    from graphmana.export.bgen_export import BGENExporter

                    exporter = BGENExporter(
                        conn,
                        filter_config=filter_config,
                        threads=threads,
                        recalculate_af=recalculate_af,
                    )
                    summary = exporter.export(Path(output))
                else:
                    click.echo(f"Unsupported format: {fmt}", err=True)
                    sys.exit(1)

            click.echo(f"Export complete ({fmt}): {summary['n_variants']} variants")
            if "n_samples" in summary:
                click.echo(f"Samples: {summary['n_samples']}")
            if "n_skipped" in summary and summary["n_skipped"] > 0:
                click.echo(f"Skipped: {summary['n_skipped']} (non-SNP)")

        except Exception as e:
            click.echo(f"Error: {e}", err=True)
            sys.exit(1)


# ---------------------------------------------------------------------------
# cohort
# ---------------------------------------------------------------------------


@cli.group()
def cohort():
    """Manage named cohort definitions."""


@cohort.command("define")
@click.option("--name", required=True, help="Cohort name (unique).")
@click.option("--query", "cypher_query", required=True, help="Cypher query returning sampleId.")
@click.option("--description", default="", help="Human-readable description.")
@click.option("--neo4j-uri", default=DEFAULT_NEO4J_URI, help="Neo4j Bolt URI.")
@click.option("--neo4j-user", default=DEFAULT_NEO4J_USER, help="Neo4j username.")
@click.option("--neo4j-password", default=DEFAULT_NEO4J_PASSWORD, help="Neo4j password.")
@click.option("--database", default=DEFAULT_DATABASE, help="Neo4j database name.")
def cohort_define(name, cypher_query, description, neo4j_uri, neo4j_user, neo4j_password, database):
    """Define or update a named cohort."""
    try:
        with GraphManaConnection(neo4j_uri, neo4j_user, neo4j_password, database=database) as conn:
            from graphmana.cohort.manager import CohortManager

            mgr = CohortManager(conn)
            props = mgr.define(name, cypher_query, description=description)
            click.echo(f"Cohort '{name}' defined ({props.get('created_date', 'unknown')})")
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cohort.command("list")
@click.option("--neo4j-uri", default=DEFAULT_NEO4J_URI, help="Neo4j Bolt URI.")
@click.option("--neo4j-user", default=DEFAULT_NEO4J_USER, help="Neo4j username.")
@click.option("--neo4j-password", default=DEFAULT_NEO4J_PASSWORD, help="Neo4j password.")
@click.option("--database", default=DEFAULT_DATABASE, help="Neo4j database name.")
def cohort_list(neo4j_uri, neo4j_user, neo4j_password, database):
    """List all cohort definitions."""
    try:
        with GraphManaConnection(neo4j_uri, neo4j_user, neo4j_password, database=database) as conn:
            from graphmana.cohort.manager import CohortManager

            mgr = CohortManager(conn)
            cohorts = mgr.list()
            if not cohorts:
                click.echo("No cohorts defined.")
                return
            for c in cohorts:
                desc = f" — {c['description']}" if c.get("description") else ""
                click.echo(f"  {c['name']}{desc}")
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cohort.command("show")
@click.option("--name", required=True, help="Cohort name.")
@click.option("--neo4j-uri", default=DEFAULT_NEO4J_URI, help="Neo4j Bolt URI.")
@click.option("--neo4j-user", default=DEFAULT_NEO4J_USER, help="Neo4j username.")
@click.option("--neo4j-password", default=DEFAULT_NEO4J_PASSWORD, help="Neo4j password.")
@click.option("--database", default=DEFAULT_DATABASE, help="Neo4j database name.")
def cohort_show(name, neo4j_uri, neo4j_user, neo4j_password, database):
    """Show details of a named cohort."""
    try:
        with GraphManaConnection(neo4j_uri, neo4j_user, neo4j_password, database=database) as conn:
            from graphmana.cohort.manager import CohortManager

            mgr = CohortManager(conn)
            c = mgr.get(name)
            if c is None:
                click.echo(f"Cohort not found: {name}", err=True)
                sys.exit(1)
            click.echo(f"Name:        {c['name']}")
            click.echo(f"Description: {c.get('description', '')}")
            click.echo(f"Created:     {c.get('created_date', 'unknown')}")
            click.echo(f"Query:       {c['cypher_query']}")
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cohort.command("delete")
@click.option("--name", required=True, help="Cohort name.")
@click.option("--neo4j-uri", default=DEFAULT_NEO4J_URI, help="Neo4j Bolt URI.")
@click.option("--neo4j-user", default=DEFAULT_NEO4J_USER, help="Neo4j username.")
@click.option("--neo4j-password", default=DEFAULT_NEO4J_PASSWORD, help="Neo4j password.")
@click.option("--database", default=DEFAULT_DATABASE, help="Neo4j database name.")
def cohort_delete(name, neo4j_uri, neo4j_user, neo4j_password, database):
    """Delete a named cohort."""
    try:
        with GraphManaConnection(neo4j_uri, neo4j_user, neo4j_password, database=database) as conn:
            from graphmana.cohort.manager import CohortManager

            mgr = CohortManager(conn)
            deleted = mgr.delete(name)
            if deleted:
                click.echo(f"Cohort '{name}' deleted.")
            else:
                click.echo(f"Cohort not found: {name}", err=True)
                sys.exit(1)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cohort.command("count")
@click.option("--name", required=True, help="Cohort name.")
@click.option("--neo4j-uri", default=DEFAULT_NEO4J_URI, help="Neo4j Bolt URI.")
@click.option("--neo4j-user", default=DEFAULT_NEO4J_USER, help="Neo4j username.")
@click.option("--neo4j-password", default=DEFAULT_NEO4J_PASSWORD, help="Neo4j password.")
@click.option("--database", default=DEFAULT_DATABASE, help="Neo4j database name.")
def cohort_count(name, neo4j_uri, neo4j_user, neo4j_password, database):
    """Count samples matching a named cohort."""
    try:
        with GraphManaConnection(neo4j_uri, neo4j_user, neo4j_password, database=database) as conn:
            from graphmana.cohort.manager import CohortManager

            mgr = CohortManager(conn)
            n = mgr.count(name)
            click.echo(f"Cohort '{name}': {n} samples")
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cohort.command("validate")
@click.option("--query", "cypher_query", required=True, help="Cypher query to validate.")
@click.option("--neo4j-uri", default=DEFAULT_NEO4J_URI, help="Neo4j Bolt URI.")
@click.option("--neo4j-user", default=DEFAULT_NEO4J_USER, help="Neo4j username.")
@click.option("--neo4j-password", default=DEFAULT_NEO4J_PASSWORD, help="Neo4j password.")
@click.option("--database", default=DEFAULT_DATABASE, help="Neo4j database name.")
def cohort_validate(cypher_query, neo4j_uri, neo4j_user, neo4j_password, database):
    """Validate a Cypher query for cohort use."""
    try:
        with GraphManaConnection(neo4j_uri, neo4j_user, neo4j_password, database=database) as conn:
            from graphmana.cohort.manager import CohortManager

            mgr = CohortManager(conn)
            result = mgr.validate(cypher_query)
            if result["valid"]:
                click.echo(f"Valid. Would select {result['n_samples']} samples.")
            else:
                click.echo(f"Invalid: {result['error']}", err=True)
                sys.exit(1)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


# ---------------------------------------------------------------------------
# sample
# ---------------------------------------------------------------------------


@cli.group()
def sample():
    """Manage samples (remove, restore, reassign, hard-remove, list)."""


@sample.command("remove")
@click.option(
    "--sample-ids",
    multiple=True,
    help="Sample IDs to exclude (repeatable).",
)
@click.option(
    "--sample-list",
    type=click.Path(exists=True, dir_okay=False),
    help="File with sample IDs (one per line).",
)
@click.option("--reason", default="", help="Exclusion reason.")
@click.option("--neo4j-uri", default=DEFAULT_NEO4J_URI, help="Neo4j Bolt URI.")
@click.option("--neo4j-user", default=DEFAULT_NEO4J_USER, help="Neo4j username.")
@click.option("--neo4j-password", default=DEFAULT_NEO4J_PASSWORD, help="Neo4j password.")
@click.option("--database", default=DEFAULT_DATABASE, help="Neo4j database name.")
def sample_remove(sample_ids, sample_list, reason, neo4j_uri, neo4j_user, neo4j_password, database):
    """Soft-delete samples (set excluded=true)."""
    ids = list(sample_ids)
    if sample_list:
        from graphmana.sample.manager import load_sample_ids_from_file

        ids.extend(load_sample_ids_from_file(sample_list))

    if not ids:
        click.echo("Error: provide --sample-ids or --sample-list.", err=True)
        sys.exit(1)

    try:
        with GraphManaConnection(neo4j_uri, neo4j_user, neo4j_password, database=database) as conn:
            from graphmana.sample.manager import SampleManager

            mgr = SampleManager(conn)
            result = mgr.remove(ids, reason=reason)
            click.echo(f"Excluded {result['n_excluded']} sample(s).")
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@sample.command("restore")
@click.option(
    "--sample-ids",
    multiple=True,
    help="Sample IDs to restore (repeatable).",
)
@click.option(
    "--sample-list",
    type=click.Path(exists=True, dir_okay=False),
    help="File with sample IDs (one per line).",
)
@click.option("--neo4j-uri", default=DEFAULT_NEO4J_URI, help="Neo4j Bolt URI.")
@click.option("--neo4j-user", default=DEFAULT_NEO4J_USER, help="Neo4j username.")
@click.option("--neo4j-password", default=DEFAULT_NEO4J_PASSWORD, help="Neo4j password.")
@click.option("--database", default=DEFAULT_DATABASE, help="Neo4j database name.")
def sample_restore(sample_ids, sample_list, neo4j_uri, neo4j_user, neo4j_password, database):
    """Restore soft-deleted samples (clear excluded flag)."""
    ids = list(sample_ids)
    if sample_list:
        from graphmana.sample.manager import load_sample_ids_from_file

        ids.extend(load_sample_ids_from_file(sample_list))

    if not ids:
        click.echo("Error: provide --sample-ids or --sample-list.", err=True)
        sys.exit(1)

    try:
        with GraphManaConnection(neo4j_uri, neo4j_user, neo4j_password, database=database) as conn:
            from graphmana.sample.manager import SampleManager

            mgr = SampleManager(conn)
            result = mgr.restore(ids)
            click.echo(f"Restored {result['n_restored']} sample(s).")
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@sample.command("list")
@click.option("--population", help="Filter by population.")
@click.option("--show-excluded", is_flag=True, help="Include excluded samples in output.")
@click.option("--neo4j-uri", default=DEFAULT_NEO4J_URI, help="Neo4j Bolt URI.")
@click.option("--neo4j-user", default=DEFAULT_NEO4J_USER, help="Neo4j username.")
@click.option("--neo4j-password", default=DEFAULT_NEO4J_PASSWORD, help="Neo4j password.")
@click.option("--database", default=DEFAULT_DATABASE, help="Neo4j database name.")
def sample_list(population, show_excluded, neo4j_uri, neo4j_user, neo4j_password, database):
    """List samples with status."""
    try:
        with GraphManaConnection(neo4j_uri, neo4j_user, neo4j_password, database=database) as conn:
            from graphmana.sample.manager import SampleManager

            mgr = SampleManager(conn)
            samples = mgr.list(population=population, show_excluded=show_excluded)
            if not samples:
                click.echo("No samples found.")
                return

            counts = mgr.count()
            click.echo(
                f"Samples: {counts['active']} active, "
                f"{counts['excluded']} excluded, "
                f"{counts['total']} total"
            )
            click.echo()
            for s in samples:
                status = " [EXCLUDED]" if s.get("excluded") else ""
                reason = f" ({s['exclusion_reason']})" if s.get("exclusion_reason") else ""
                click.echo(f"  {s['sampleId']:20s}  {s['population']:15s}{status}{reason}")
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@sample.command("reassign")
@click.option(
    "--sample-ids",
    multiple=True,
    help="Sample IDs to reassign (repeatable).",
)
@click.option(
    "--sample-list",
    type=click.Path(exists=True, dir_okay=False),
    help="File with sample IDs (one per line).",
)
@click.option(
    "--new-population",
    required=True,
    help="Target population ID.",
)
@click.option("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Variant batch size.")
@click.option("--neo4j-uri", default=DEFAULT_NEO4J_URI, help="Neo4j Bolt URI.")
@click.option("--neo4j-user", default=DEFAULT_NEO4J_USER, help="Neo4j username.")
@click.option("--neo4j-password", default=DEFAULT_NEO4J_PASSWORD, help="Neo4j password.")
@click.option("--database", default=DEFAULT_DATABASE, help="Neo4j database name.")
@click.option("--verbose/--quiet", default=False, help="Verbose logging.")
def sample_reassign(
    sample_ids,
    sample_list,
    new_population,
    batch_size,
    neo4j_uri,
    neo4j_user,
    neo4j_password,
    database,
    verbose,
):
    """Move samples to a different population, updating all variant statistics."""
    _setup_logging(verbose)
    ids = list(sample_ids)
    if sample_list:
        from graphmana.sample.manager import load_sample_ids_from_file

        ids.extend(load_sample_ids_from_file(sample_list))

    if not ids:
        click.echo("Error: provide --sample-ids or --sample-list.", err=True)
        sys.exit(1)

    try:
        with GraphManaConnection(neo4j_uri, neo4j_user, neo4j_password, database=database) as conn:
            from graphmana.sample.manager import SampleManager

            mgr = SampleManager(conn)
            result = mgr.reassign(ids, new_population, batch_size=batch_size)
            click.echo(
                f"Reassigned {result['n_reassigned']} sample(s) to {new_population}. "
                f"Updated {result['n_variants_updated']} variants."
            )
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@sample.command("hard-remove")
@click.option(
    "--sample-ids",
    multiple=True,
    help="Sample IDs to permanently remove (repeatable).",
)
@click.option(
    "--sample-list",
    type=click.Path(exists=True, dir_okay=False),
    help="File with sample IDs (one per line).",
)
@click.option(
    "--require-soft-deleted",
    is_flag=True,
    help="Only remove samples already soft-deleted.",
)
@click.option("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Variant batch size.")
@click.option("--neo4j-uri", default=DEFAULT_NEO4J_URI, help="Neo4j Bolt URI.")
@click.option("--neo4j-user", default=DEFAULT_NEO4J_USER, help="Neo4j username.")
@click.option("--neo4j-password", default=DEFAULT_NEO4J_PASSWORD, help="Neo4j password.")
@click.option("--database", default=DEFAULT_DATABASE, help="Neo4j database name.")
@click.option("--yes", is_flag=True, help="Skip confirmation prompt.")
@click.option("--verbose/--quiet", default=False, help="Verbose logging.")
def sample_hard_remove(
    sample_ids,
    sample_list,
    require_soft_deleted,
    batch_size,
    neo4j_uri,
    neo4j_user,
    neo4j_password,
    database,
    yes,
    verbose,
):
    """Permanently remove samples by zeroing packed arrays and deleting nodes."""
    _setup_logging(verbose)
    ids = list(sample_ids)
    if sample_list:
        from graphmana.sample.manager import load_sample_ids_from_file

        ids.extend(load_sample_ids_from_file(sample_list))

    if not ids:
        click.echo("Error: provide --sample-ids or --sample-list.", err=True)
        sys.exit(1)

    if not yes:
        click.confirm(
            f"This will permanently remove {len(ids)} sample(s) and update all "
            f"variants. This cannot be undone. Continue?",
            abort=True,
        )

    try:
        with GraphManaConnection(neo4j_uri, neo4j_user, neo4j_password, database=database) as conn:
            from graphmana.sample.manager import SampleManager

            mgr = SampleManager(conn)
            result = mgr.hard_remove(
                ids,
                require_soft_deleted=require_soft_deleted,
                batch_size=batch_size,
            )
            click.echo(
                f"Hard-removed {result['n_removed']} sample(s). "
                f"Updated {result['n_variants_updated']} variants. "
                f"Populations affected: {', '.join(result['populations_affected'])}."
            )
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


# ---------------------------------------------------------------------------
# qc
# ---------------------------------------------------------------------------


@cli.command()
@click.option(
    "--type",
    "qc_type",
    type=click.Choice(["sample", "variant", "batch", "all"]),
    default="all",
    help="QC type to run.",
)
@click.option(
    "--output",
    required=True,
    type=click.Path(),
    help="Output file path.",
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["tsv", "json", "html"]),
    default="tsv",
    help="Output format.",
)
@click.option("--neo4j-uri", default=DEFAULT_NEO4J_URI, help="Neo4j Bolt URI.")
@click.option("--neo4j-user", default=DEFAULT_NEO4J_USER, help="Neo4j username.")
@click.option("--neo4j-password", default=DEFAULT_NEO4J_PASSWORD, help="Neo4j password.")
@click.option("--database", default=DEFAULT_DATABASE, help="Neo4j database name.")
@click.option("--verbose/--quiet", default=False, help="Verbose logging.")
def qc(qc_type, output, output_format, neo4j_uri, neo4j_user, neo4j_password, database, verbose):
    """Run quality control checks and generate a report."""
    _setup_logging(verbose)
    try:
        from pathlib import Path

        with GraphManaConnection(neo4j_uri, neo4j_user, neo4j_password, database=database) as conn:
            from graphmana.qc.formatters import write_qc_report
            from graphmana.qc.manager import QCManager

            mgr = QCManager(conn)
            results = mgr.run(qc_type)
            write_qc_report(results, Path(output), fmt=output_format)

        # Summary
        parts = []
        if "variant" in results:
            n_v = results["variant"]["summary"].get("n_variants", 0)
            parts.append(f"{n_v} variants")
        if "sample" in results:
            n_s = results["sample"].get("n_samples", 0)
            parts.append(f"{n_s} samples")
        if "batch" in results:
            n_p = len(results["batch"].get("population_summary", []))
            parts.append(f"{n_p} populations")
        click.echo(f"QC report written to {output} ({', '.join(parts)})")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


# ---------------------------------------------------------------------------
# annotate
# ---------------------------------------------------------------------------


@cli.group()
def annotate():
    """Manage annotation versions (load, list, remove)."""


@annotate.command("load")
@click.option(
    "--input",
    "input_file",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="VEP/SnpEff annotated VCF file.",
)
@click.option("--version", required=True, help="Annotation version label (e.g. 'VEP_v110').")
@click.option(
    "--mode",
    type=click.Choice(["add", "update", "replace"]),
    default="add",
    help="Load mode: add (layer), update (merge), replace (clean swap).",
)
@click.option(
    "--type",
    "annotation_type",
    type=click.Choice(["auto", "vep", "snpeff"]),
    default="auto",
    help="Annotation type (auto-detected from VCF header by default).",
)
@click.option("--description", default="", help="Human-readable description.")
@click.option(
    "--batch-size", type=int, default=DEFAULT_ANNOTATION_BATCH_SIZE, help="Edges per Cypher batch."
)
@click.option("--neo4j-uri", default=DEFAULT_NEO4J_URI, help="Neo4j Bolt URI.")
@click.option("--neo4j-user", default=DEFAULT_NEO4J_USER, help="Neo4j username.")
@click.option("--neo4j-password", default=DEFAULT_NEO4J_PASSWORD, help="Neo4j password.")
@click.option("--database", default=DEFAULT_DATABASE, help="Neo4j database name.")
@click.option("--verbose/--quiet", default=False, help="Verbose logging.")
def annotate_load(
    input_file,
    version,
    mode,
    annotation_type,
    description,
    batch_size,
    neo4j_uri,
    neo4j_user,
    neo4j_password,
    database,
    verbose,
):
    """Load annotations from a VEP/SnpEff VCF."""
    _setup_logging(verbose)
    try:
        with GraphManaConnection(neo4j_uri, neo4j_user, neo4j_password, database=database) as conn:
            from graphmana.annotation.manager import AnnotationManager

            mgr = AnnotationManager(conn)
            result = mgr.load(
                input_file,
                version,
                mode=mode,
                annotation_type=annotation_type,
                description=description,
                batch_size=batch_size,
            )
            click.echo(
                f"Annotation loaded: version={result['version']}, "
                f"mode={result['mode']}, {result['n_genes']} genes, "
                f"{result['n_edges']} edges ({result['source']})"
            )
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@annotate.command("list")
@click.option("--neo4j-uri", default=DEFAULT_NEO4J_URI, help="Neo4j Bolt URI.")
@click.option("--neo4j-user", default=DEFAULT_NEO4J_USER, help="Neo4j username.")
@click.option("--neo4j-password", default=DEFAULT_NEO4J_PASSWORD, help="Neo4j password.")
@click.option("--database", default=DEFAULT_DATABASE, help="Neo4j database name.")
def annotate_list(neo4j_uri, neo4j_user, neo4j_password, database):
    """List all annotation versions."""
    try:
        with GraphManaConnection(neo4j_uri, neo4j_user, neo4j_password, database=database) as conn:
            from graphmana.annotation.manager import AnnotationManager

            mgr = AnnotationManager(conn)
            versions = mgr.list()
            if not versions:
                click.echo("No annotation versions loaded.")
                return
            for v in versions:
                desc = f" — {v['description']}" if v.get("description") else ""
                click.echo(
                    f"  {v['version_id']}  ({v.get('source', '?')}, "
                    f"{v.get('n_annotations', 0)} edges, "
                    f"{v.get('loaded_date', 'unknown')}){desc}"
                )
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@annotate.command("remove")
@click.option("--version", required=True, help="Annotation version to remove.")
@click.option("--neo4j-uri", default=DEFAULT_NEO4J_URI, help="Neo4j Bolt URI.")
@click.option("--neo4j-user", default=DEFAULT_NEO4J_USER, help="Neo4j username.")
@click.option("--neo4j-password", default=DEFAULT_NEO4J_PASSWORD, help="Neo4j password.")
@click.option("--database", default=DEFAULT_DATABASE, help="Neo4j database name.")
def annotate_remove(version, neo4j_uri, neo4j_user, neo4j_password, database):
    """Remove an annotation version and its edges."""
    try:
        with GraphManaConnection(neo4j_uri, neo4j_user, neo4j_password, database=database) as conn:
            from graphmana.annotation.manager import AnnotationManager

            mgr = AnnotationManager(conn)
            result = mgr.remove(version)
            click.echo(
                f"Annotation removed: version={result['version']}, "
                f"{result['n_edges_deleted']} edges deleted, "
                f"{result['n_genes_deleted']} orphan genes removed"
            )
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@annotate.command("load-cadd")
@click.option(
    "--input",
    "input_file",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="CADD TSV file (plain or gzipped).",
)
@click.option("--version", required=True, help="Annotation version label (e.g. 'CADD_v1.7').")
@click.option("--chr-prefix", default="", help="Chromosome prefix (e.g. 'chr').")
@click.option(
    "--batch-size", type=int, default=DEFAULT_ANNOTATION_BATCH_SIZE, help="Records per batch."
)
@click.option("--description", default="", help="Human-readable description.")
@click.option("--neo4j-uri", default=DEFAULT_NEO4J_URI, help="Neo4j Bolt URI.")
@click.option("--neo4j-user", default=DEFAULT_NEO4J_USER, help="Neo4j username.")
@click.option("--neo4j-password", default=DEFAULT_NEO4J_PASSWORD, help="Neo4j password.")
@click.option("--database", default=DEFAULT_DATABASE, help="Neo4j database name.")
@click.option("--verbose/--quiet", default=False, help="Verbose logging.")
def annotate_load_cadd(
    input_file,
    version,
    chr_prefix,
    batch_size,
    description,
    neo4j_uri,
    neo4j_user,
    neo4j_password,
    database,
    verbose,
):
    """Load CADD scores from a TSV file."""
    _setup_logging(verbose)
    try:
        with GraphManaConnection(neo4j_uri, neo4j_user, neo4j_password, database=database) as conn:
            from graphmana.annotation.parsers.cadd import CADDParser

            parser = CADDParser(conn)
            result = parser.load(
                input_file,
                version,
                description=description,
                batch_size=batch_size,
                chr_prefix=chr_prefix,
            )
            click.echo(
                f"CADD loaded: version={result['version']}, "
                f"{result['n_parsed']} parsed, {result['n_matched']} matched"
            )
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@annotate.command("load-constraint")
@click.option(
    "--input",
    "input_file",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="gnomAD gene constraint TSV file.",
)
@click.option("--version", required=True, help="Annotation version label (e.g. 'gnomAD_v4.1').")
@click.option(
    "--batch-size", type=int, default=DEFAULT_ANNOTATION_BATCH_SIZE, help="Records per batch."
)
@click.option("--description", default="", help="Human-readable description.")
@click.option("--neo4j-uri", default=DEFAULT_NEO4J_URI, help="Neo4j Bolt URI.")
@click.option("--neo4j-user", default=DEFAULT_NEO4J_USER, help="Neo4j username.")
@click.option("--neo4j-password", default=DEFAULT_NEO4J_PASSWORD, help="Neo4j password.")
@click.option("--database", default=DEFAULT_DATABASE, help="Neo4j database name.")
@click.option("--verbose/--quiet", default=False, help="Verbose logging.")
def annotate_load_constraint(
    input_file,
    version,
    batch_size,
    description,
    neo4j_uri,
    neo4j_user,
    neo4j_password,
    database,
    verbose,
):
    """Load gene constraint scores (pLI, LOEUF, mis_z, syn_z)."""
    _setup_logging(verbose)
    try:
        with GraphManaConnection(neo4j_uri, neo4j_user, neo4j_password, database=database) as conn:
            from graphmana.annotation.parsers.constraint import GeneConstraintParser

            parser = GeneConstraintParser(conn)
            result = parser.load(
                input_file,
                version,
                description=description,
                batch_size=batch_size,
            )
            click.echo(
                f"Constraint loaded: version={result['version']}, "
                f"{result['n_parsed']} parsed, {result['n_matched']} matched"
            )
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@annotate.command("load-bed")
@click.option(
    "--input",
    "input_file",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="BED file with genomic regions.",
)
@click.option("--version", required=True, help="Annotation version label.")
@click.option(
    "--region-type",
    default="regulatory",
    help="Type label for regions (e.g. 'enhancer', 'promoter', 'regulatory').",
)
@click.option(
    "--batch-size", type=int, default=DEFAULT_ANNOTATION_BATCH_SIZE, help="Regions per batch."
)
@click.option("--description", default="", help="Human-readable description.")
@click.option("--neo4j-uri", default=DEFAULT_NEO4J_URI, help="Neo4j Bolt URI.")
@click.option("--neo4j-user", default=DEFAULT_NEO4J_USER, help="Neo4j username.")
@click.option("--neo4j-password", default=DEFAULT_NEO4J_PASSWORD, help="Neo4j password.")
@click.option("--database", default=DEFAULT_DATABASE, help="Neo4j database name.")
@click.option("--verbose/--quiet", default=False, help="Verbose logging.")
def annotate_load_bed(
    input_file,
    version,
    region_type,
    batch_size,
    description,
    neo4j_uri,
    neo4j_user,
    neo4j_password,
    database,
    verbose,
):
    """Load BED regions and link overlapping variants."""
    _setup_logging(verbose)
    try:
        with GraphManaConnection(neo4j_uri, neo4j_user, neo4j_password, database=database) as conn:
            from graphmana.annotation.parsers.bed_region import BEDRegionParser

            parser = BEDRegionParser(conn, region_type=region_type)
            result = parser.load(
                input_file,
                version,
                description=description,
                batch_size=batch_size,
            )
            click.echo(
                f"BED regions loaded: version={result['version']}, "
                f"{result['n_parsed']} regions, {result['n_matched']} edges"
            )
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@annotate.command("load-go")
@click.option(
    "--input",
    "input_file",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="GO annotation file (GAF format).",
)
@click.option("--version", required=True, help="Annotation version label.")
@click.option(
    "--obo-file",
    type=click.Path(exists=True, dir_okay=False),
    default=None,
    help="OBO ontology file for GO term hierarchy (optional).",
)
@click.option(
    "--batch-size", type=int, default=DEFAULT_ANNOTATION_BATCH_SIZE, help="Records per batch."
)
@click.option("--description", default="", help="Human-readable description.")
@click.option("--neo4j-uri", default=DEFAULT_NEO4J_URI, help="Neo4j Bolt URI.")
@click.option("--neo4j-user", default=DEFAULT_NEO4J_USER, help="Neo4j username.")
@click.option("--neo4j-password", default=DEFAULT_NEO4J_PASSWORD, help="Neo4j password.")
@click.option("--database", default=DEFAULT_DATABASE, help="Neo4j database name.")
@click.option("--verbose/--quiet", default=False, help="Verbose logging.")
def annotate_load_go(
    input_file,
    version,
    obo_file,
    batch_size,
    description,
    neo4j_uri,
    neo4j_user,
    neo4j_password,
    database,
    verbose,
):
    """Load GO term annotations from a GAF file."""
    _setup_logging(verbose)
    try:
        with GraphManaConnection(neo4j_uri, neo4j_user, neo4j_password, database=database) as conn:
            from graphmana.annotation.parsers.go_pathway import GOParser

            parser = GOParser(conn, obo_path=obo_file)
            result = parser.load(
                input_file,
                version,
                description=description,
                batch_size=batch_size,
            )
            msg = (
                f"GO loaded: version={result['version']}, "
                f"{result['n_parsed']} annotations, {result['n_matched']} edges"
            )
            if "n_hierarchy_edges" in result:
                msg += f", {result['n_hierarchy_edges']} IS_A edges"
            click.echo(msg)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@annotate.command("load-pathway")
@click.option(
    "--input",
    "input_file",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Pathway TSV file (gene_symbol, pathway_id, pathway_name, source).",
)
@click.option("--version", required=True, help="Annotation version label.")
@click.option("--source", "pathway_source", default="KEGG", help="Pathway database source.")
@click.option(
    "--batch-size", type=int, default=DEFAULT_ANNOTATION_BATCH_SIZE, help="Records per batch."
)
@click.option("--description", default="", help="Human-readable description.")
@click.option("--neo4j-uri", default=DEFAULT_NEO4J_URI, help="Neo4j Bolt URI.")
@click.option("--neo4j-user", default=DEFAULT_NEO4J_USER, help="Neo4j username.")
@click.option("--neo4j-password", default=DEFAULT_NEO4J_PASSWORD, help="Neo4j password.")
@click.option("--database", default=DEFAULT_DATABASE, help="Neo4j database name.")
@click.option("--verbose/--quiet", default=False, help="Verbose logging.")
def annotate_load_pathway(
    input_file,
    version,
    pathway_source,
    batch_size,
    description,
    neo4j_uri,
    neo4j_user,
    neo4j_password,
    database,
    verbose,
):
    """Load pathway annotations from a TSV file."""
    _setup_logging(verbose)
    try:
        with GraphManaConnection(neo4j_uri, neo4j_user, neo4j_password, database=database) as conn:
            from graphmana.annotation.parsers.go_pathway import PathwayParser

            parser = PathwayParser(conn, pathway_source=pathway_source)
            result = parser.load(
                input_file,
                version,
                description=description,
                batch_size=batch_size,
            )
            click.echo(
                f"Pathways loaded: version={result['version']}, "
                f"{result['n_parsed']} parsed, {result['n_matched']} edges"
            )
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@annotate.command("load-clinvar")
@click.option(
    "--input",
    "input_file",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="ClinVar VCF file.",
)
@click.option("--version", required=True, help="Annotation version label (e.g. 'ClinVar_2024-01').")
@click.option("--chr-prefix", default="", help="Chromosome prefix (e.g. 'chr').")
@click.option(
    "--batch-size", type=int, default=DEFAULT_ANNOTATION_BATCH_SIZE, help="Records per batch."
)
@click.option("--description", default="", help="Human-readable description.")
@click.option("--neo4j-uri", default=DEFAULT_NEO4J_URI, help="Neo4j Bolt URI.")
@click.option("--neo4j-user", default=DEFAULT_NEO4J_USER, help="Neo4j username.")
@click.option("--neo4j-password", default=DEFAULT_NEO4J_PASSWORD, help="Neo4j password.")
@click.option("--database", default=DEFAULT_DATABASE, help="Neo4j database name.")
@click.option("--verbose/--quiet", default=False, help="Verbose logging.")
def annotate_load_clinvar(
    input_file,
    version,
    chr_prefix,
    batch_size,
    description,
    neo4j_uri,
    neo4j_user,
    neo4j_password,
    database,
    verbose,
):
    """Load ClinVar annotations from a VCF file."""
    _setup_logging(verbose)
    try:
        with GraphManaConnection(neo4j_uri, neo4j_user, neo4j_password, database=database) as conn:
            from graphmana.annotation.parsers.clinvar import ClinVarParser

            parser = ClinVarParser(conn)
            result = parser.load(
                input_file,
                version,
                description=description,
                batch_size=batch_size,
                chr_prefix=chr_prefix,
            )
            click.echo(
                f"ClinVar loaded: version={result['version']}, "
                f"{result['n_parsed']} parsed, {result['n_matched']} matched"
            )
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


# ---------------------------------------------------------------------------
# snapshot
# ---------------------------------------------------------------------------


@cli.group()
def snapshot():
    """Manage database snapshots (create, list, restore, delete)."""


@snapshot.command("create")
@click.option("--name", required=True, help="Snapshot name (unique).")
@click.option(
    "--neo4j-home",
    required=True,
    type=click.Path(exists=True, file_okay=False),
    help="Neo4j installation directory.",
)
@click.option("--database", default=DEFAULT_DATABASE, help="Neo4j database name.")
@click.option(
    "--snapshot-dir",
    type=click.Path(file_okay=False),
    default=DEFAULT_SNAPSHOT_DIR,
    help="Directory to store snapshots.",
)
def snapshot_create(name, neo4j_home, database, snapshot_dir):
    """Create a database snapshot via neo4j-admin dump."""
    try:
        from graphmana.snapshot.manager import SnapshotManager

        mgr = SnapshotManager(snapshot_dir)
        result = mgr.create(name, neo4j_home=neo4j_home, database=database)
        size_mb = result["size_bytes"] / (1024 * 1024)
        click.echo(f"Snapshot created: {result['name']} ({size_mb:.1f} MB)")
        click.echo(f"Path: {result['path']}")
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@snapshot.command("list")
@click.option(
    "--snapshot-dir",
    type=click.Path(file_okay=False),
    default=DEFAULT_SNAPSHOT_DIR,
    help="Directory containing snapshots.",
)
def snapshot_list(snapshot_dir):
    """List all snapshots."""
    try:
        from graphmana.snapshot.manager import SnapshotManager

        mgr = SnapshotManager(snapshot_dir)
        snapshots = mgr.list()
        if not snapshots:
            click.echo("No snapshots found.")
            return
        for s in snapshots:
            size_mb = s["size_bytes"] / (1024 * 1024)
            click.echo(f"  {s['name']:30s}  {size_mb:>8.1f} MB  {s['modified_date']}")
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@snapshot.command("restore")
@click.option("--name", required=True, help="Snapshot name to restore.")
@click.option(
    "--neo4j-home",
    required=True,
    type=click.Path(exists=True, file_okay=False),
    help="Neo4j installation directory.",
)
@click.option("--database", default=DEFAULT_DATABASE, help="Neo4j database name.")
@click.option(
    "--snapshot-dir",
    type=click.Path(file_okay=False),
    default=DEFAULT_SNAPSHOT_DIR,
    help="Directory containing snapshots.",
)
def snapshot_restore(name, neo4j_home, database, snapshot_dir):
    """Restore a database from a snapshot. Neo4j must be stopped."""
    try:
        from graphmana.snapshot.manager import SnapshotManager

        mgr = SnapshotManager(snapshot_dir)
        result = mgr.restore(name, neo4j_home=neo4j_home, database=database)
        click.echo(f"Snapshot restored: {result['name']} -> database {result['database']}")
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@snapshot.command("delete")
@click.option("--name", required=True, help="Snapshot name to delete.")
@click.option(
    "--snapshot-dir",
    type=click.Path(file_okay=False),
    default=DEFAULT_SNAPSHOT_DIR,
    help="Directory containing snapshots.",
)
def snapshot_delete(name, snapshot_dir):
    """Delete a snapshot."""
    try:
        from graphmana.snapshot.manager import SnapshotManager

        mgr = SnapshotManager(snapshot_dir)
        deleted = mgr.delete(name)
        if deleted:
            click.echo(f"Snapshot deleted: {name}")
        else:
            click.echo(f"Snapshot not found: {name}", err=True)
            sys.exit(1)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


# ---------------------------------------------------------------------------
# liftover
# ---------------------------------------------------------------------------


@cli.command()
@click.option(
    "--chain",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="UCSC chain file for coordinate conversion (.chain or .chain.gz).",
)
@click.option(
    "--target-reference",
    required=True,
    help="Target reference genome name (e.g. GRCh38).",
)
@click.option(
    "--reject-file",
    type=click.Path(dir_okay=False),
    default=None,
    help="Write unmapped/ambiguous variants to this TSV file.",
)
@click.option(
    "--update-annotations",
    is_flag=True,
    help="Attempt to liftover Gene coordinates (currently a no-op).",
)
@click.option("--backup-before", is_flag=True, help="Create a database snapshot before liftover.")
@click.option(
    "--neo4j-home",
    type=click.Path(exists=True, file_okay=False),
    default=None,
    help="Neo4j installation directory (required for --backup-before).",
)
@click.option(
    "--snapshot-dir",
    type=click.Path(file_okay=False),
    default=DEFAULT_SNAPSHOT_DIR,
    help="Directory for snapshot storage.",
)
@click.option("--neo4j-uri", default=DEFAULT_NEO4J_URI, help="Neo4j Bolt URI.")
@click.option("--neo4j-user", default=DEFAULT_NEO4J_USER, help="Neo4j username.")
@click.option("--neo4j-password", default=DEFAULT_NEO4J_PASSWORD, help="Neo4j password.")
@click.option("--database", default=DEFAULT_DATABASE, help="Neo4j database name.")
@click.option("--batch-size", default=500, type=int, help="Variants per database write batch.")
@click.option("--dry-run", is_flag=True, help="Compute mappings without modifying the database.")
@click.option("--verbose/--quiet", default=False, help="Verbose logging.")
def liftover(
    chain,
    target_reference,
    reject_file,
    update_annotations,
    backup_before,
    neo4j_home,
    snapshot_dir,
    neo4j_uri,
    neo4j_user,
    neo4j_password,
    database,
    batch_size,
    dry_run,
    verbose,
):
    """Convert variant coordinates between reference genome assemblies."""
    _setup_logging(verbose)

    try:
        # Optional backup before liftover
        if backup_before:
            if not neo4j_home:
                click.echo("Error: --neo4j-home is required for --backup-before.", err=True)
                sys.exit(1)
            from graphmana.snapshot.manager import SnapshotManager

            snap_name = f"pre_liftover_{target_reference}"
            mgr = SnapshotManager(snapshot_dir)
            click.echo(f"Creating backup snapshot: {snap_name} ...")
            mgr.create(snap_name, neo4j_home=neo4j_home)
            click.echo("Backup complete.")

        from graphmana.liftover.chain_parser import LiftoverConverter
        from graphmana.liftover.lifter import GraphLiftover

        converter = LiftoverConverter(chain)

        with GraphManaConnection(neo4j_uri, neo4j_user, neo4j_password, database=database) as conn:
            lifter = GraphLiftover(conn, converter, target_reference)
            result = lifter.run(
                dry_run=dry_run,
                reject_file=reject_file,
                update_annotations=update_annotations,
                batch_size=batch_size,
            )

        click.echo()
        if dry_run:
            click.echo("DRY RUN — no database changes made.")
        click.echo(f"Target reference: {result['target_reference']}")
        click.echo(f"Total variants:   {result['total_variants']:,}")
        click.echo(f"Mapped:           {result['mapped']:,}")
        click.echo(f"Unmapped:         {result['unmapped']:,}")
        click.echo(f"Ambiguous:        {result['ambiguous']:,}")
        click.echo(f"Collisions:       {result['collision']:,}")
        if reject_file:
            click.echo(f"Reject file:      {reject_file}")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


# ---------------------------------------------------------------------------
# migrate
# ---------------------------------------------------------------------------


@cli.command()
@click.option("--neo4j-uri", default=DEFAULT_NEO4J_URI, help="Neo4j Bolt URI.")
@click.option("--neo4j-user", default=DEFAULT_NEO4J_USER, help="Neo4j username.")
@click.option("--neo4j-password", default=DEFAULT_NEO4J_PASSWORD, help="Neo4j password.")
@click.option("--database", default=DEFAULT_DATABASE, help="Neo4j database name.")
@click.option("--dry-run", is_flag=True, help="Show pending migrations without executing.")
@click.option("--verbose/--quiet", default=False, help="Verbose logging.")
def migrate(neo4j_uri, neo4j_user, neo4j_password, database, dry_run, verbose):
    """Apply pending schema migrations to the database."""
    _setup_logging(verbose)

    try:
        from graphmana.migration.manager import MigrationManager

        with GraphManaConnection(neo4j_uri, neo4j_user, neo4j_password, database=database) as conn:
            mgr = MigrationManager(conn)
            result = mgr.run(dry_run=dry_run)

        if result["migrations_applied"] == 0:
            click.echo(f"Schema is up to date (version {result['from_version']}).")
            return

        if dry_run:
            click.echo("DRY RUN — no database changes made.")
        click.echo(f"From version:        {result['from_version']}")
        click.echo(f"To version:          {result['to_version']}")
        click.echo(f"Migrations applied:  {result['migrations_applied']}")
        for m in result["migrations"]:
            click.echo(f"  {m['from']} -> {m['to']}: {m['description']}")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


# ---------------------------------------------------------------------------
# merge
# ---------------------------------------------------------------------------


@cli.command()
@click.option(
    "--source-uri",
    required=True,
    help="Source database Bolt URI (e.g. bolt://localhost:7688).",
)
@click.option("--source-user", default=DEFAULT_NEO4J_USER, help="Source Neo4j username.")
@click.option("--source-password", default=DEFAULT_NEO4J_PASSWORD, help="Source Neo4j password.")
@click.option("--source-database", default=DEFAULT_DATABASE, help="Source Neo4j database name.")
@click.option("--neo4j-uri", default=DEFAULT_NEO4J_URI, help="Target database Bolt URI.")
@click.option("--neo4j-user", default=DEFAULT_NEO4J_USER, help="Target Neo4j username.")
@click.option("--neo4j-password", default=DEFAULT_NEO4J_PASSWORD, help="Target Neo4j password.")
@click.option("--database", default=DEFAULT_DATABASE, help="Target Neo4j database name.")
@click.option(
    "--on-duplicate-sample",
    type=click.Choice(["error", "skip"]),
    default="error",
    help="How to handle sample IDs present in both databases.",
)
@click.option("--batch-size", type=int, default=500, help="Variants per transaction batch.")
@click.option("--dry-run", is_flag=True, help="Validate only, don't modify target.")
@click.option("--auto-start-neo4j", is_flag=True, help="Auto start/stop Neo4j around merge.")
@click.option(
    "--neo4j-home",
    "merge_neo4j_home",
    type=click.Path(exists=True, file_okay=False),
    default=None,
    help="Neo4j installation directory (required with --auto-start-neo4j).",
)
@click.option(
    "--neo4j-data-dir",
    "merge_neo4j_data_dir",
    type=click.Path(file_okay=False),
    default=None,
    help="Neo4j data directory (required with --auto-start-neo4j).",
)
@click.option("--verbose/--quiet", default=False, help="Enable verbose logging.")
def merge(
    source_uri,
    source_user,
    source_password,
    source_database,
    neo4j_uri,
    neo4j_user,
    neo4j_password,
    database,
    on_duplicate_sample,
    batch_size,
    dry_run,
    auto_start_neo4j,
    merge_neo4j_home,
    merge_neo4j_data_dir,
    verbose,
):
    """Merge a source GraphMana database into the target database."""
    from graphmana.merge.merger import DatabaseMerger, MergeValidationError

    _setup_logging(verbose)

    try:
        with _auto_neo4j_lifecycle(auto_start_neo4j, merge_neo4j_home, merge_neo4j_data_dir):
            with GraphManaConnection(
                source_uri, source_user, source_password, database=source_database
            ) as source_conn:
                with GraphManaConnection(
                    neo4j_uri, neo4j_user, neo4j_password, database=database
                ) as target_conn:
                    merger = DatabaseMerger(
                        source_conn=source_conn,
                        target_conn=target_conn,
                        on_duplicate_sample=on_duplicate_sample,
                        dry_run=dry_run,
                    )
                    result = merger.run(batch_size=batch_size)

        click.echo()
        if dry_run:
            click.echo("Dry run complete — no modifications made.")
        else:
            click.echo("Merge complete.")
        click.echo(f"  Variants extended:        {result['n_variants_extended']:>10,}")
        click.echo(f"  Variants homref-extended: {result['n_variants_homref_extended']:>10,}")
        click.echo(f"  Variants created:         {result['n_variants_created']:>10,}")
        click.echo(f"  Samples merged:           {result['n_samples_merged']:>10,}")
        click.echo(f"  Populations created:      {result['n_populations_created']:>10,}")
        click.echo(f"  Chromosomes processed:    {result['n_chromosomes_processed']:>10,}")
        if result["n_skipped_samples"] > 0:
            click.echo(f"  Samples skipped (dupes):  {result['n_skipped_samples']:>10,}")

    except MergeValidationError as e:
        click.echo(f"Validation error: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


# ---------------------------------------------------------------------------
# provenance
# ---------------------------------------------------------------------------


@cli.group()
def provenance():
    """Query import provenance and audit trail."""


@provenance.command("list")
@click.option("--neo4j-uri", default=DEFAULT_NEO4J_URI, help="Neo4j Bolt URI.")
@click.option("--neo4j-user", default=DEFAULT_NEO4J_USER, help="Neo4j username.")
@click.option("--neo4j-password", default=DEFAULT_NEO4J_PASSWORD, help="Neo4j password.")
@click.option("--database", default=DEFAULT_DATABASE, help="Neo4j database name.")
@click.option("--json", "output_json", is_flag=True, help="Output as JSON.")
def provenance_list(neo4j_uri, neo4j_user, neo4j_password, database, output_json):
    """List all ingestion logs."""
    try:
        with GraphManaConnection(neo4j_uri, neo4j_user, neo4j_password, database=database) as conn:
            from graphmana.provenance.manager import ProvenanceManager

            mgr = ProvenanceManager(conn)
            logs = mgr.list_ingestions()
            if output_json:
                click.echo(json.dumps(logs, indent=2, default=str))
                return
            if not logs:
                click.echo("No ingestion logs found.")
                return
            for log in logs:
                click.echo(
                    f"  {log.get('log_id', '?')}  "
                    f"{log.get('mode', '?')}  "
                    f"{log.get('n_samples', 0)} samples  "
                    f"{log.get('n_variants', 0)} variants  "
                    f"{log.get('import_date', '?')}"
                )
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@provenance.command("show")
@click.argument("log_id")
@click.option("--neo4j-uri", default=DEFAULT_NEO4J_URI, help="Neo4j Bolt URI.")
@click.option("--neo4j-user", default=DEFAULT_NEO4J_USER, help="Neo4j username.")
@click.option("--neo4j-password", default=DEFAULT_NEO4J_PASSWORD, help="Neo4j password.")
@click.option("--database", default=DEFAULT_DATABASE, help="Neo4j database name.")
@click.option("--json", "output_json", is_flag=True, help="Output as JSON.")
def provenance_show(log_id, neo4j_uri, neo4j_user, neo4j_password, database, output_json):
    """Show details of a single ingestion log."""
    try:
        with GraphManaConnection(neo4j_uri, neo4j_user, neo4j_password, database=database) as conn:
            from graphmana.provenance.manager import ProvenanceManager

            mgr = ProvenanceManager(conn)
            log = mgr.get_ingestion(log_id)
            if log is None:
                click.echo(f"Ingestion log not found: {log_id}", err=True)
                sys.exit(1)
            if output_json:
                click.echo(json.dumps(log, indent=2, default=str))
                return
            for key, value in log.items():
                click.echo(f"  {key}: {value}")
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@provenance.command("headers")
@click.option("--neo4j-uri", default=DEFAULT_NEO4J_URI, help="Neo4j Bolt URI.")
@click.option("--neo4j-user", default=DEFAULT_NEO4J_USER, help="Neo4j username.")
@click.option("--neo4j-password", default=DEFAULT_NEO4J_PASSWORD, help="Neo4j password.")
@click.option("--database", default=DEFAULT_DATABASE, help="Neo4j database name.")
@click.option("--json", "output_json", is_flag=True, help="Output as JSON.")
def provenance_headers(neo4j_uri, neo4j_user, neo4j_password, database, output_json):
    """List all VCF header records."""
    try:
        with GraphManaConnection(neo4j_uri, neo4j_user, neo4j_password, database=database) as conn:
            from graphmana.provenance.manager import ProvenanceManager

            mgr = ProvenanceManager(conn)
            headers = mgr.list_vcf_headers()
            if output_json:
                click.echo(json.dumps(headers, indent=2, default=str))
                return
            if not headers:
                click.echo("No VCF headers found.")
                return
            for h in headers:
                click.echo(
                    f"  {h.get('dataset_id', '?')}  "
                    f"{h.get('source_file', '?')}  "
                    f"{h.get('import_date', '?')}"
                )
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@provenance.command("summary")
@click.option("--neo4j-uri", default=DEFAULT_NEO4J_URI, help="Neo4j Bolt URI.")
@click.option("--neo4j-user", default=DEFAULT_NEO4J_USER, help="Neo4j username.")
@click.option("--neo4j-password", default=DEFAULT_NEO4J_PASSWORD, help="Neo4j password.")
@click.option("--database", default=DEFAULT_DATABASE, help="Neo4j database name.")
@click.option("--json", "output_json", is_flag=True, help="Output as JSON.")
def provenance_summary(neo4j_uri, neo4j_user, neo4j_password, database, output_json):
    """Show aggregate provenance summary."""
    try:
        with GraphManaConnection(neo4j_uri, neo4j_user, neo4j_password, database=database) as conn:
            from graphmana.provenance.manager import ProvenanceManager

            mgr = ProvenanceManager(conn)
            s = mgr.summary()
            if output_json:
                click.echo(json.dumps(s, indent=2, default=str))
                return
            click.echo(f"  Total ingestions:   {s['n_ingestions']}")
            click.echo(f"  Total samples:      {s['total_samples_imported']}")
            click.echo(f"  Total variants:     {s['total_variants_imported']}")
            click.echo(f"  First import:       {s['first_import'] or 'N/A'}")
            click.echo(f"  Last import:        {s['last_import'] or 'N/A'}")
            click.echo(f"  Source files:        {len(s['source_files'])}")
            for f in s["source_files"]:
                click.echo(f"    - {f}")
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


# ---------------------------------------------------------------------------
# cluster: setup-neo4j, neo4j-start, neo4j-stop, check-filesystem
# ---------------------------------------------------------------------------


@cli.command("setup-neo4j")
@click.option(
    "--install-dir",
    required=True,
    type=click.Path(file_okay=False),
    help="Directory to install Neo4j into.",
)
@click.option(
    "--version",
    "neo4j_version",
    default="5.26.2",
    help="Neo4j Community version to download.",
)
@click.option(
    "--data-dir",
    type=click.Path(file_okay=False),
    default=None,
    help="Custom data directory (use local SSD/scratch on clusters).",
)
@click.option(
    "--memory-auto",
    is_flag=True,
    help="Auto-set heap and page cache based on available RAM.",
)
@click.option("--verbose/--quiet", default=False, help="Verbose logging.")
def setup_neo4j(install_dir, neo4j_version, data_dir, memory_auto, verbose):
    """Download and configure Neo4j for user-space operation."""
    _setup_logging(verbose)

    try:
        from graphmana.cluster.neo4j_lifecycle import setup_neo4j as _setup

        result = _setup(
            install_dir,
            version=neo4j_version,
            data_dir=data_dir,
            memory_auto=memory_auto,
        )

        click.echo(f"Neo4j {result['version']} installed.")
        click.echo(f"  Home:         {result['neo4j_home']}")
        click.echo(f"  Data dir:     {result['data_dir']}")
        click.echo(f"  Java:         {result['java_version']}")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command("neo4j-start")
@click.option(
    "--neo4j-home",
    required=True,
    type=click.Path(exists=True, file_okay=False),
    help="Neo4j installation directory.",
)
@click.option(
    "--data-dir",
    type=click.Path(file_okay=False),
    default=None,
    help="Override data directory.",
)
@click.option("--wait/--no-wait", default=True, help="Wait until Neo4j is ready.")
@click.option("--timeout", default=120, type=int, help="Max seconds to wait.")
@click.option("--verbose/--quiet", default=False, help="Verbose logging.")
def neo4j_start(neo4j_home, data_dir, wait, timeout, verbose):
    """Start Neo4j in user space."""
    _setup_logging(verbose)

    try:
        from graphmana.cluster.neo4j_lifecycle import start_neo4j

        result = start_neo4j(neo4j_home, data_dir=data_dir, wait=wait, timeout=timeout)

        click.echo(f"Neo4j status: {result['status']}")
        if result["pid"]:
            click.echo(f"  PID:  {result['pid']}")
        click.echo(f"  Bolt: localhost:{result['bolt_port']}")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command("neo4j-stop")
@click.option(
    "--neo4j-home",
    required=True,
    type=click.Path(exists=True, file_okay=False),
    help="Neo4j installation directory.",
)
@click.option("--verbose/--quiet", default=False, help="Verbose logging.")
def neo4j_stop(neo4j_home, verbose):
    """Stop a running Neo4j instance."""
    _setup_logging(verbose)

    try:
        from graphmana.cluster.neo4j_lifecycle import stop_neo4j

        result = stop_neo4j(neo4j_home)
        click.echo(f"Neo4j {result['status']}.")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command("check-filesystem")
@click.option(
    "--neo4j-data-dir",
    required=True,
    type=click.Path(),
    help="Neo4j data directory to check.",
)
def check_filesystem(neo4j_data_dir):
    """Check if Neo4j data directory is on suitable storage."""
    from graphmana.cluster.filesystem_check import check_neo4j_data_dir

    result = check_neo4j_data_dir(neo4j_data_dir)

    click.echo(f"Path:       {result['path']}")
    click.echo(f"Filesystem: {result['fs_type']}")

    if result["is_network"]:
        click.echo()
        click.echo(f"WARNING: {result['warning']}", err=True)
        sys.exit(1)
    else:
        click.echo("Status:     OK (local storage)")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@contextmanager
def _auto_neo4j_lifecycle(auto_start, neo4j_home, neo4j_data_dir=None):
    """Context manager that starts/stops Neo4j when --auto-start-neo4j is set."""
    if not auto_start:
        yield
        return
    if not neo4j_home:
        raise click.UsageError("--neo4j-home is required when using --auto-start-neo4j")
    from graphmana.cluster.neo4j_lifecycle import start_neo4j, stop_neo4j

    click.echo(f"Starting Neo4j at {neo4j_home} ...")
    start_neo4j(neo4j_home, data_dir=neo4j_data_dir, wait=True)
    click.echo("Neo4j ready.")
    try:
        yield
    finally:
        click.echo("Stopping Neo4j ...")
        stop_neo4j(neo4j_home)
        click.echo("Neo4j stopped.")


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )


def _build_filter_config(
    min_qual,
    min_call_rate,
    maf_min,
    maf_max,
    variant_types,
    *,
    region=None,
    contigs=None,
):
    """Build an ImportFilterConfig from CLI options. Returns None if no filters set."""
    from graphmana.filtering.import_filters import ImportFilterConfig

    has_any = any(
        [
            min_qual is not None,
            min_call_rate is not None,
            maf_min is not None,
            maf_max is not None,
            variant_types,
            region is not None,
            contigs,
        ]
    )
    if not has_any:
        return None

    return ImportFilterConfig(
        min_qual=min_qual,
        min_call_rate=min_call_rate,
        maf_min=maf_min,
        maf_max=maf_max,
        variant_types=set(variant_types) if variant_types else None,
        region=region,
        contigs=contigs,
    )
