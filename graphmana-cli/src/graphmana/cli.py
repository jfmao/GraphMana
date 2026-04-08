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
# version: detailed version info
# ---------------------------------------------------------------------------


@cli.command("version")
def version_info():
    """Show detailed version information for GraphMana and its dependencies."""
    import platform
    import shutil
    import subprocess

    from graphmana.config import GRAPHMANA_VERSION, SCHEMA_VERSION

    click.echo(f"GraphMana:      {GRAPHMANA_VERSION}")
    click.echo(f"Schema version: {SCHEMA_VERSION}")
    click.echo(f"Python:         {platform.python_version()}")

    # cyvcf2
    try:
        import cyvcf2

        click.echo(f"cyvcf2:         {cyvcf2.__version__}")
    except ImportError:
        click.echo("cyvcf2:         not installed")

    # numpy
    try:
        import numpy

        click.echo(f"NumPy:          {numpy.__version__}")
    except ImportError:
        click.echo("NumPy:          not installed")

    # Java
    java_path = shutil.which("java")
    if java_path:
        try:
            result = subprocess.run(
                ["java", "-version"], capture_output=True, text=True, timeout=5
            )
            java_ver = (result.stderr or result.stdout).split("\n")[0]
            click.echo(f"Java:           {java_ver}")
        except Exception:
            click.echo(f"Java:           found at {java_path}")
    else:
        click.echo("Java:           not found")

    # Neo4j
    neo4j_path = shutil.which("neo4j")
    if neo4j_path:
        try:
            result = subprocess.run(
                ["neo4j", "version"], capture_output=True, text=True, timeout=5
            )
            click.echo(f"Neo4j:          {result.stdout.strip()}")
        except Exception:
            click.echo(f"Neo4j:          found at {neo4j_path}")
    else:
        click.echo("Neo4j:          not found on PATH")

    # bcftools
    bcf_path = shutil.which("bcftools")
    if bcf_path:
        try:
            result = subprocess.run(
                ["bcftools", "--version"], capture_output=True, text=True, timeout=5
            )
            click.echo(f"bcftools:       {result.stdout.split(chr(10))[0]}")
        except Exception:
            click.echo(f"bcftools:       found at {bcf_path}")
    else:
        click.echo("bcftools:       not found on PATH")


# ---------------------------------------------------------------------------
# list-formats: show export format reference
# ---------------------------------------------------------------------------

_EXPORT_FORMATS = [
    ("treemix", "FAST", "TreeMix allele count matrix (gzipped)"),
    ("sfs-dadi", "FAST", "dadi site frequency spectrum (.fs)"),
    ("sfs-fsc", "FAST", "fastsimcoal2 SFS (.obs)"),
    ("bed", "FAST", "BED variant positions for bedtools/IGV"),
    ("tsv", "FAST", "Tab-separated variant table"),
    ("json", "FAST", "JSON Lines variant records"),
    ("vcf", "FULL", "VCF/BCF genotype calls"),
    ("plink", "FULL", "PLINK 1.9 binary (.bed/.bim/.fam)"),
    ("plink2", "FULL", "PLINK 2.0 binary (.pgen/.pvar/.psam)"),
    ("eigenstrat", "FULL", "EIGENSTRAT for smartPCA/AdmixTools (.geno/.snp/.ind)"),
    ("beagle", "FULL", "Beagle phasing/imputation input"),
    ("structure", "FULL", "STRUCTURE population assignment"),
    ("genepop", "FULL", "Genepop conservation genetics format"),
    ("hap", "FULL", "Haplotype for selscan (.hap/.map)"),
    ("bgen", "FULL", "BGEN probabilistic genotypes"),
    ("gds", "FULL", "SeqArray/R HDF5-based format"),
    ("zarr", "FULL", "Zarr chunked arrays for sgkit/Python"),
]


@cli.command("list-formats")
def list_formats():
    """List all available export formats with access path and description."""
    click.echo(f"{'Format':<14s} {'Path':<6s} Description")
    click.echo(f"{'------':<14s} {'----':<6s} -----------")
    for name, path, desc in _EXPORT_FORMATS:
        click.echo(f"{name:<14s} {path:<6s} {desc}")
    click.echo(f"\nTotal: {len(_EXPORT_FORMATS)} formats")
    click.echo("\nFAST PATH: reads pre-computed population arrays (constant time in N samples)")
    click.echo("FULL PATH: unpacks per-sample genotypes (linear time in N samples)")


# ---------------------------------------------------------------------------
# config-show: display current configuration
# ---------------------------------------------------------------------------


@cli.command("config-show")
def config_show():
    """Display current configuration defaults and environment variable overrides."""
    import os

    from graphmana.config import (
        DEFAULT_BATCH_SIZE,
        DEFAULT_DATABASE,
        DEFAULT_NEO4J_PASSWORD,
        DEFAULT_NEO4J_URI,
        DEFAULT_NEO4J_USER,
        DEFAULT_THREADS,
        GRAPHMANA_VERSION,
        SCHEMA_VERSION,
    )

    click.echo("GraphMana Configuration")
    click.echo("=======================")
    click.echo()
    click.echo("Connection:")
    uri_env = os.environ.get("GRAPHMANA_NEO4J_URI")
    click.echo(f"  Neo4j URI:      {DEFAULT_NEO4J_URI}" + (f"  (from env)" if uri_env else ""))
    click.echo(f"  Neo4j user:     {DEFAULT_NEO4J_USER}")
    pw_env = os.environ.get("GRAPHMANA_NEO4J_PASSWORD")
    click.echo(f"  Neo4j password: {'(from env)' if pw_env else '(default)'}")
    click.echo(f"  Database:       {DEFAULT_DATABASE}")
    click.echo()
    click.echo("Processing:")
    click.echo(f"  Batch size:     {DEFAULT_BATCH_SIZE:,}")
    click.echo(f"  Threads:        {DEFAULT_THREADS}")
    click.echo()
    click.echo("Version:")
    click.echo(f"  GraphMana:      {GRAPHMANA_VERSION}")
    click.echo(f"  Schema:         {SCHEMA_VERSION}")
    click.echo()
    click.echo("Environment variables:")
    env_vars = [
        ("GRAPHMANA_NEO4J_PASSWORD", "Neo4j password"),
        ("GRAPHMANA_NEO4J_URI", "Neo4j Bolt URI"),
        ("NEO4J_HOME", "Neo4j installation directory"),
    ]
    for var, desc in env_vars:
        val = os.environ.get(var)
        status = f"= {val}" if val else "(not set)"
        click.echo(f"  {var:<30s} {status}")


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
    help="Include per-sample genotypes in JSON output. Requires unpacking packed arrays (FULL PATH).",
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
@click.option("--no-manifest", is_flag=True, default=False,
              help="Skip writing the .manifest.json sidecar file.")
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
    no_manifest,
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

            # Write manifest sidecar unless --no-manifest
            if not no_manifest and exporter is not None:
                manifest_path = exporter.write_manifest(Path(output), summary)
                click.echo(f"Manifest: {manifest_path}")

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


@provenance.command("search")
@click.option("--neo4j-uri", default=DEFAULT_NEO4J_URI)
@click.option("--neo4j-user", default=DEFAULT_NEO4J_USER)
@click.option("--neo4j-password", default=DEFAULT_NEO4J_PASSWORD)
@click.option("--database", default=DEFAULT_DATABASE, help="Neo4j database name.")
@click.option("--since", default=None, help="Start date (ISO format, e.g. 2026-03-01).")
@click.option("--until", default=None, help="End date (ISO format, e.g. 2026-03-31).")
@click.option("--dataset-id", default=None, help="Filter by dataset identifier.")
@click.option("--json", "output_json", is_flag=True, help="Output as JSON.")
def provenance_search(
    neo4j_uri, neo4j_user, neo4j_password, database, since, until, dataset_id, output_json
):
    """Search ingestion logs by date range or dataset ID."""
    try:
        with GraphManaConnection(neo4j_uri, neo4j_user, neo4j_password, database=database) as conn:
            from graphmana.provenance.manager import ProvenanceManager

            mgr = ProvenanceManager(conn)
            results = mgr.search(since=since, until=until, dataset_id=dataset_id)

            if output_json:
                click.echo(json.dumps(results, indent=2, default=str))
            else:
                if not results:
                    click.echo("No matching ingestion logs found.")
                    return
                click.echo(f"Found {len(results)} ingestion log(s):")
                click.echo()
                for log in results:
                    click.echo(f"  {log.get('log_id', 'unknown')}")
                    click.echo(f"    Date:     {log.get('import_date', 'unknown')}")
                    click.echo(f"    Mode:     {log.get('mode', 'unknown')}")
                    click.echo(f"    Source:   {log.get('source_file', 'unknown')}")
                    click.echo(f"    Dataset:  {log.get('dataset_id', 'unknown')}")
                    click.echo(f"    Samples:  {log.get('n_samples', 0)}")
                    click.echo(f"    Variants: {log.get('n_variants', 0)}")
                    click.echo()
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


# ---------------------------------------------------------------------------
# diff: compare database states
# ---------------------------------------------------------------------------


@cli.command("diff")
@click.option("--neo4j-uri", default=DEFAULT_NEO4J_URI)
@click.option("--neo4j-user", default=DEFAULT_NEO4J_USER)
@click.option("--neo4j-password", default=DEFAULT_NEO4J_PASSWORD)
@click.option("--database", default=DEFAULT_DATABASE, help="Neo4j database name.")
@click.option(
    "--snapshot",
    "snapshot_path",
    required=True,
    type=click.Path(exists=True),
    help="Path to a previously saved .summary.json file to compare against.",
)
@click.option("--save-current", type=click.Path(), default=None,
              help="Save current database state to this .summary.json file.")
def diff(neo4j_uri, neo4j_user, neo4j_password, database, snapshot_path, save_current):
    """Compare current database state against a saved snapshot summary."""
    from pathlib import Path

    from graphmana.snapshot.diff import capture_db_summary, diff_summaries, load_summary, save_summary

    try:
        with GraphManaConnection(neo4j_uri, neo4j_user, neo4j_password, database=database) as conn:
            current = capture_db_summary(conn)

        if save_current:
            save_summary(current, Path(save_current))
            click.echo(f"Current state saved to: {save_current}")

        previous = load_summary(Path(snapshot_path))
        lines = diff_summaries(previous, current, label_a="snapshot", label_b="current")

        for line in lines:
            click.echo(line)

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command("save-state")
@click.option("--neo4j-uri", default=DEFAULT_NEO4J_URI)
@click.option("--neo4j-user", default=DEFAULT_NEO4J_USER)
@click.option("--neo4j-password", default=DEFAULT_NEO4J_PASSWORD)
@click.option("--database", default=DEFAULT_DATABASE, help="Neo4j database name.")
@click.option("--output", required=True, type=click.Path(), help="Output .summary.json path.")
def save_state(neo4j_uri, neo4j_user, neo4j_password, database, output):
    """Save current database state summary for later comparison with diff."""
    from pathlib import Path

    from graphmana.snapshot.diff import capture_db_summary, save_summary

    try:
        with GraphManaConnection(neo4j_uri, neo4j_user, neo4j_password, database=database) as conn:
            summary = capture_db_summary(conn)
        save_summary(summary, Path(output))
        click.echo(f"Database state saved to: {output}")
        click.echo(f"  Variants:    {summary['n_variants']:,}")
        click.echo(f"  Samples:     {summary['n_active_samples']:,}")
        click.echo(f"  Populations: {summary['n_populations']:,}")
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


# ---------------------------------------------------------------------------
# ref-check: reference allele verification
# ---------------------------------------------------------------------------


@cli.command("ref-check")
@click.option("--fasta", required=True, type=click.Path(exists=True),
              help="Reference genome FASTA file (with .fai index recommended).")
@click.option("--output", type=click.Path(), default=None,
              help="Output TSV file for mismatches. Default: stdout.")
@click.option("--chromosomes", multiple=True, default=None,
              help="Limit check to these chromosomes.")
@click.option("--max-mismatches", type=int, default=0,
              help="Stop after N mismatches (0 = report all).")
@click.option("--neo4j-uri", default=DEFAULT_NEO4J_URI)
@click.option("--neo4j-user", default=DEFAULT_NEO4J_USER)
@click.option("--neo4j-password", default=DEFAULT_NEO4J_PASSWORD)
@click.option("--database", default=DEFAULT_DATABASE, help="Neo4j database name.")
def ref_check(
    fasta, output, chromosomes, max_mismatches,
    neo4j_uri, neo4j_user, neo4j_password, database,
):
    """Verify stored REF alleles against a FASTA reference genome."""
    from pathlib import Path

    from graphmana.qc.ref_check import check_ref_alleles

    chroms = list(chromosomes) if chromosomes else None

    try:
        with GraphManaConnection(neo4j_uri, neo4j_user, neo4j_password, database=database) as conn:
            result = check_ref_alleles(
                conn, Path(fasta), chromosomes=chroms, max_mismatches=max_mismatches,
            )

        click.echo(f"Checked:     {result['n_checked']:,} variants")
        click.echo(f"Matched:     {result['n_matched']:,}")
        click.echo(f"Mismatched:  {result['n_mismatched']:,}")
        if result.get("stopped_early"):
            click.echo(f"(stopped early after {max_mismatches} mismatches)")

        if result["mismatches"]:
            header = "variantId\tchr\tpos\tstored_ref\tgenome_ref"
            lines = [header]
            for m in result["mismatches"]:
                lines.append(
                    f"{m['variantId']}\t{m['chr']}\t{m['pos']}\t{m['stored_ref']}\t{m['genome_ref']}"
                )

            if output:
                Path(output).parent.mkdir(parents=True, exist_ok=True)
                with open(output, "w") as f:
                    f.write("\n".join(lines) + "\n")
                click.echo(f"Mismatches written to: {output}")
            else:
                click.echo()
                for line in lines:
                    click.echo(line)

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
@click.option(
    "--install-java",
    is_flag=True,
    help="Download Eclipse Temurin JDK 21 to user space (no admin needed).",
)
def setup_neo4j(install_dir, neo4j_version, data_dir, memory_auto, verbose, install_java):
    """Download and configure Neo4j for user-space operation.

    Automatically deploys the bundled GraphMana procedures JAR to the
    Neo4j plugins directory. Use --install-java to also download a JDK
    if Java 21+ is not already installed.
    """
    _setup_logging(verbose)

    try:
        # Optionally install Java first
        if install_java:
            from graphmana.cluster.neo4j_lifecycle import download_java

            java_bin = download_java(install_dir)
            # Add to PATH for the current process
            import os

            java_home = java_bin.parent.parent
            os.environ["JAVA_HOME"] = str(java_home)
            os.environ["PATH"] = f"{java_bin.parent}:{os.environ.get('PATH', '')}"
            click.echo(f"JDK 21 installed at: {java_home}")

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
        click.echo(f"  Procedures:   JAR deployed to plugins/")

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
# cluster: generate-job, check-env
# ---------------------------------------------------------------------------


@cli.group()
def cluster():
    """Cluster deployment helpers (SLURM/PBS job scripts, environment checks)."""


@cluster.command("generate-job")
@click.option(
    "--scheduler",
    type=click.Choice(["slurm", "pbs"]),
    default="slurm",
    help="Job scheduler type.",
)
@click.option(
    "--operation",
    type=click.Choice(["prepare-csv", "load-csv", "ingest", "export"]),
    required=True,
    help="GraphMana operation to generate a job script for.",
)
@click.option("--input", "input_files", multiple=True, help="Input VCF file(s).")
@click.option("--input-list", type=click.Path(), help="File listing VCF paths.")
@click.option("--population-map", type=click.Path(), help="Population map file.")
@click.option("--output-dir", type=click.Path(), help="Output directory for CSVs or exports.")
@click.option("--format", "export_format", help="Export format (for export operation).")
@click.option("--output", "export_output", help="Export output file (for export operation).")
@click.option("--reference", default="GRCh38", help="Reference genome. Default: GRCh38.")
@click.option("--cpus", default=16, type=int, help="CPUs to request. Default: 16.")
@click.option("--mem", default="64G", help="Memory to request. Default: 64G.")
@click.option("--time", "walltime", default="4:00:00", help="Walltime limit. Default: 4:00:00.")
@click.option("--neo4j-home", default="$HOME/neo4j", help="Neo4j installation directory.")
@click.option("--neo4j-data-dir", default="/scratch/$USER/graphmana_db", help="Neo4j data dir.")
@click.option("--neo4j-password", default=DEFAULT_NEO4J_PASSWORD, help="Neo4j password.")
@click.option("--database", default=DEFAULT_DATABASE, help="Neo4j database name.")
@click.option("--threads", default=None, type=int, help="GraphMana threads (defaults to --cpus).")
@click.option("--extra-args", default="", help="Additional arguments to pass to the command.")
@click.option(
    "--output-script",
    default=None,
    type=click.Path(),
    help="Write script to file instead of stdout.",
)
def cluster_generate_job(
    scheduler,
    operation,
    input_files,
    input_list,
    population_map,
    output_dir,
    export_format,
    export_output,
    reference,
    cpus,
    mem,
    walltime,
    neo4j_home,
    neo4j_data_dir,
    neo4j_password,
    database,
    threads,
    extra_args,
    output_script,
):
    """Generate a SLURM or PBS job script for a GraphMana operation."""
    threads = threads or cpus
    lines = []

    # Header
    if scheduler == "slurm":
        lines.append("#!/bin/bash")
        lines.append(f"#SBATCH --job-name=graphmana_{operation}")
        lines.append(f"#SBATCH --cpus-per-task={cpus}")
        lines.append(f"#SBATCH --mem={mem}")
        lines.append(f"#SBATCH --time={walltime}")
        lines.append(f"#SBATCH --output=graphmana_{operation}_%j.log")
    else:  # pbs
        lines.append("#!/bin/bash")
        lines.append(f"#PBS -N graphmana_{operation}")
        mem_num = mem.rstrip("GgMm")
        mem_unit = mem[-1:].lower() + "b" if mem[-1:].isalpha() else "gb"
        lines.append(f"#PBS -l select=1:ncpus={cpus}:mem={mem_num}{mem_unit}")
        lines.append(f"#PBS -l walltime={walltime}")
        lines.append(f"#PBS -o graphmana_{operation}.log")

    lines.append("")
    lines.append("# Load dependencies")
    lines.append("module load java/21 2>/dev/null || true")
    lines.append('source "${HOME}/miniforge3/bin/activate" graphmana 2>/dev/null || \\')
    lines.append('    source "${HOME}/graphmana-env/bin/activate" 2>/dev/null || true')
    if scheduler == "pbs":
        lines.append("cd $PBS_O_WORKDIR")
    lines.append("")
    lines.append(f"# GraphMana {operation}")

    # Build command
    neo4j_args = f"--neo4j-home {neo4j_home} --neo4j-data-dir {neo4j_data_dir}"
    conn_args = (
        f"--neo4j-password {neo4j_password} --database {database}"
    )

    if operation == "prepare-csv":
        input_args = " ".join(f"--input {f}" for f in input_files)
        if input_list:
            input_args += f" --input-list {input_list}"
        pop_arg = f"--population-map {population_map}" if population_map else ""
        out_arg = f"--output-dir {output_dir}" if output_dir else "--output-dir ./csv_out"
        cmd = (
            f"graphmana prepare-csv \\\n"
            f"    {input_args} \\\n"
            f"    {pop_arg} \\\n"
            f"    {out_arg} \\\n"
            f"    --reference {reference} \\\n"
            f"    --threads {threads} \\\n"
            f"    --verbose"
        )
    elif operation == "load-csv":
        csv_dir = output_dir or "./csv_out"
        cmd = (
            f"graphmana load-csv \\\n"
            f"    --csv-dir {csv_dir} \\\n"
            f"    {neo4j_args} \\\n"
            f"    --auto-start-neo4j \\\n"
            f"    {conn_args} \\\n"
            f"    --verbose"
        )
    elif operation == "ingest":
        input_args = " ".join(f"--input {f}" for f in input_files)
        if input_list:
            input_args += f" --input-list {input_list}"
        pop_arg = f"--population-map {population_map}" if population_map else ""
        cmd = (
            f"graphmana ingest \\\n"
            f"    {input_args} \\\n"
            f"    {pop_arg} \\\n"
            f"    {neo4j_args} \\\n"
            f"    --auto-start-neo4j \\\n"
            f"    {conn_args} \\\n"
            f"    --reference {reference} \\\n"
            f"    --threads {threads} \\\n"
            f"    --verbose"
        )
    elif operation == "export":
        fmt = export_format or "vcf"
        out = export_output or f"./export_output.{fmt}"
        cmd = (
            f"graphmana export \\\n"
            f"    --format {fmt} \\\n"
            f"    --output {out} \\\n"
            f"    {neo4j_args} \\\n"
            f"    --auto-start-neo4j \\\n"
            f"    {conn_args} \\\n"
            f"    --threads {threads} \\\n"
            f"    --verbose"
        )
    else:
        cmd = f"graphmana {operation}"

    if extra_args:
        cmd += f" \\\n    {extra_args}"

    lines.append(cmd)
    lines.append("")
    lines.append(f'echo "GraphMana {operation} complete: $(date)"')

    script = "\n".join(lines) + "\n"

    if output_script:
        from pathlib import Path

        Path(output_script).write_text(script)
        Path(output_script).chmod(0o755)
        click.echo(f"Job script written to: {output_script}")
        click.echo(f"Submit with: {'sbatch' if scheduler == 'slurm' else 'qsub'} {output_script}")
    else:
        click.echo(script)


@cluster.command("check-env")
def cluster_check_env():
    """Verify cluster environment: Java, conda, Neo4j, ports, filesystem."""
    import shutil
    import subprocess

    checks = []

    # Java
    java = shutil.which("java")
    if java:
        result = subprocess.run(["java", "-version"], capture_output=True, text=True)
        version_line = result.stderr.split("\n")[0] if result.stderr else "unknown"
        if "21" in version_line or "22" in version_line or "23" in version_line:
            checks.append(("Java 21+", "OK", version_line.strip()))
        else:
            checks.append(("Java 21+", "WARN", f"Found {version_line.strip()} — need 21+"))
    else:
        checks.append(("Java 21+", "FAIL", "Not found. Try: module load java/21"))

    # Python / graphmana
    try:
        from graphmana import __version__

        checks.append(("GraphMana CLI", "OK", f"v{__version__}"))
    except ImportError:
        checks.append(("GraphMana CLI", "FAIL", "Not installed"))

    # cyvcf2
    try:
        import cyvcf2

        checks.append(("cyvcf2", "OK", f"v{cyvcf2.__version__}"))
    except ImportError:
        checks.append(("cyvcf2", "FAIL", "Not installed. conda install -c bioconda cyvcf2"))

    # bcftools
    bcftools = shutil.which("bcftools")
    if bcftools:
        result = subprocess.run(["bcftools", "--version"], capture_output=True, text=True)
        checks.append(("bcftools", "OK", result.stdout.split("\n")[0].strip()))
    else:
        checks.append(("bcftools", "WARN", "Not found (optional, needed for --normalize)"))

    # Neo4j
    import os

    neo4j_home = os.environ.get("NEO4J_HOME", os.path.expanduser("~/neo4j"))
    neo4j_bin = os.path.join(neo4j_home, "bin", "neo4j-admin")
    if os.path.exists(neo4j_bin):
        result = subprocess.run(
            [os.path.join(neo4j_home, "bin", "neo4j"), "version"],
            capture_output=True, text=True,
        )
        checks.append(("Neo4j", "OK", f"{result.stdout.strip()} at {neo4j_home}"))
    else:
        checks.append(("Neo4j", "WARN", f"Not found at {neo4j_home}. Run: graphmana setup-neo4j"))

    # Port 7687
    import socket

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(1)
    port_open = sock.connect_ex(("localhost", 7687)) == 0
    sock.close()
    if port_open:
        checks.append(("Bolt port 7687", "OK", "Neo4j is running"))
    else:
        checks.append(("Bolt port 7687", "INFO", "Closed (Neo4j not running)"))

    # Disk space
    for path in ["/scratch", "/tmp", os.path.expanduser("~")]:
        if os.path.exists(path):
            stat = shutil.disk_usage(path)
            free_gb = stat.free / (1024**3)
            total_gb = stat.total / (1024**3)
            status = "OK" if free_gb > 100 else ("WARN" if free_gb > 20 else "FAIL")
            checks.append((f"Disk {path}", status, f"{free_gb:.0f} GB free / {total_gb:.0f} GB total"))

    # Print results
    click.echo("GraphMana Cluster Environment Check")
    click.echo("=" * 60)
    for name, status, detail in checks:
        icon = {"OK": "  OK ", "WARN": "WARN ", "FAIL": "FAIL ", "INFO": "INFO "}[status]
        click.echo(f"  [{icon}] {name}: {detail}")


# ---------------------------------------------------------------------------
# init: one-command project setup
# ---------------------------------------------------------------------------


@cli.command()
@click.option(
    "--project-dir",
    required=True,
    type=click.Path(),
    help="Directory for the new GraphMana project.",
)
@click.option(
    "--install-neo4j",
    is_flag=True,
    help="Download and install Neo4j into the project directory.",
)
@click.option(
    "--neo4j-password",
    default=DEFAULT_NEO4J_PASSWORD,
    help=f"Initial Neo4j password. Default: {DEFAULT_NEO4J_PASSWORD}.",
)
@click.option("--database", default=DEFAULT_DATABASE, help="Neo4j database name.")
def init(project_dir, install_neo4j, neo4j_password, database):
    """Initialize a new GraphMana project directory with standard structure."""
    import subprocess
    from pathlib import Path

    project = Path(project_dir)
    project.mkdir(parents=True, exist_ok=True)

    # Create directory structure
    dirs = ["data", "exports", "csv_out", "logs", "snapshots"]
    for d in dirs:
        (project / d).mkdir(exist_ok=True)

    # Write environment file
    neo4j_home = project / "neo4j" if install_neo4j else Path("~/neo4j").expanduser()
    env_file = project / "graphmana.env"
    env_file.write_text(
        f"# GraphMana project configuration\n"
        f"# Source this file: source {env_file}\n"
        f"export GRAPHMANA_PROJECT_DIR={project.resolve()}\n"
        f"export GRAPHMANA_NEO4J_HOME={neo4j_home}\n"
        f"export GRAPHMANA_NEO4J_DATA_DIR={project.resolve() / 'db'}\n"
        f"export GRAPHMANA_NEO4J_PASSWORD={neo4j_password}\n"
        f"export GRAPHMANA_DATABASE={database}\n"
        f"export GRAPHMANA_SNAPSHOT_DIR={project.resolve() / 'snapshots'}\n"
    )

    click.echo(f"Project initialized: {project.resolve()}")
    click.echo(f"  data/       — input VCF and population files")
    click.echo(f"  exports/    — export output files")
    click.echo(f"  csv_out/    — intermediate CSV files")
    click.echo(f"  logs/       — log files")
    click.echo(f"  snapshots/  — database snapshots")
    click.echo(f"  graphmana.env — project environment variables")

    if install_neo4j:
        click.echo("\nInstalling Neo4j...")
        try:
            from graphmana.cluster.neo4j_lifecycle import setup_neo4j as _setup

            result = _setup(
                str(project),
                data_dir=str(project / "db"),
                memory_auto=True,
            )
            # Set initial password
            neo4j_admin = Path(result["neo4j_home"]) / "bin" / "neo4j-admin"
            subprocess.run(
                [str(neo4j_admin), "dbms", "set-initial-password", neo4j_password],
                capture_output=True, text=True,
            )
            click.echo(f"  Neo4j {result['version']} installed at {result['neo4j_home']}")
            click.echo(f"  Password set to: {neo4j_password}")
        except Exception as e:
            click.echo(f"  Neo4j installation failed: {e}", err=True)
            click.echo("  You can install manually: graphmana setup-neo4j --install-dir ...")

    click.echo(f"\nQuick start:")
    click.echo(f"  source {env_file}")
    click.echo(f"  graphmana prepare-csv --input data/your.vcf.gz --population-map data/pops.tsv \\")
    click.echo(f"      --output-dir csv_out --reference GRCh38 --threads 8")
    click.echo(f"  graphmana load-csv --csv-dir csv_out --neo4j-home $GRAPHMANA_NEO4J_HOME \\")
    click.echo(f"      --auto-start-neo4j --neo4j-password $GRAPHMANA_NEO4J_PASSWORD")
    click.echo(f"  graphmana status --neo4j-password $GRAPHMANA_NEO4J_PASSWORD")


# ---------------------------------------------------------------------------
# db: database administration
# ---------------------------------------------------------------------------


@cli.group()
def db():
    """Database administration (info, check, password, compact, copy)."""


@db.command()
@click.option("--neo4j-home", type=click.Path(exists=True, file_okay=False), default=None)
@click.option("--neo4j-uri", default=DEFAULT_NEO4J_URI)
@click.option("--neo4j-user", default=DEFAULT_NEO4J_USER)
@click.option("--neo4j-password", default=DEFAULT_NEO4J_PASSWORD)
@click.option("--database", default=DEFAULT_DATABASE)
def info(neo4j_home, neo4j_uri, neo4j_user, neo4j_password, database):
    """Show database size, location, Neo4j version, and connection status."""
    import os

    # Try connecting
    connected = False
    try:
        with GraphManaConnection(neo4j_uri, neo4j_user, neo4j_password, database=database) as conn:
            result = conn.execute_read("RETURN 1 AS ok")
            connected = True
    except Exception:
        pass

    click.echo(f"Neo4j URI:      {neo4j_uri}")
    click.echo(f"Database:       {database}")
    click.echo(f"Connected:      {'Yes' if connected else 'No'}")

    # Database size on disk
    if neo4j_home:
        db_path = os.path.join(neo4j_home, "data", "databases", database)
        if os.path.exists(db_path):
            import shutil

            total = sum(
                f.stat().st_size
                for f in __import__("pathlib").Path(db_path).rglob("*")
                if f.is_file()
            )
            click.echo(f"DB path:        {db_path}")
            click.echo(f"DB size:        {total / (1024**3):.1f} GB")

        # Neo4j version
        import subprocess

        neo4j_bin = os.path.join(neo4j_home, "bin", "neo4j")
        if os.path.exists(neo4j_bin):
            result = subprocess.run([neo4j_bin, "version"], capture_output=True, text=True)
            click.echo(f"Neo4j version:  {result.stdout.strip()}")

        # Neo4j status
        result = subprocess.run([neo4j_bin, "status"], capture_output=True, text=True)
        status = result.stdout.strip() if result.returncode == 0 else "Not running"
        click.echo(f"Neo4j status:   {status}")

    if connected:
        with GraphManaConnection(neo4j_uri, neo4j_user, neo4j_password, database=database) as conn:
            for label in ["Variant", "Sample", "Population", "Chromosome"]:
                result = conn.execute_read(f"MATCH (n:{label}) RETURN count(n) AS c")
                rec = result.single()
                click.echo(f"  {label + ':':16s}{rec['c']:,}" if rec else f"  {label}: 0")


@db.command()
@click.option("--neo4j-home", required=True, type=click.Path(exists=True, file_okay=False))
@click.option("--database", default=DEFAULT_DATABASE)
def check(neo4j_home, database):
    """Run Neo4j consistency check on the database."""
    import subprocess
    from pathlib import Path

    neo4j_admin = Path(neo4j_home) / "bin" / "neo4j-admin"
    if not neo4j_admin.exists():
        click.echo(f"Error: neo4j-admin not found at {neo4j_admin}", err=True)
        sys.exit(1)

    click.echo(f"Running consistency check on database '{database}'...")
    click.echo("(Neo4j must be stopped for this operation)")
    result = subprocess.run(
        [str(neo4j_admin), "database", "check", database],
        capture_output=True, text=True,
    )
    click.echo(result.stdout)
    if result.stderr:
        click.echo(result.stderr, err=True)
    if result.returncode != 0:
        click.echo("Consistency check failed.", err=True)
        sys.exit(result.returncode)
    else:
        click.echo("Consistency check passed.")


@db.command()
@click.option("--neo4j-home", required=True, type=click.Path(exists=True, file_okay=False))
@click.option("--new-password", required=True, prompt=True, hide_input=True, confirmation_prompt=True)
def password(neo4j_home, new_password):
    """Change the Neo4j password."""
    import subprocess
    from pathlib import Path

    neo4j_admin = Path(neo4j_home) / "bin" / "neo4j-admin"
    result = subprocess.run(
        [str(neo4j_admin), "dbms", "set-initial-password", new_password],
        capture_output=True, text=True,
    )
    if result.returncode == 0:
        click.echo("Password updated. Takes effect on next Neo4j start.")
    else:
        click.echo(f"Error: {result.stderr.strip()}", err=True)
        click.echo("Note: use Neo4j Browser (http://localhost:7474) to change password on a running instance.")


@db.command()
@click.option("--neo4j-home", required=True, type=click.Path(exists=True, file_okay=False))
@click.option("--database", default=DEFAULT_DATABASE)
@click.option("--destination", required=True, type=click.Path(), help="Destination directory.")
def copy(neo4j_home, database, destination):
    """Copy a database to a new location via neo4j-admin dump/load."""
    import subprocess
    import tempfile
    from pathlib import Path

    neo4j_admin = Path(neo4j_home) / "bin" / "neo4j-admin"
    dest = Path(destination)
    dest.mkdir(parents=True, exist_ok=True)

    click.echo("Neo4j must be stopped for this operation.")

    with tempfile.NamedTemporaryFile(suffix=".dump", delete=False) as tmp:
        dump_path = tmp.name

    try:
        click.echo(f"Dumping database '{database}'...")
        result = subprocess.run(
            [str(neo4j_admin), "database", "dump", database, f"--to-path={dest}"],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            click.echo(f"Error: {result.stderr.strip()}", err=True)
            sys.exit(1)
        click.echo(f"Database copied to: {dest}")
    finally:
        import os

        if os.path.exists(dump_path):
            os.unlink(dump_path)


@db.command()
@click.option("--neo4j-uri", default=DEFAULT_NEO4J_URI)
@click.option("--neo4j-user", default=DEFAULT_NEO4J_USER)
@click.option("--neo4j-password", default=DEFAULT_NEO4J_PASSWORD)
@click.option("--database", default=DEFAULT_DATABASE, help="Neo4j database name.")
@click.option("--fix", is_flag=True, help="Attempt to fix detected issues.")
def validate(neo4j_uri, neo4j_user, neo4j_password, database, fix):
    """Validate database integrity: packed array sizes, population arrays, NEXT chains."""
    try:
        with GraphManaConnection(neo4j_uri, neo4j_user, neo4j_password, database=database) as conn:
            issues = []
            click.echo("Validating database integrity...")

            # 1. Get sample count
            from graphmana.db.queries import ACTIVE_SAMPLE_FILTER

            result = conn.execute_read(
                f"MATCH (s:Sample) WHERE {ACTIVE_SAMPLE_FILTER} "
                "RETURN count(s) AS n"
            )
            n_samples = result.single()["n"]
            click.echo(f"  Active samples: {n_samples}")

            # 2. Check packed array lengths on a sample of variants
            expected_gt_len = (n_samples + 3) // 4
            expected_phase_len = (n_samples + 7) // 8
            result = conn.execute_read(
                "MATCH (v:Variant) "
                "WHERE size(v.gt_packed) <> $expected_gt "
                "RETURN count(v) AS n",
                {"expected_gt": expected_gt_len},
            )
            bad_gt = result.single()["n"]
            if bad_gt > 0:
                issues.append(f"  FAIL: {bad_gt} variants have wrong gt_packed length "
                              f"(expected {expected_gt_len} bytes for {n_samples} samples)")
            else:
                click.echo(f"  gt_packed lengths: OK ({expected_gt_len} bytes)")

            result = conn.execute_read(
                "MATCH (v:Variant) "
                "WHERE v.phase_packed IS NOT NULL AND size(v.phase_packed) <> $expected_phase "
                "RETURN count(v) AS n",
                {"expected_phase": expected_phase_len},
            )
            bad_phase = result.single()["n"]
            if bad_phase > 0:
                issues.append(f"  FAIL: {bad_phase} variants have wrong phase_packed length "
                              f"(expected {expected_phase_len} bytes for {n_samples} samples)")
            else:
                click.echo(f"  phase_packed lengths: OK ({expected_phase_len} bytes)")

            # 3. Check population array consistency
            result = conn.execute_read(
                "MATCH (v:Variant) "
                "WHERE size(v.pop_ids) <> size(v.ac) "
                "   OR size(v.pop_ids) <> size(v.an) "
                "   OR size(v.pop_ids) <> size(v.af) "
                "RETURN count(v) AS n"
            )
            bad_pop = result.single()["n"]
            if bad_pop > 0:
                issues.append(f"  FAIL: {bad_pop} variants have mismatched population array lengths")
            else:
                click.echo("  Population array lengths: OK")

            # 4. Check NEXT chain continuity (spot check per chromosome)
            result = conn.execute_read(
                "MATCH (c:Chromosome) RETURN c.chromosomeId AS chr"
            )
            chroms = [r["chr"] for r in result]
            broken_chains = 0
            for chrom in chroms[:5]:  # Spot check first 5 chromosomes
                result = conn.execute_read(
                    "MATCH (v:Variant {chr: $chr}) "
                    "WHERE NOT (v)-[:NEXT]->() "
                    "AND NOT v.pos = ("
                    "  MATCH (vmax:Variant {chr: $chr}) RETURN max(vmax.pos))[0] "
                    "RETURN count(v) AS n",
                    {"chr": chrom},
                )
                # Simplified: just check that NEXT edges exist
                result2 = conn.execute_read(
                    "MATCH (v:Variant {chr: $chr})-[:NEXT]->() RETURN count(v) AS n_edges",
                    {"chr": chrom},
                )
                result3 = conn.execute_read(
                    "MATCH (v:Variant {chr: $chr}) RETURN count(v) AS n_variants",
                    {"chr": chrom},
                )
                n_edges = result2.single()["n_edges"]
                n_vars = result3.single()["n_variants"]
                if n_vars > 1 and n_edges < n_vars - 1:
                    broken_chains += 1
                    issues.append(
                        f"  FAIL: {chrom} NEXT chain incomplete "
                        f"({n_edges} edges for {n_vars} variants, expected {n_vars - 1})"
                    )
            if broken_chains == 0:
                click.echo(f"  NEXT chains: OK (checked {min(len(chroms), 5)} chromosomes)")

            # Report
            click.echo()
            if issues:
                click.echo(f"Found {len(issues)} issue(s):")
                for issue in issues:
                    click.echo(issue)
                if not fix:
                    click.echo("\nRun with --fix to attempt automatic repair.")
                sys.exit(1)
            else:
                click.echo("Database validation passed.")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


# ---------------------------------------------------------------------------
# summary: human-readable dataset overview
# ---------------------------------------------------------------------------


@cli.command()
@click.option("--neo4j-uri", default=DEFAULT_NEO4J_URI)
@click.option("--neo4j-user", default=DEFAULT_NEO4J_USER)
@click.option("--neo4j-password", default=DEFAULT_NEO4J_PASSWORD)
@click.option("--database", default=DEFAULT_DATABASE)
@click.option("--output", "output_file", type=click.Path(), default=None, help="Write to file.")
def summary(neo4j_uri, neo4j_user, neo4j_password, database, output_file):
    """Generate a human-readable dataset summary report."""
    lines = []

    try:
        with GraphManaConnection(neo4j_uri, neo4j_user, neo4j_password, database=database) as conn:
            # Schema metadata
            result = conn.execute_read("MATCH (m:SchemaMetadata) RETURN m LIMIT 1")
            meta = dict(result.single()["m"]) if result.single is not None else {}
            try:
                rec = conn.execute_read("MATCH (m:SchemaMetadata) RETURN m LIMIT 1").single()
                meta = dict(rec["m"]) if rec else {}
            except Exception:
                meta = {}

            # Counts
            from graphmana.db.queries import ACTIVE_SAMPLE_FILTER

            counts = {}
            for label in ["Variant", "Population", "Chromosome", "Gene", "VCFHeader"]:
                r = conn.execute_read(f"MATCH (n:{label}) RETURN count(n) AS c").single()
                counts[label] = r["c"] if r else 0
            # Sample count with soft-delete filter
            r = conn.execute_read(
                f"MATCH (s:Sample) WHERE {ACTIVE_SAMPLE_FILTER} RETURN count(s) AS c"
            ).single()
            counts["Sample"] = r["c"] if r else 0

            # Variant type breakdown
            vtype_result = conn.execute_read(
                "MATCH (v:Variant) WHERE v.variant_type IS NOT NULL "
                "RETURN v.variant_type AS vt, count(v) AS c ORDER BY c DESC"
            )
            vtypes = [(r["vt"], r["c"]) for r in vtype_result]

            # Populations with sample counts
            pop_result = conn.execute_read(
                "MATCH (s:Sample)-[:IN_POPULATION]->(p:Population) "
                f"WHERE {ACTIVE_SAMPLE_FILTER} "
                "RETURN p.populationId AS pop, count(s) AS n ORDER BY n DESC"
            )
            pops = [(r["pop"], r["n"]) for r in pop_result]

            # Chromosomes with variant counts
            chr_result = conn.execute_read(
                "MATCH (v:Variant)-[:ON_CHROMOSOME]->(c:Chromosome) "
                "RETURN c.chromosomeId AS chr, count(v) AS n ORDER BY n DESC LIMIT 5"
            )
            top_chrs = [(r["chr"], r["n"]) for r in chr_result]

            # Provenance
            prov_result = conn.execute_read(
                "MATCH (l:IngestionLog) "
                "RETURN count(l) AS n, max(l.import_date) AS last_import"
            )
            prov = prov_result.single() if prov_result else None

            # Build report
            lines.append("=" * 60)
            lines.append("GraphMana Dataset Summary")
            lines.append("=" * 60)
            lines.append("")
            ref = meta.get("reference_genome", "unknown")
            lines.append(f"Reference genome:    {ref}")
            lines.append(f"Schema version:      {meta.get('schema_version', 'unknown')}")
            if prov:
                lines.append(f"Last import:         {prov.get('last_import', 'unknown')}")
                lines.append(f"Total imports:       {prov.get('n', 0)}")
            lines.append("")
            lines.append("--- Node Counts ---")
            for label, c in counts.items():
                lines.append(f"  {label + ':':20s}{c:>12,}")
            lines.append("")

            if vtypes:
                lines.append("--- Variant Types ---")
                for vt, c in vtypes:
                    lines.append(f"  {(vt or 'unknown') + ':':20s}{c:>12,}")
                lines.append("")

            if pops:
                lines.append(f"--- Populations ({len(pops)} total) ---")
                for pop, n in pops[:10]:
                    lines.append(f"  {pop + ':':20s}{n:>6} samples")
                if len(pops) > 10:
                    lines.append(f"  ... and {len(pops) - 10} more")
                lines.append("")

            if top_chrs:
                lines.append("--- Top Chromosomes by Variant Count ---")
                for ch, n in top_chrs:
                    lines.append(f"  {ch + ':':20s}{n:>12,}")
                lines.append("")

            lines.append("=" * 60)

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

    report = "\n".join(lines)

    if output_file:
        from pathlib import Path

        Path(output_file).write_text(report + "\n")
        click.echo(f"Summary written to: {output_file}")
    else:
        click.echo(report)


# ---------------------------------------------------------------------------
# query: run Cypher from CLI
# ---------------------------------------------------------------------------


@cli.command()
@click.argument("cypher", required=False)
@click.option("--file", "query_file", type=click.Path(exists=True), help="Read query from file.")
@click.option("--neo4j-uri", default=DEFAULT_NEO4J_URI)
@click.option("--neo4j-user", default=DEFAULT_NEO4J_USER)
@click.option("--neo4j-password", default=DEFAULT_NEO4J_PASSWORD)
@click.option("--database", default=DEFAULT_DATABASE)
@click.option("--format", "output_format", type=click.Choice(["table", "json", "csv"]), default="table")
@click.option("--limit", "max_rows", type=int, default=100, help="Max rows to display. Default: 100.")
def query(cypher, query_file, neo4j_uri, neo4j_user, neo4j_password, database, output_format, max_rows):
    """Run a Cypher query against the database.

    Pass the query as an argument or use --file to read from a file.

    \b
    Examples:
      graphmana query "MATCH (v:Variant) RETURN count(v) AS n"
      graphmana query "MATCH (p:Population) RETURN p.populationId, p.n_samples" --format csv
      graphmana query --file my_query.cypher --format json
    """
    if query_file:
        from pathlib import Path

        cypher = Path(query_file).read_text().strip()
    elif not cypher:
        click.echo("Error: provide a Cypher query as argument or use --file.", err=True)
        sys.exit(1)

    # Safety: block write operations
    upper = cypher.upper().strip()
    if any(kw in upper for kw in ["CREATE", "DELETE", "SET ", "REMOVE ", "MERGE", "DROP", "DETACH"]):
        click.echo("Error: write operations are not allowed via 'graphmana query'.", err=True)
        click.echo("Use Neo4j Browser or Cypher Shell for write operations.", err=True)
        sys.exit(1)

    try:
        with GraphManaConnection(neo4j_uri, neo4j_user, neo4j_password, database=database) as conn:
            result = conn.execute_read(cypher)
            records = list(result)

            if not records:
                click.echo("(no results)")
                return

            keys = records[0].keys() if hasattr(records[0], "keys") else list(records[0].data().keys())
            keys = list(keys)

            rows = []
            for r in records[:max_rows]:
                if hasattr(r, "data"):
                    rows.append(r.data())
                else:
                    rows.append({k: r[k] for k in keys})

            if output_format == "json":
                click.echo(json.dumps(rows, indent=2, default=str))
            elif output_format == "csv":
                click.echo(",".join(keys))
                for row in rows:
                    click.echo(",".join(str(row.get(k, "")) for k in keys))
            else:  # table
                # Calculate column widths
                widths = {k: len(k) for k in keys}
                for row in rows:
                    for k in keys:
                        val = str(row.get(k, ""))
                        if len(val) > 60:
                            val = val[:57] + "..."
                        widths[k] = max(widths[k], len(val))

                # Header
                header = " | ".join(k.ljust(widths[k]) for k in keys)
                click.echo(header)
                click.echo("-+-".join("-" * widths[k] for k in keys))

                # Rows
                for row in rows:
                    vals = []
                    for k in keys:
                        val = str(row.get(k, ""))
                        if len(val) > 60:
                            val = val[:57] + "..."
                        vals.append(val.ljust(widths[k]))
                    click.echo(" | ".join(vals))

                if len(records) > max_rows:
                    click.echo(f"\n... {len(records) - max_rows} more rows (use --limit to show more)")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


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
