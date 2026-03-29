"""MCP server wrapping GraphManaClient for AI agent access to a GraphMana database.

Each tool is a thin wrapper: call the client method, serialize DataFrames to JSON.
Connection parameters come from environment variables with the same defaults as the client.
"""

from __future__ import annotations

import json
import os

from mcp.server.fastmcp import FastMCP

mcp = FastMCP(
    "GraphMana",
    instructions=(
        "Query and export variant genomics data from a GraphMana Neo4j database. "
        "Supports samples, populations, variants, genotype matrices, allele frequencies, "
        "annotations, cohorts, and multi-format export."
    ),
)

_client = None


def _get_client():
    """Lazy-initialize a shared GraphManaClient instance."""
    global _client
    if _client is None:
        from graphmana_py import GraphManaClient

        uri = os.environ.get("GRAPHMANA_NEO4J_URI", "bolt://localhost:7687")
        user = os.environ.get("GRAPHMANA_NEO4J_USER", "neo4j")
        password = os.environ.get("GRAPHMANA_NEO4J_PASSWORD", "graphmana")
        _client = GraphManaClient(uri=uri, user=user, password=password)
    return _client


def _df_to_json(df) -> str:
    """Serialize a pandas DataFrame to a JSON string of records."""
    if df.empty:
        return "[]"
    return df.to_json(orient="records")


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------


@mcp.tool()
def graphmana_status() -> str:
    """Return database summary: node counts per label and schema metadata.

    No parameters required. Returns a JSON object with 'counts'
    (Variant, Sample, Population, etc.) and 'schema' metadata.
    """
    result = _get_client().status()
    return json.dumps(result)


@mcp.tool()
def graphmana_samples(include_excluded: bool = False) -> str:
    """Return all samples in the database.

    Args:
        include_excluded: Include soft-deleted samples (default False).

    Returns JSON array with columns: sampleId, population, packed_index,
    sex, source_file, ingestion_date.
    """
    df = _get_client().samples(include_excluded=include_excluded)
    return _df_to_json(df)


@mcp.tool()
def graphmana_populations() -> str:
    """Return all populations with sample counts.

    Returns JSON array with columns: populationId, name, n_samples,
    n_active_samples, a_n, a_n2.
    """
    df = _get_client().populations()
    return _df_to_json(df)


@mcp.tool()
def graphmana_chromosomes() -> str:
    """Return all chromosomes with variant counts.

    Returns JSON array with columns: chromosomeId, length, n_variants, aliases.
    """
    df = _get_client().chromosomes()
    return _df_to_json(df)


@mcp.tool()
def graphmana_variants(chr: str, start: int | None = None, end: int | None = None) -> str:
    """Return variants on a chromosome or genomic region.

    Args:
        chr: Chromosome ID (e.g. "22" or "chr22").
        start: Start position (inclusive). Optional.
        end: End position (inclusive). Optional.

    Returns JSON array with columns: variantId, chr, pos, ref, alt,
    variant_type, af_total, ac_total, an_total, call_rate, consequence,
    impact, gene_symbol.
    """
    df = _get_client().variants(chr=chr, start=start, end=end)
    return _df_to_json(df)


@mcp.tool()
def graphmana_filtered_variants(
    chr: str | None = None,
    start: int | None = None,
    end: int | None = None,
    variant_type: str | None = None,
    maf_min: float | None = None,
    maf_max: float | None = None,
    populations: list[str] | None = None,
    consequence: str | None = None,
    impact: str | None = None,
    gene: str | None = None,
) -> str:
    """Return variants matching filter criteria. All parameters optional.

    Args:
        chr: Chromosome ID.
        start: Start position (inclusive).
        end: End position (inclusive).
        variant_type: Variant type (e.g. "SNP", "INDEL").
        maf_min: Minimum minor allele frequency.
        maf_max: Maximum minor allele frequency.
        populations: Filter to variants present in these populations.
        consequence: Consequence type (e.g. "missense_variant").
        impact: Impact level (e.g. "HIGH", "MODERATE").
        gene: Gene symbol or Ensembl ID.

    Returns JSON array of matching variant records.
    """
    df = _get_client().filtered_variants(
        chr=chr,
        start=start,
        end=end,
        variant_type=variant_type,
        maf_min=maf_min,
        maf_max=maf_max,
        populations=populations,
        consequence=consequence,
        impact=impact,
        gene=gene,
    )
    return _df_to_json(df)


@mcp.tool()
def graphmana_genotype_matrix(chr: str, start: int | None = None, end: int | None = None) -> str:
    """Return a samples-by-variants genotype matrix (FULL PATH).

    Values: 0=HomRef, 1=Het, 2=HomAlt, 3=Missing.
    WARNING: memory scales with n_samples * n_variants. Use a small region.

    Args:
        chr: Chromosome ID.
        start: Start position (inclusive). Recommended.
        end: End position (inclusive). Recommended.

    Returns JSON object with keys 'index' (sample IDs), 'columns'
    (variant IDs), and 'data' (genotype values).
    """
    df = _get_client().genotype_matrix(chr=chr, start=start, end=end)
    if df.empty:
        return json.dumps({"index": [], "columns": [], "data": []})
    return df.to_json(orient="split")


@mcp.tool()
def graphmana_allele_frequencies(chr: str, start: int | None = None, end: int | None = None) -> str:
    """Return per-population allele frequencies (FAST PATH — instant at any sample count).

    Args:
        chr: Chromosome ID.
        start: Start position (inclusive). Optional.
        end: End position (inclusive). Optional.

    Returns JSON array with columns: variantId, pos, and per-population
    ac_<pop>, an_<pop>, af_<pop> columns.
    """
    df = _get_client().allele_frequencies(chr=chr, start=start, end=end)
    return _df_to_json(df)


@mcp.tool()
def graphmana_gene_variants(gene_symbol: str) -> str:
    """Return variants associated with a gene via functional annotations.

    Args:
        gene_symbol: Gene symbol (e.g. "BRCA1") or Ensembl gene ID.

    Returns JSON array with variant info and consequence/impact columns.
    """
    df = _get_client().gene_variants(gene_symbol)
    return _df_to_json(df)


@mcp.tool()
def graphmana_annotated_variants(annotation_version: str) -> str:
    """Return variants with a specific annotation version.

    Args:
        annotation_version: Annotation version label (e.g. "VEP_110").

    Returns JSON array with variant info, consequence, and annotation version.
    """
    df = _get_client().annotated_variants(annotation_version)
    return _df_to_json(df)


@mcp.tool()
def graphmana_annotation_versions() -> str:
    """Return all annotation versions loaded in the database.

    Returns JSON array with columns: version_id, source, version,
    loaded_date, n_annotations, description.
    """
    df = _get_client().annotation_versions()
    return _df_to_json(df)


@mcp.tool()
def graphmana_cohorts() -> str:
    """Return all cohort definitions stored in the database.

    Returns JSON array with columns: name, cypher_query, created_date, description.
    """
    df = _get_client().cohorts()
    return _df_to_json(df)


@mcp.tool()
def graphmana_cohort_samples(cohort_name: str) -> str:
    """Return samples matching a cohort definition.

    Args:
        cohort_name: Name of the cohort.

    Returns JSON array with columns: sampleId, population, packed_index, sex.
    """
    df = _get_client().cohort_samples(cohort_name)
    return _df_to_json(df)


@mcp.tool()
def graphmana_export(
    format: str,
    output_path: str,
    chr: str | None = None,
    populations: str | None = None,
) -> str:
    """Export data to a file in the specified format via the graphmana CLI.

    Supported formats: vcf, plink, plink2, eigenstrat, treemix, sfs-dadi,
    sfs-fsc, beagle, structure, genepop, bed, hap, tsv.

    Args:
        format: Output format name.
        output_path: Path for the output file.
        chr: Optional chromosome filter.
        populations: Optional comma-separated population filter.

    Returns a status message indicating success or failure.
    """
    filters = {}
    if chr is not None:
        filters["filter_chr"] = chr
    if populations is not None:
        filters["filter_populations"] = populations
    try:
        _get_client()._run_cli_export(format, output_path, **filters)
        return json.dumps({"status": "success", "file": output_path, "format": format})
    except Exception as exc:
        return json.dumps({"status": "error", "message": str(exc)})


@mcp.tool()
def graphmana_query(cypher: str, params: str | None = None) -> str:
    """Run an arbitrary Cypher query against the GraphMana database.

    Args:
        cypher: Cypher query string. Use $param placeholders for parameters.
        params: Optional JSON string of query parameters (e.g. '{"chr": "22"}').

    Returns JSON array of result records.
    """
    parsed_params = None
    if params:
        parsed_params = json.loads(params)
    df = _get_client().query(cypher, parsed_params)
    return _df_to_json(df)


def main():
    """Entry point for the graphmana-mcp command."""
    mcp.run()


if __name__ == "__main__":
    main()
