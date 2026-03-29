"""ToolUniverse BaseTool wrapper for GraphMana.

Provides five coarse-grained tools that map to the GraphMana Python client:
  - GraphMana_QueryVariants
  - GraphMana_GetSamples
  - GraphMana_AlleleFrequencies
  - GraphMana_GenotypeMatrix
  - GraphMana_ExportData

Each tool instantiates a GraphManaClient, calls the appropriate method(s),
and returns JSON results in the ToolUniverse format: {"result": ..., "success": True}.

Usage:
    Register this module with ToolUniverse by placing it alongside graphmana_tools.json
    in the ToolUniverse tools directory.

Environment variables:
    GRAPHMANA_NEO4J_URI      (default: bolt://localhost:7687)
    GRAPHMANA_NEO4J_USER     (default: neo4j)
    GRAPHMANA_NEO4J_PASSWORD (default: graphmana)
"""

from __future__ import annotations

import json
import os

_client = None


def _get_client():
    """Lazy-initialize a shared GraphManaClient."""
    global _client
    if _client is None:
        from graphmana_py import GraphManaClient

        _client = GraphManaClient(
            uri=os.environ.get("GRAPHMANA_NEO4J_URI", "bolt://localhost:7687"),
            user=os.environ.get("GRAPHMANA_NEO4J_USER", "neo4j"),
            password=os.environ.get("GRAPHMANA_NEO4J_PASSWORD", "graphmana"),
        )
    return _client


def _df_to_records(df):
    """Serialize a DataFrame to a list of dicts."""
    if df.empty:
        return []
    return json.loads(df.to_json(orient="records"))


def _ok(result):
    return {"result": result, "success": True}


def _err(message: str):
    return {"result": message, "success": False}


# ---------------------------------------------------------------------------
# Tool implementations
# ---------------------------------------------------------------------------


def query_variants(arguments: dict) -> dict:
    """GraphMana_QueryVariants — query variants by region, gene, or filters."""
    try:
        client = _get_client()
        gene = arguments.get("gene")

        # If gene is specified and no region filters, use gene_variants shortcut
        if gene and not arguments.get("chr"):
            df = client.gene_variants(gene)
        else:
            df = client.filtered_variants(
                chr=arguments.get("chr"),
                start=arguments.get("start"),
                end=arguments.get("end"),
                variant_type=arguments.get("variant_type"),
                maf_min=arguments.get("maf_min"),
                maf_max=arguments.get("maf_max"),
                populations=arguments.get("populations"),
                consequence=arguments.get("consequence"),
                impact=arguments.get("impact"),
                gene=gene,
            )
        return _ok(_df_to_records(df))
    except Exception as exc:
        return _err(str(exc))


def get_samples(arguments: dict) -> dict:
    """GraphMana_GetSamples — retrieve sample or population metadata."""
    try:
        client = _get_client()
        query_type = arguments.get("query_type", "samples")

        if query_type == "populations":
            df = client.populations()
        elif query_type == "cohort":
            cohort_name = arguments.get("cohort_name")
            if not cohort_name:
                return _err("cohort_name is required when query_type is 'cohort'")
            df = client.cohort_samples(cohort_name)
        else:
            include_excluded = arguments.get("include_excluded", False)
            df = client.samples(include_excluded=include_excluded)

        return _ok(_df_to_records(df))
    except Exception as exc:
        return _err(str(exc))


def allele_frequencies(arguments: dict) -> dict:
    """GraphMana_AlleleFrequencies — per-population allele frequencies (FAST PATH)."""
    try:
        client = _get_client()
        chr_val = arguments.get("chr")
        if not chr_val:
            return _err("'chr' is required")
        df = client.allele_frequencies(
            chr=chr_val,
            start=arguments.get("start"),
            end=arguments.get("end"),
        )
        return _ok(_df_to_records(df))
    except Exception as exc:
        return _err(str(exc))


def genotype_matrix(arguments: dict) -> dict:
    """GraphMana_GenotypeMatrix — samples-by-variants matrix (FULL PATH)."""
    try:
        client = _get_client()
        chr_val = arguments.get("chr")
        if not chr_val:
            return _err("'chr' is required")
        df = client.genotype_matrix(
            chr=chr_val,
            start=arguments.get("start"),
            end=arguments.get("end"),
        )
        if df.empty:
            result = {"index": [], "columns": [], "data": []}
        else:
            result = json.loads(df.to_json(orient="split"))
        return _ok(result)
    except Exception as exc:
        return _err(str(exc))


def export_data(arguments: dict) -> dict:
    """GraphMana_ExportData — export to standard genomics formats."""
    try:
        client = _get_client()
        fmt = arguments.get("format")
        output_path = arguments.get("output_path")
        if not fmt or not output_path:
            return _err("'format' and 'output_path' are required")

        filters = {}
        if arguments.get("chr"):
            filters["filter_chr"] = arguments["chr"]
        if arguments.get("populations"):
            filters["filter_populations"] = arguments["populations"]

        client._run_cli_export(fmt, output_path, **filters)
        return _ok({"file": output_path, "format": fmt})
    except Exception as exc:
        return _err(str(exc))


# ---------------------------------------------------------------------------
# Tool registry — maps ToolUniverse tool names to functions
# ---------------------------------------------------------------------------

TOOL_REGISTRY = {
    "GraphMana_QueryVariants": query_variants,
    "GraphMana_GetSamples": get_samples,
    "GraphMana_AlleleFrequencies": allele_frequencies,
    "GraphMana_GenotypeMatrix": genotype_matrix,
    "GraphMana_ExportData": export_data,
}


def run(tool_name: str, arguments: dict) -> dict:
    """ToolUniverse entry point — dispatch to the appropriate tool function.

    Args:
        tool_name: One of the registered tool names.
        arguments: Tool arguments as a dict.

    Returns:
        Dict with 'result' and 'success' keys.
    """
    handler = TOOL_REGISTRY.get(tool_name)
    if handler is None:
        return _err(f"Unknown tool: {tool_name}")
    return handler(arguments)
