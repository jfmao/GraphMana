"""Tests for graphmana-mcp server — tool definitions and schema validation.

These tests verify that all MCP tools are registered correctly with expected
names, parameter types, and descriptions. No live Neo4j connection needed.
"""

from __future__ import annotations

import inspect

import pytest

from graphmana_mcp import server

# -- Tool function existence ---------------------------------------------------

EXPECTED_TOOLS = [
    "graphmana_status",
    "graphmana_samples",
    "graphmana_populations",
    "graphmana_chromosomes",
    "graphmana_variants",
    "graphmana_filtered_variants",
    "graphmana_genotype_matrix",
    "graphmana_allele_frequencies",
    "graphmana_gene_variants",
    "graphmana_annotated_variants",
    "graphmana_annotation_versions",
    "graphmana_cohorts",
    "graphmana_cohort_samples",
    "graphmana_export",
    "graphmana_query",
]


class TestToolsExist:
    """Verify all expected tool functions are defined in the server module."""

    @pytest.mark.parametrize("name", EXPECTED_TOOLS)
    def test_tool_function_exists(self, name: str):
        assert hasattr(server, name), f"Missing tool function: {name}"
        assert callable(getattr(server, name))


class TestToolSignatures:
    """Verify key tool function signatures."""

    def test_variants_has_chr(self):
        sig = inspect.signature(server.graphmana_variants)
        assert "chr" in sig.parameters

    def test_variants_has_start_end(self):
        sig = inspect.signature(server.graphmana_variants)
        assert "start" in sig.parameters
        assert "end" in sig.parameters

    def test_filtered_variants_params(self):
        sig = inspect.signature(server.graphmana_filtered_variants)
        expected = {
            "chr",
            "start",
            "end",
            "variant_type",
            "maf_min",
            "maf_max",
            "populations",
            "consequence",
            "impact",
            "gene",
        }
        assert expected.issubset(sig.parameters.keys())

    def test_genotype_matrix_has_chr(self):
        sig = inspect.signature(server.graphmana_genotype_matrix)
        assert "chr" in sig.parameters

    def test_export_has_format_and_path(self):
        sig = inspect.signature(server.graphmana_export)
        assert "format" in sig.parameters
        assert "output_path" in sig.parameters

    def test_query_has_cypher(self):
        sig = inspect.signature(server.graphmana_query)
        assert "cypher" in sig.parameters
        assert "params" in sig.parameters

    def test_samples_has_include_excluded(self):
        sig = inspect.signature(server.graphmana_samples)
        assert "include_excluded" in sig.parameters

    def test_gene_variants_has_gene_symbol(self):
        sig = inspect.signature(server.graphmana_gene_variants)
        assert "gene_symbol" in sig.parameters

    def test_cohort_samples_has_name(self):
        sig = inspect.signature(server.graphmana_cohort_samples)
        assert "cohort_name" in sig.parameters


class TestToolDocstrings:
    """Verify all tools have docstrings (used as MCP tool descriptions)."""

    @pytest.mark.parametrize("name", EXPECTED_TOOLS)
    def test_tool_has_docstring(self, name: str):
        func = getattr(server, name)
        assert func.__doc__, f"Tool {name} is missing a docstring"


class TestHelpers:
    """Verify internal helpers."""

    def test_df_to_json_empty(self):
        import pandas as pd

        result = server._df_to_json(pd.DataFrame())
        assert result == "[]"

    def test_df_to_json_records(self):
        import json

        import pandas as pd

        df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        result = json.loads(server._df_to_json(df))
        assert len(result) == 2
        assert result[0]["a"] == 1
        assert result[1]["b"] == "y"

    def test_get_client_returns_client(self, monkeypatch):
        """Verify _get_client creates a GraphManaClient (without connecting)."""
        # Reset global state
        monkeypatch.setattr(server, "_client", None)

        # Mock GraphManaClient to avoid Neo4j connection
        class MockClient:
            def __init__(self, **kwargs):
                self.kwargs = kwargs

        monkeypatch.setattr(
            "graphmana_mcp.server.GraphManaClient",
            MockClient,
            raising=False,
        )
        # We need to mock at the point of import inside _get_client
        import graphmana_py

        monkeypatch.setattr(graphmana_py, "GraphManaClient", MockClient)
        monkeypatch.setattr(server, "_client", None)

        client = server._get_client()
        assert isinstance(client, MockClient)

        # Restore
        monkeypatch.setattr(server, "_client", None)


class TestFastMCPInstance:
    """Verify the FastMCP instance is configured correctly."""

    def test_mcp_exists(self):
        assert hasattr(server, "mcp")

    def test_mcp_name(self):
        assert server.mcp.name == "GraphMana"

    def test_main_is_callable(self):
        assert callable(server.main)
