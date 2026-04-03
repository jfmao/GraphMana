"""Tests verifying pipeline function defaults match config.py constants."""

import inspect

from graphmana.config import DEFAULT_NEO4J_PASSWORD, DEFAULT_NEO4J_URI, DEFAULT_NEO4J_USER


class TestPipelineDefaults:
    """Verify that pipeline functions use config constants as defaults."""

    def test_run_ingest_password_default(self):
        """run_ingest should default neo4j_password to DEFAULT_NEO4J_PASSWORD."""
        from graphmana.ingest.pipeline import run_ingest

        sig = inspect.signature(run_ingest)
        default = sig.parameters["neo4j_password"].default
        assert default == DEFAULT_NEO4J_PASSWORD

    def test_run_ingest_user_default(self):
        """run_ingest should default neo4j_user to DEFAULT_NEO4J_USER."""
        from graphmana.ingest.pipeline import run_ingest

        sig = inspect.signature(run_ingest)
        default = sig.parameters["neo4j_user"].default
        assert default == DEFAULT_NEO4J_USER

    def test_run_incremental_password_default(self):
        """run_incremental should default neo4j_password to DEFAULT_NEO4J_PASSWORD."""
        from graphmana.ingest.pipeline import run_incremental

        sig = inspect.signature(run_incremental)
        default = sig.parameters["neo4j_password"].default
        assert default == DEFAULT_NEO4J_PASSWORD

    def test_run_incremental_user_default(self):
        """run_incremental should default neo4j_user to DEFAULT_NEO4J_USER."""
        from graphmana.ingest.pipeline import run_incremental

        sig = inspect.signature(run_incremental)
        default = sig.parameters["neo4j_user"].default
        assert default == DEFAULT_NEO4J_USER

    def test_run_incremental_uri_default(self):
        """run_incremental should default neo4j_uri to DEFAULT_NEO4J_URI."""
        from graphmana.ingest.pipeline import run_incremental

        sig = inspect.signature(run_incremental)
        default = sig.parameters["neo4j_uri"].default
        assert default == DEFAULT_NEO4J_URI
