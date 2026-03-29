"""Shared test fixtures for GraphMana CLI tests."""

import pytest
from neo4j import GraphDatabase


@pytest.fixture(scope="session")
def neo4j_driver():
    """Create a Neo4j driver for integration tests.

    Skips tests if Neo4j is not available.
    """
    uri = "bolt://localhost:7687"
    try:
        driver = GraphDatabase.driver(uri, auth=("neo4j", "graphmana"))
        driver.verify_connectivity()
        yield driver
        driver.close()
    except Exception:
        pytest.skip("Neo4j not available at bolt://localhost:7687")
