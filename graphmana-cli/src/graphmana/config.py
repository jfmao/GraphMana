"""Default configuration for GraphMana."""

import os

# Neo4j connection defaults
DEFAULT_NEO4J_URI = "bolt://localhost:7687"
DEFAULT_NEO4J_USER = "neo4j"
DEFAULT_NEO4J_PASSWORD = os.environ.get("GRAPHMANA_NEO4J_PASSWORD", "graphmana")

# Version
GRAPHMANA_VERSION = "1.0.0-dev"
SCHEMA_VERSION = "0.1.0"

# Processing defaults
DEFAULT_BATCH_SIZE = 100_000
DEFAULT_ANNOTATION_BATCH_SIZE = 10_000
DEFAULT_THREADS = 1
DEFAULT_DATABASE = "neo4j"
