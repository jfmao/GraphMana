"""Cohort management — named Cypher queries that define sample subsets."""

from __future__ import annotations

import re
from datetime import datetime, timezone

from graphmana.db.connection import GraphManaConnection
from graphmana.db.queries import (
    ACTIVE_SAMPLE_FILTER,
    CREATE_COHORT,
    DELETE_COHORT,
    GET_COHORT,
    LIST_COHORTS,
)

_WRITE_KEYWORDS_RE = re.compile(
    r"\b(CREATE|DELETE|SET|REMOVE|MERGE|DROP|DETACH)\b",
    re.IGNORECASE,
)


class CohortManager:
    """Manage named cohort definitions stored as CohortDefinition nodes."""

    def __init__(self, conn: GraphManaConnection) -> None:
        self._conn = conn

    def define(self, name: str, cypher_query: str, description: str = "") -> dict:
        """Create or update a named cohort definition.

        Args:
            name: Unique cohort name.
            cypher_query: Cypher query returning ``sampleId`` column.
            description: Human-readable description.

        Returns:
            Dict of CohortDefinition node properties.
        """
        validation = self.validate(cypher_query)
        if not validation["valid"]:
            raise ValueError(f"Invalid cohort query: {validation['error']}")

        now = datetime.now(timezone.utc).isoformat()
        with self._conn.driver.session() as session:
            result = session.run(
                CREATE_COHORT,
                {
                    "name": name,
                    "cypher_query": cypher_query,
                    "created_date": now,
                    "description": description,
                },
            )
            record = result.single()
            return dict(record["c"]) if record else {}

    def list(self) -> list[dict]:
        """List all cohort definitions.

        Returns:
            List of CohortDefinition property dicts, ordered by name.
        """
        with self._conn.driver.session() as session:
            result = session.run(LIST_COHORTS)
            return [dict(record["c"]) for record in result]

    def get(self, name: str) -> dict | None:
        """Get a single cohort definition by name.

        Returns:
            CohortDefinition property dict, or None if not found.
        """
        with self._conn.driver.session() as session:
            result = session.run(GET_COHORT, {"name": name})
            record = result.single()
            return dict(record["c"]) if record else None

    def delete(self, name: str) -> bool:
        """Delete a cohort definition.

        Returns:
            True if deleted, False if not found.
        """
        with self._conn.driver.session() as session:
            result = session.run(DELETE_COHORT, {"name": name})
            record = result.single()
            return record is not None and record["deleted"] > 0

    def count(self, name: str) -> int:
        """Count samples matching a named cohort.

        Args:
            name: Cohort name.

        Returns:
            Number of samples returned by the cohort query.

        Raises:
            ValueError: If cohort not found.
        """
        sample_ids = self.resolve_sample_ids(name)
        return len(sample_ids)

    def validate(self, cypher_query: str) -> dict:
        """Validate a Cypher query for use as a cohort definition.

        Checks:
        1. No write keywords (CREATE, DELETE, SET, REMOVE, MERGE, DROP, DETACH).
        2. Query parses without error (via EXPLAIN).
        3. Query returns a ``sampleId`` column.

        Returns:
            Dict with keys: valid (bool), error (str|None), n_samples (int|None).
        """
        # Check for write keywords
        if _WRITE_KEYWORDS_RE.search(cypher_query):
            return {
                "valid": False,
                "error": (
                    "Query contains write operations"
                    " (CREATE/DELETE/SET/REMOVE/MERGE/DROP/DETACH)"
                ),
                "n_samples": None,
            }

        # Check syntax via EXPLAIN.
        # Note: EXPLAIN does not support parameterized queries, so we must
        # prepend to the user string. The write-keyword check above prevents
        # mutation, but a crafted query could still read unintended data.
        # Cohort definitions should only be created by trusted administrators.
        try:
            with self._conn.driver.session() as session:
                session.run("EXPLAIN " + cypher_query).consume()
        except Exception as exc:
            return {
                "valid": False,
                "error": f"Cypher syntax error: {exc}",
                "n_samples": None,
            }

        # Execute and check for sampleId column
        try:
            with self._conn.driver.session() as session:
                result = session.run(cypher_query)
                records = list(result)
                if records and "sampleId" not in records[0].keys():
                    return {
                        "valid": False,
                        "error": "Query must return a 'sampleId' column",
                        "n_samples": None,
                    }
                return {
                    "valid": True,
                    "error": None,
                    "n_samples": len(records),
                }
        except Exception as exc:
            return {
                "valid": False,
                "error": f"Query execution error: {exc}",
                "n_samples": None,
            }

    def resolve_sample_ids(self, name: str) -> list[str]:
        """Execute the cohort query and return matching sample IDs.

        Args:
            name: Cohort name.

        Returns:
            List of sampleId strings.

        Raises:
            ValueError: If cohort not found or query fails.
        """
        cohort = self.get(name)
        if cohort is None:
            raise ValueError(f"Cohort not found: {name!r}")

        cypher_query = cohort["cypher_query"]
        with self._conn.driver.session() as session:
            result = session.run(cypher_query)
            candidate_ids = [record["sampleId"] for record in result]

        if not candidate_ids:
            return []

        # Post-filter: exclude soft-deleted samples
        with self._conn.driver.session() as session:
            result = session.run(
                "MATCH (s:Sample) WHERE s.sampleId IN $ids "
                f"AND ({ACTIVE_SAMPLE_FILTER}) "
                "RETURN s.sampleId AS sampleId",
                {"ids": candidate_ids},
            )
            return [record["sampleId"] for record in result]
