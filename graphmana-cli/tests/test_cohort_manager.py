"""Tests for CohortManager business logic."""

import pytest

from graphmana.cohort.manager import _WRITE_KEYWORDS_RE, CohortManager


class TestWriteKeywordDetection:
    """Validate that write keywords are rejected."""

    @pytest.mark.parametrize(
        "keyword",
        ["CREATE", "DELETE", "SET", "REMOVE", "MERGE", "DROP", "DETACH"],
    )
    def test_rejects_write_keyword_uppercase(self, keyword):
        query = f"MATCH (s:Sample) {keyword} s.name = 'x' RETURN s.sampleId AS sampleId"
        assert _WRITE_KEYWORDS_RE.search(query) is not None

    @pytest.mark.parametrize(
        "keyword",
        ["create", "delete", "set", "remove", "merge", "drop", "detach"],
    )
    def test_rejects_write_keyword_lowercase(self, keyword):
        query = f"MATCH (s:Sample) {keyword} s.name = 'x' RETURN s.sampleId AS sampleId"
        assert _WRITE_KEYWORDS_RE.search(query) is not None

    def test_accepts_read_only_query(self):
        query = "MATCH (s:Sample) WHERE s.population = 'EUR' RETURN s.sampleId AS sampleId"
        assert _WRITE_KEYWORDS_RE.search(query) is None

    def test_rejects_mixed_case(self):
        query = "MATCH (s:Sample) Set s.name = 'x' RETURN s.sampleId AS sampleId"
        assert _WRITE_KEYWORDS_RE.search(query) is not None

    def test_no_false_positive_substring(self):
        """'dataset' contains 'set' but should NOT match (word boundary)."""
        query = "MATCH (s:Sample) WHERE s.dataset = 'foo' RETURN s.sampleId AS sampleId"
        assert _WRITE_KEYWORDS_RE.search(query) is None

    def test_no_false_positive_reset(self):
        """'reset' contains 'set' but should NOT match (word boundary)."""
        query = "MATCH (s:Sample) WHERE s.reset = true RETURN s.sampleId AS sampleId"
        assert _WRITE_KEYWORDS_RE.search(query) is None

    def test_detach_delete(self):
        query = "MATCH (n) DETACH DELETE n"
        assert _WRITE_KEYWORDS_RE.search(query) is not None


class TestCohortManagerInterface:
    """Verify CohortManager has the expected methods."""

    def test_class_exists(self):
        assert CohortManager is not None

    def test_has_define(self):
        assert callable(getattr(CohortManager, "define", None))

    def test_has_list(self):
        assert callable(getattr(CohortManager, "list", None))

    def test_has_get(self):
        assert callable(getattr(CohortManager, "get", None))

    def test_has_delete(self):
        assert callable(getattr(CohortManager, "delete", None))

    def test_has_count(self):
        assert callable(getattr(CohortManager, "count", None))

    def test_has_validate(self):
        assert callable(getattr(CohortManager, "validate", None))

    def test_has_resolve_sample_ids(self):
        assert callable(getattr(CohortManager, "resolve_sample_ids", None))


class TestWriteKeywordRegex:
    """Additional edge cases for the regex pattern."""

    def test_create_at_start(self):
        assert _WRITE_KEYWORDS_RE.search("CREATE (n:Node)") is not None

    def test_keyword_in_string_literal_still_matches(self):
        """The regex does not parse Cypher strings — it matches raw text.
        This is a known conservative limitation: queries with write keywords
        in string literals are also rejected."""
        query = "MATCH (s:Sample) WHERE s.name = 'CREATE' RETURN s.sampleId AS sampleId"
        assert _WRITE_KEYWORDS_RE.search(query) is not None

    def test_return_only_query(self):
        query = "RETURN 1 AS sampleId"
        assert _WRITE_KEYWORDS_RE.search(query) is None

    def test_with_clause(self):
        query = (
            "MATCH (s:Sample)-[:IN_POPULATION]->(p:Population) "
            "WITH s, p WHERE p.populationId = 'EUR' "
            "RETURN s.sampleId AS sampleId"
        )
        assert _WRITE_KEYWORDS_RE.search(query) is None
