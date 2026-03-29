"""Tests for schema creation queries."""

from graphmana.db.schema import (
    _SCHEMA_METADATA_QUERY,
    CONSTRAINTS,
    INDEXES,
)


class TestConstraints:
    """Verify constraint Cypher statements."""

    def test_count(self):
        assert len(CONSTRAINTS) == 12

    def test_all_create_constraint(self):
        for stmt in CONSTRAINTS:
            assert stmt.startswith("CREATE CONSTRAINT")

    def test_if_not_exists(self):
        for stmt in CONSTRAINTS:
            assert "IF NOT EXISTS" in stmt

    def test_required_labels(self):
        labels = {"Variant", "Sample", "Population", "Gene", "Chromosome"}
        found = set()
        for stmt in CONSTRAINTS:
            for label in labels:
                if (
                    f"(v:{label})" in stmt
                    or f"(s:{label})" in stmt
                    or f"(p:{label})" in stmt
                    or f"(g:{label})" in stmt
                    or f"(c:{label})" in stmt
                ):
                    found.add(label)
        assert found == labels

    def test_cohort_constraint(self):
        found = any("CohortDefinition" in stmt for stmt in CONSTRAINTS)
        assert found, "CohortDefinition constraint missing"

    def test_annotation_version_constraint(self):
        found = any("AnnotationVersion" in stmt and "version_id" in stmt for stmt in CONSTRAINTS)
        assert found, "AnnotationVersion constraint missing"


class TestIndexes:
    """Verify index Cypher statements."""

    def test_count(self):
        assert len(INDEXES) == 15

    def test_all_create_index(self):
        for stmt in INDEXES:
            assert stmt.startswith("CREATE INDEX")

    def test_if_not_exists(self):
        for stmt in INDEXES:
            assert "IF NOT EXISTS" in stmt

    def test_annotation_version_source_index(self):
        found = any("AnnotationVersion" in stmt and "source" in stmt for stmt in INDEXES)
        assert found, "AnnotationVersion source index missing"

    def test_has_consequence_version_index(self):
        found = any("HAS_CONSEQUENCE" in stmt and "annotation_version" in stmt for stmt in INDEXES)
        assert found, "HAS_CONSEQUENCE annotation_version relationship index missing"


class TestSchemaMetadataQuery:
    """Verify the schema metadata Cypher query."""

    def test_merge_operation(self):
        assert "MERGE" in _SCHEMA_METADATA_QUERY

    def test_parameters(self):
        for param in [
            "$schema_version",
            "$graphmana_version",
            "$reference_genome",
            "$now",
            "$chr_naming_style",
            "$n_samples",
            "$n_variants",
            "$n_populations",
        ]:
            assert param in _SCHEMA_METADATA_QUERY

    def test_created_date_coalesce(self):
        """created_date should use coalesce to keep the first value."""
        assert "coalesce(m.created_date, $now)" in _SCHEMA_METADATA_QUERY
