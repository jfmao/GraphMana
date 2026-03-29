"""Tests for annotation-related Cypher query constants."""

from graphmana.db.queries import (
    COUNT_EDGES_BY_VERSION,
    CREATE_ANNOTATION_VERSION,
    CREATE_CONSEQUENCE_BATCH,
    DELETE_ANNOTATION_VERSION,
    DELETE_EDGES_BY_VERSION_BATCH,
    DELETE_ORPHAN_GENES,
    GET_ANNOTATION_VERSION,
    LIST_ANNOTATION_VERSIONS,
    MERGE_CONSEQUENCE_BATCH,
    MERGE_GENE_BATCH,
)


class TestAnnotationVersionQueries:
    """Verify annotation version management queries."""

    def test_create_uses_merge(self):
        assert "MERGE" in CREATE_ANNOTATION_VERSION

    def test_create_has_version_id(self):
        assert "$version_id" in CREATE_ANNOTATION_VERSION

    def test_create_sets_all_fields(self):
        for field in ["source", "version", "loaded_date", "n_annotations", "description"]:
            assert f"${'$' if False else ''}{field}" in CREATE_ANNOTATION_VERSION or (
                f"a.{field}" in CREATE_ANNOTATION_VERSION
            )

    def test_get_matches_version_id(self):
        assert "$version_id" in GET_ANNOTATION_VERSION
        assert "AnnotationVersion" in GET_ANNOTATION_VERSION

    def test_list_orders_by_loaded_date(self):
        assert "loaded_date" in LIST_ANNOTATION_VERSIONS
        assert "DESC" in LIST_ANNOTATION_VERSIONS

    def test_delete_version_returns_count(self):
        assert "DELETE" in DELETE_ANNOTATION_VERSION
        assert "deleted" in DELETE_ANNOTATION_VERSION


class TestEdgeQueries:
    """Verify HAS_CONSEQUENCE edge management queries."""

    def test_count_edges_by_version(self):
        assert "$version" in COUNT_EDGES_BY_VERSION
        assert "HAS_CONSEQUENCE" in COUNT_EDGES_BY_VERSION

    def test_delete_edges_batched(self):
        assert "LIMIT" in DELETE_EDGES_BY_VERSION_BATCH
        assert "$batch_size" in DELETE_EDGES_BY_VERSION_BATCH
        assert "$version" in DELETE_EDGES_BY_VERSION_BATCH

    def test_create_consequence_has_all_fields(self):
        for field in [
            "consequence",
            "impact",
            "feature",
            "feature_type",
            "sift_score",
            "sift_pred",
            "polyphen_score",
            "polyphen_pred",
            "cadd_phred",
            "revel",
            "annotation_source",
            "annotation_version",
        ]:
            assert field in CREATE_CONSEQUENCE_BATCH

    def test_create_consequence_uses_unwind(self):
        assert "UNWIND" in CREATE_CONSEQUENCE_BATCH

    def test_merge_consequence_uses_merge(self):
        assert "MERGE" in MERGE_CONSEQUENCE_BATCH
        assert "annotation_version" in MERGE_CONSEQUENCE_BATCH
        assert "feature" in MERGE_CONSEQUENCE_BATCH

    def test_merge_consequence_uses_unwind(self):
        assert "UNWIND" in MERGE_CONSEQUENCE_BATCH


class TestGeneQueries:
    """Verify Gene node management queries."""

    def test_merge_gene_uses_unwind(self):
        assert "UNWIND" in MERGE_GENE_BATCH
        assert "MERGE" in MERGE_GENE_BATCH

    def test_merge_gene_sets_fields(self):
        assert "symbol" in MERGE_GENE_BATCH
        assert "biotype" in MERGE_GENE_BATCH

    def test_delete_orphan_genes_uses_not_exists(self):
        assert "NOT EXISTS" in DELETE_ORPHAN_GENES
        assert "HAS_CONSEQUENCE" in DELETE_ORPHAN_GENES
        assert "deleted" in DELETE_ORPHAN_GENES
