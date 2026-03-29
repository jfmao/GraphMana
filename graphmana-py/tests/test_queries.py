"""Tests for Cypher query strings in graphmana_py._queries."""

from graphmana_py import _queries as Q


class TestQueryConstants:
    """Verify all query constants are importable and contain expected patterns."""

    def test_status_node_count(self):
        assert "{label}" in Q.STATUS_NODE_COUNT
        assert "count" in Q.STATUS_NODE_COUNT

    def test_schema_metadata(self):
        assert "SchemaMetadata" in Q.GET_SCHEMA_METADATA

    def test_fetch_samples(self):
        assert "Sample" in Q.FETCH_SAMPLES
        assert "IN_POPULATION" in Q.FETCH_SAMPLES
        assert "excluded" in Q.FETCH_SAMPLES
        assert "packed_index" in Q.FETCH_SAMPLES

    def test_fetch_all_samples(self):
        assert "Sample" in Q.FETCH_ALL_SAMPLES
        assert "excluded" in Q.FETCH_ALL_SAMPLES
        # Should NOT filter excluded
        assert "WHERE s.excluded IS NULL" not in Q.FETCH_ALL_SAMPLES

    def test_fetch_populations(self):
        assert "Population" in Q.FETCH_POPULATIONS
        assert "n_samples" in Q.FETCH_POPULATIONS
        assert "n_active_samples" in Q.FETCH_POPULATIONS

    def test_fetch_chromosomes(self):
        assert "Chromosome" in Q.FETCH_CHROMOSOMES
        assert "n_variants" in Q.FETCH_CHROMOSOMES

    def test_fetch_variants_by_chr(self):
        assert "$chr" in Q.FETCH_VARIANTS_BY_CHR
        assert "variantId" in Q.FETCH_VARIANTS_BY_CHR
        assert "ORDER BY v.pos" in Q.FETCH_VARIANTS_BY_CHR

    def test_fetch_variants_region(self):
        assert "$chr" in Q.FETCH_VARIANTS_REGION
        assert "$start" in Q.FETCH_VARIANTS_REGION
        assert "$end" in Q.FETCH_VARIANTS_REGION

    def test_fetch_variant_genotypes_by_chr(self):
        assert "gt_packed" in Q.FETCH_VARIANT_GENOTYPES_BY_CHR
        assert "$chr" in Q.FETCH_VARIANT_GENOTYPES_BY_CHR

    def test_fetch_variant_genotypes_region(self):
        assert "gt_packed" in Q.FETCH_VARIANT_GENOTYPES_REGION
        assert "$start" in Q.FETCH_VARIANT_GENOTYPES_REGION
        assert "$end" in Q.FETCH_VARIANT_GENOTYPES_REGION

    def test_fetch_annotation_versions(self):
        assert "AnnotationVersion" in Q.FETCH_ANNOTATION_VERSIONS
        assert "version_id" in Q.FETCH_ANNOTATION_VERSIONS
        assert "loaded_date" in Q.FETCH_ANNOTATION_VERSIONS

    def test_fetch_cohorts(self):
        assert "CohortDefinition" in Q.FETCH_COHORTS
        assert "cypher_query" in Q.FETCH_COHORTS

    def test_fetch_variant_pop_arrays(self):
        assert "pop_ids" in Q.FETCH_VARIANT_POP_ARRAYS
        assert "ac" in Q.FETCH_VARIANT_POP_ARRAYS
        assert "an" in Q.FETCH_VARIANT_POP_ARRAYS
        assert "af" in Q.FETCH_VARIANT_POP_ARRAYS

    def test_fetch_variant_pop_arrays_region(self):
        assert "$start" in Q.FETCH_VARIANT_POP_ARRAYS_REGION
        assert "$end" in Q.FETCH_VARIANT_POP_ARRAYS_REGION
        assert "pop_ids" in Q.FETCH_VARIANT_POP_ARRAYS_REGION
