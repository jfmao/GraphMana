"""Tests for expanded GraphManaClient methods."""

import inspect

from graphmana_py import GraphManaClient
from graphmana_py._unpack import unpack_phase, unpack_ploidy
from graphmana_py import _queries as Q


class TestExpandedClientInterface:
    """Verify new methods exist on GraphManaClient."""

    def test_has_gene_variants(self):
        assert hasattr(GraphManaClient, "gene_variants")
        assert callable(GraphManaClient.gene_variants)

    def test_has_annotated_variants(self):
        assert hasattr(GraphManaClient, "annotated_variants")
        assert callable(GraphManaClient.annotated_variants)

    def test_has_cohort_samples(self):
        assert hasattr(GraphManaClient, "cohort_samples")
        assert callable(GraphManaClient.cohort_samples)

    def test_has_filtered_variants(self):
        assert hasattr(GraphManaClient, "filtered_variants")
        assert callable(GraphManaClient.filtered_variants)

    def test_has_to_vcf(self):
        assert hasattr(GraphManaClient, "to_vcf")
        assert callable(GraphManaClient.to_vcf)

    def test_has_to_plink(self):
        assert hasattr(GraphManaClient, "to_plink")
        assert callable(GraphManaClient.to_plink)

    def test_has_to_treemix(self):
        assert hasattr(GraphManaClient, "to_treemix")
        assert callable(GraphManaClient.to_treemix)


class TestFilteredVariantsSignature:
    """Verify filtered_variants method accepts expected parameters."""

    def test_accepts_chr(self):
        sig = inspect.signature(GraphManaClient.filtered_variants)
        assert "chr" in sig.parameters

    def test_accepts_start_end(self):
        sig = inspect.signature(GraphManaClient.filtered_variants)
        assert "start" in sig.parameters
        assert "end" in sig.parameters

    def test_accepts_variant_type(self):
        sig = inspect.signature(GraphManaClient.filtered_variants)
        assert "variant_type" in sig.parameters

    def test_accepts_maf_range(self):
        sig = inspect.signature(GraphManaClient.filtered_variants)
        assert "maf_min" in sig.parameters
        assert "maf_max" in sig.parameters

    def test_all_params_keyword_only(self):
        sig = inspect.signature(GraphManaClient.filtered_variants)
        for name, param in sig.parameters.items():
            if name == "self":
                continue
            assert param.kind == inspect.Parameter.KEYWORD_ONLY


class TestNewQueries:
    """Verify new query strings exist and have correct placeholders."""

    def test_gene_variants_query(self):
        assert hasattr(Q, "GENE_VARIANTS")
        assert "$gene_symbol" in Q.GENE_VARIANTS
        assert "HAS_CONSEQUENCE" in Q.GENE_VARIANTS

    def test_annotated_variants_query(self):
        assert hasattr(Q, "ANNOTATED_VARIANTS")
        assert "$annotation_version" in Q.ANNOTATED_VARIANTS

    def test_cohort_samples_query(self):
        assert hasattr(Q, "COHORT_SAMPLES")
        assert "$cohort_name" in Q.COHORT_SAMPLES

    def test_filtered_variants_query(self):
        assert hasattr(Q, "FILTERED_VARIANTS")
        assert "$chr" in Q.FILTERED_VARIANTS
        assert "$start" in Q.FILTERED_VARIANTS
        assert "$end" in Q.FILTERED_VARIANTS
        assert "$variant_type" in Q.FILTERED_VARIANTS
        assert "$maf_min" in Q.FILTERED_VARIANTS
        assert "$maf_max" in Q.FILTERED_VARIANTS

    def test_fetch_samples_by_population_query(self):
        assert hasattr(Q, "FETCH_SAMPLES_BY_POPULATION")
        assert "$populations" in Q.FETCH_SAMPLES_BY_POPULATION


class TestUnpackImports:
    """Verify phase and ploidy unpack functions are importable."""

    def test_unpack_phase_importable(self):
        assert callable(unpack_phase)

    def test_unpack_ploidy_importable(self):
        assert callable(unpack_ploidy)
