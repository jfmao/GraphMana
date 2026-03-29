"""Tests for QC-related Cypher query constants."""

from graphmana.db.queries import (
    QC_POPULATION_SUMMARY,
    QC_VARIANT_CHR_COUNTS,
    QC_VARIANT_SUMMARY,
    QC_VARIANT_TYPE_COUNTS,
    UPDATE_SAMPLE_QC_BATCH,
)


class TestVariantQCQueries:
    """Verify variant QC queries."""

    def test_summary_returns_counts(self):
        assert "n_variants" in QC_VARIANT_SUMMARY
        assert "count(v)" in QC_VARIANT_SUMMARY

    def test_summary_has_call_rate_stats(self):
        assert "mean_call_rate" in QC_VARIANT_SUMMARY
        assert "min_call_rate" in QC_VARIANT_SUMMARY
        assert "max_call_rate" in QC_VARIANT_SUMMARY

    def test_summary_has_af_stats(self):
        assert "mean_af" in QC_VARIANT_SUMMARY
        assert "af_total" in QC_VARIANT_SUMMARY

    def test_summary_has_low_call_rate_count(self):
        assert "n_low_call_rate" in QC_VARIANT_SUMMARY

    def test_summary_has_monomorphic_count(self):
        assert "n_monomorphic" in QC_VARIANT_SUMMARY

    def test_type_counts_groups_by_type(self):
        assert "variant_type" in QC_VARIANT_TYPE_COUNTS
        assert "count" in QC_VARIANT_TYPE_COUNTS
        assert "ORDER BY" in QC_VARIANT_TYPE_COUNTS

    def test_chr_counts_joins_chromosome(self):
        assert "Chromosome" in QC_VARIANT_CHR_COUNTS
        assert "chromosomeId" in QC_VARIANT_CHR_COUNTS


class TestPopulationSummaryQuery:
    """Verify population summary query."""

    def test_returns_population_id(self):
        assert "populationId" in QC_POPULATION_SUMMARY

    def test_counts_active_samples(self):
        assert "n_samples_active" in QC_POPULATION_SUMMARY
        assert "excluded" in QC_POPULATION_SUMMARY

    def test_returns_total_samples(self):
        assert "n_samples_total" in QC_POPULATION_SUMMARY


class TestUpdateSampleQCQuery:
    """Verify sample QC stat update query."""

    def test_uses_unwind(self):
        assert "UNWIND" in UPDATE_SAMPLE_QC_BATCH

    def test_sets_all_qc_fields(self):
        for field in ["n_het", "n_hom_alt", "heterozygosity", "call_rate"]:
            assert field in UPDATE_SAMPLE_QC_BATCH

    def test_matches_by_sample_id(self):
        assert "sampleId" in UPDATE_SAMPLE_QC_BATCH
