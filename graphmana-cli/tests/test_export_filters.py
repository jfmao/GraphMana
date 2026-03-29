"""Tests for export-time variant filters."""

import pytest

from graphmana.filtering.export_filters import ExportFilter, ExportFilterConfig


class TestParseRegion:
    """Test region string parsing."""

    def test_valid_region(self):
        cfg = ExportFilterConfig(region="chr1:1000-2000")
        f = ExportFilter(cfg)
        result = f.parse_region()
        assert result == ("chr1", 1000, 2000)

    def test_no_region(self):
        f = ExportFilter(ExportFilterConfig())
        assert f.parse_region() is None

    def test_invalid_format_no_colon(self):
        cfg = ExportFilterConfig(region="chr1_1000_2000")
        f = ExportFilter(cfg)
        with pytest.raises(ValueError, match="Invalid region format"):
            f.parse_region()

    def test_invalid_format_no_dash(self):
        cfg = ExportFilterConfig(region="chr1:1000")
        f = ExportFilter(cfg)
        with pytest.raises(ValueError, match="Invalid region format"):
            f.parse_region()

    def test_ensembl_chr_name(self):
        cfg = ExportFilterConfig(region="22:1000-2000")
        f = ExportFilter(cfg)
        result = f.parse_region()
        assert result == ("22", 1000, 2000)


class TestVariantPasses:
    """Test post-query variant filtering."""

    def test_no_filters(self):
        f = ExportFilter(ExportFilterConfig())
        assert f.variant_passes({"af_total": 0.5, "call_rate": 1.0, "variant_type": "SNP"})

    def test_maf_min_pass(self):
        cfg = ExportFilterConfig(maf_min=0.01)
        f = ExportFilter(cfg)
        assert f.variant_passes({"af_total": 0.05})

    def test_maf_min_fail(self):
        cfg = ExportFilterConfig(maf_min=0.05)
        f = ExportFilter(cfg)
        assert not f.variant_passes({"af_total": 0.01})

    def test_maf_max_pass(self):
        cfg = ExportFilterConfig(maf_max=0.4)
        f = ExportFilter(cfg)
        assert f.variant_passes({"af_total": 0.3})

    def test_maf_max_fail(self):
        cfg = ExportFilterConfig(maf_max=0.1)
        f = ExportFilter(cfg)
        assert not f.variant_passes({"af_total": 0.4})

    def test_maf_high_af(self):
        """AF > 0.5 should compute MAF as 1 - AF."""
        cfg = ExportFilterConfig(maf_min=0.05)
        f = ExportFilter(cfg)
        # AF=0.95 → MAF=0.05, should pass
        assert f.variant_passes({"af_total": 0.95})
        # AF=0.99 → MAF=0.01, should fail
        assert not f.variant_passes({"af_total": 0.99})

    def test_call_rate_pass(self):
        cfg = ExportFilterConfig(min_call_rate=0.9)
        f = ExportFilter(cfg)
        assert f.variant_passes({"call_rate": 0.95})

    def test_call_rate_fail(self):
        cfg = ExportFilterConfig(min_call_rate=0.9)
        f = ExportFilter(cfg)
        assert not f.variant_passes({"call_rate": 0.8})

    def test_variant_type_pass(self):
        cfg = ExportFilterConfig(variant_types={"SNP", "INDEL"})
        f = ExportFilter(cfg)
        assert f.variant_passes({"variant_type": "SNP"})
        assert f.variant_passes({"variant_type": "INDEL"})

    def test_variant_type_fail(self):
        cfg = ExportFilterConfig(variant_types={"SNP"})
        f = ExportFilter(cfg)
        assert not f.variant_passes({"variant_type": "INDEL"})

    def test_combined_filters(self):
        cfg = ExportFilterConfig(maf_min=0.01, min_call_rate=0.9, variant_types={"SNP"})
        f = ExportFilter(cfg)
        # All pass
        assert f.variant_passes({"af_total": 0.1, "call_rate": 0.95, "variant_type": "SNP"})
        # MAF too low
        assert not f.variant_passes({"af_total": 0.001, "call_rate": 0.95, "variant_type": "SNP"})
        # Call rate too low
        assert not f.variant_passes({"af_total": 0.1, "call_rate": 0.8, "variant_type": "SNP"})
        # Wrong type
        assert not f.variant_passes({"af_total": 0.1, "call_rate": 0.95, "variant_type": "INDEL"})

    def test_none_af_treated_as_zero(self):
        cfg = ExportFilterConfig(maf_min=0.01)
        f = ExportFilter(cfg)
        assert not f.variant_passes({"af_total": None})

    def test_none_call_rate_treated_as_one(self):
        cfg = ExportFilterConfig(min_call_rate=0.9)
        f = ExportFilter(cfg)
        assert f.variant_passes({"call_rate": None})


class TestGetTargetChromosomes:
    """Test chromosome selection logic."""

    def test_all_available(self):
        f = ExportFilter(ExportFilterConfig())
        result = f.get_target_chromosomes(["chr1", "chr2", "chr3"])
        assert result == ["chr1", "chr2", "chr3"]

    def test_filtered_subset(self):
        cfg = ExportFilterConfig(chromosomes=["chr1", "chr3"])
        f = ExportFilter(cfg)
        result = f.get_target_chromosomes(["chr1", "chr2", "chr3"])
        assert result == ["chr1", "chr3"]

    def test_region_overrides_chromosomes(self):
        cfg = ExportFilterConfig(region="chr2:100-200", chromosomes=["chr1", "chr3"])
        f = ExportFilter(cfg)
        result = f.get_target_chromosomes(["chr1", "chr2", "chr3"])
        assert result == ["chr2"]

    def test_region_chr_not_available(self):
        cfg = ExportFilterConfig(region="chrX:100-200")
        f = ExportFilter(cfg)
        result = f.get_target_chromosomes(["chr1", "chr2"])
        assert result == []

    def test_filter_chr_not_available(self):
        cfg = ExportFilterConfig(chromosomes=["chrX"])
        f = ExportFilter(cfg)
        result = f.get_target_chromosomes(["chr1", "chr2"])
        assert result == []


class TestPopulationFilter:
    """Test population filter helper."""

    def test_no_population_filter(self):
        f = ExportFilter(ExportFilterConfig())
        assert not f.has_population_filter()
        assert f.populations is None

    def test_with_populations(self):
        cfg = ExportFilterConfig(populations=["EUR", "AFR"])
        f = ExportFilter(cfg)
        assert f.has_population_filter()
        assert f.populations == ["EUR", "AFR"]

    def test_empty_list(self):
        cfg = ExportFilterConfig(populations=[])
        f = ExportFilter(cfg)
        assert not f.has_population_filter()


class TestSampleIdFilter:
    """Test sample ID filter helper."""

    def test_no_sample_id_filter(self):
        f = ExportFilter(ExportFilterConfig())
        assert not f.has_sample_id_filter()
        assert f.sample_ids is None

    def test_with_sample_ids(self):
        cfg = ExportFilterConfig(sample_ids=["S1", "S2", "S3"])
        f = ExportFilter(cfg)
        assert f.has_sample_id_filter()
        assert f.sample_ids == ["S1", "S2", "S3"]

    def test_empty_list(self):
        cfg = ExportFilterConfig(sample_ids=[])
        f = ExportFilter(cfg)
        assert not f.has_sample_id_filter()

    def test_sample_ids_default(self):
        cfg = ExportFilterConfig()
        assert cfg.sample_ids is None


class TestCohortFilter:
    """Test cohort filter helper."""

    def test_no_cohort_filter(self):
        f = ExportFilter(ExportFilterConfig())
        assert not f.has_cohort_filter()
        assert f.cohort is None

    def test_with_cohort(self):
        cfg = ExportFilterConfig(cohort="my_cohort")
        f = ExportFilter(cfg)
        assert f.has_cohort_filter()
        assert f.cohort == "my_cohort"

    def test_empty_string_cohort(self):
        cfg = ExportFilterConfig(cohort="")
        f = ExportFilter(cfg)
        assert not f.has_cohort_filter()

    def test_cohort_field_default(self):
        cfg = ExportFilterConfig()
        assert cfg.cohort is None
