"""Tests for annotation-based export filters."""

import pytest

from graphmana.filtering.export_filters import ExportFilter, ExportFilterConfig


class TestAnnotationFilterConfig:
    """Test ExportFilterConfig annotation fields."""

    def test_default_none(self):
        cfg = ExportFilterConfig()
        assert cfg.consequences is None
        assert cfg.impacts is None
        assert cfg.genes is None
        assert cfg.cadd_min is None
        assert cfg.cadd_max is None
        assert cfg.annotation_version is None

    def test_set_consequences(self):
        cfg = ExportFilterConfig(consequences=["missense_variant", "stop_gained"])
        assert cfg.consequences == ["missense_variant", "stop_gained"]

    def test_set_impacts(self):
        cfg = ExportFilterConfig(impacts=["HIGH", "MODERATE"])
        assert cfg.impacts == ["HIGH", "MODERATE"]

    def test_set_genes(self):
        cfg = ExportFilterConfig(genes=["BRCA1", "TP53"])
        assert cfg.genes == ["BRCA1", "TP53"]

    def test_set_cadd(self):
        cfg = ExportFilterConfig(cadd_min=15.0, cadd_max=30.0)
        assert cfg.cadd_min == 15.0
        assert cfg.cadd_max == 30.0

    def test_set_annotation_version(self):
        cfg = ExportFilterConfig(annotation_version="VEP_v110")
        assert cfg.annotation_version == "VEP_v110"


class TestHasAnnotationFilter:
    """Test has_annotation_filter() detection."""

    def test_no_filters(self):
        f = ExportFilter(ExportFilterConfig())
        assert not f.has_annotation_filter()

    def test_consequences_only(self):
        cfg = ExportFilterConfig(consequences=["missense_variant"])
        f = ExportFilter(cfg)
        assert f.has_annotation_filter()

    def test_impacts_only(self):
        cfg = ExportFilterConfig(impacts=["HIGH"])
        f = ExportFilter(cfg)
        assert f.has_annotation_filter()

    def test_genes_only(self):
        cfg = ExportFilterConfig(genes=["BRCA1"])
        f = ExportFilter(cfg)
        assert f.has_annotation_filter()

    def test_annotation_version_only(self):
        cfg = ExportFilterConfig(annotation_version="VEP_v110")
        f = ExportFilter(cfg)
        assert f.has_annotation_filter()

    def test_empty_lists_not_active(self):
        cfg = ExportFilterConfig(consequences=[], impacts=[], genes=[])
        f = ExportFilter(cfg)
        assert not f.has_annotation_filter()

    def test_empty_version_not_active(self):
        cfg = ExportFilterConfig(annotation_version="")
        f = ExportFilter(cfg)
        assert not f.has_annotation_filter()

    def test_cadd_alone_not_annotation_filter(self):
        """CADD is a separate path — has_annotation_filter should be False."""
        cfg = ExportFilterConfig(cadd_min=20.0)
        f = ExportFilter(cfg)
        assert not f.has_annotation_filter()


class TestHasCaddFilter:
    """Test has_cadd_filter() detection."""

    def test_no_cadd(self):
        f = ExportFilter(ExportFilterConfig())
        assert not f.has_cadd_filter()

    def test_cadd_min(self):
        cfg = ExportFilterConfig(cadd_min=15.0)
        f = ExportFilter(cfg)
        assert f.has_cadd_filter()

    def test_cadd_max(self):
        cfg = ExportFilterConfig(cadd_max=30.0)
        f = ExportFilter(cfg)
        assert f.has_cadd_filter()

    def test_cadd_both(self):
        cfg = ExportFilterConfig(cadd_min=10.0, cadd_max=30.0)
        f = ExportFilter(cfg)
        assert f.has_cadd_filter()


class TestCheckCadd:
    """Test _check_cadd() post-query filter."""

    def test_no_filter_passes(self):
        f = ExportFilter(ExportFilterConfig())
        assert f._check_cadd({"cadd_phred": 25.0})
        assert f._check_cadd({})

    def test_cadd_min_pass(self):
        cfg = ExportFilterConfig(cadd_min=20.0)
        f = ExportFilter(cfg)
        assert f._check_cadd({"cadd_phred": 25.0})

    def test_cadd_min_fail(self):
        cfg = ExportFilterConfig(cadd_min=20.0)
        f = ExportFilter(cfg)
        assert not f._check_cadd({"cadd_phred": 15.0})

    def test_cadd_max_pass(self):
        cfg = ExportFilterConfig(cadd_max=30.0)
        f = ExportFilter(cfg)
        assert f._check_cadd({"cadd_phred": 25.0})

    def test_cadd_max_fail(self):
        cfg = ExportFilterConfig(cadd_max=20.0)
        f = ExportFilter(cfg)
        assert not f._check_cadd({"cadd_phred": 25.0})

    def test_cadd_none_fails_when_filter_active(self):
        """Variants without CADD scores should fail when filter is active."""
        cfg = ExportFilterConfig(cadd_min=10.0)
        f = ExportFilter(cfg)
        assert not f._check_cadd({"cadd_phred": None})
        assert not f._check_cadd({})

    def test_cadd_range(self):
        cfg = ExportFilterConfig(cadd_min=15.0, cadd_max=30.0)
        f = ExportFilter(cfg)
        assert f._check_cadd({"cadd_phred": 20.0})
        assert not f._check_cadd({"cadd_phred": 10.0})
        assert not f._check_cadd({"cadd_phred": 35.0})


class TestVariantPassesWithCadd:
    """Test that CADD filter integrates into variant_passes()."""

    def test_cadd_integrated(self):
        cfg = ExportFilterConfig(cadd_min=20.0)
        f = ExportFilter(cfg)
        assert f.variant_passes({"cadd_phred": 25.0})
        assert not f.variant_passes({"cadd_phred": 10.0})

    def test_combined_cadd_and_maf(self):
        cfg = ExportFilterConfig(cadd_min=15.0, maf_min=0.01)
        f = ExportFilter(cfg)
        # Both pass
        assert f.variant_passes({"cadd_phred": 20.0, "af_total": 0.05})
        # CADD too low
        assert not f.variant_passes({"cadd_phred": 10.0, "af_total": 0.05})
        # MAF too low
        assert not f.variant_passes({"cadd_phred": 20.0, "af_total": 0.001})


class TestGetAnnotationFilterParams:
    """Test Cypher parameter dict generation."""

    def test_all_none(self):
        f = ExportFilter(ExportFilterConfig())
        params = f.get_annotation_filter_params()
        assert params == {
            "consequences": None,
            "impacts": None,
            "genes": None,
            "annotation_version": None,
        }

    def test_partial(self):
        cfg = ExportFilterConfig(
            consequences=["missense_variant"],
            impacts=["HIGH", "MODERATE"],
        )
        f = ExportFilter(cfg)
        params = f.get_annotation_filter_params()
        assert params["consequences"] == ["missense_variant"]
        assert params["impacts"] == ["HIGH", "MODERATE"]
        assert params["genes"] is None
        assert params["annotation_version"] is None

    def test_all_set(self):
        cfg = ExportFilterConfig(
            consequences=["stop_gained"],
            impacts=["HIGH"],
            genes=["BRCA1"],
            annotation_version="VEP_v110",
        )
        f = ExportFilter(cfg)
        params = f.get_annotation_filter_params()
        assert params["consequences"] == ["stop_gained"]
        assert params["impacts"] == ["HIGH"]
        assert params["genes"] == ["BRCA1"]
        assert params["annotation_version"] == "VEP_v110"


class TestFilterConfigSerialization:
    """Test that _get_filter_config_dict handles new fields."""

    def test_roundtrip(self):
        from graphmana.export.parallel import _get_filter_config_dict

        cfg = ExportFilterConfig(
            populations=["EUR"],
            maf_min=0.01,
            consequences=["missense_variant"],
            impacts=["HIGH"],
            genes=["BRCA1"],
            cadd_min=15.0,
            cadd_max=30.0,
            annotation_version="VEP_v110",
            sample_ids=["S1", "S2"],
        )
        d = _get_filter_config_dict(cfg)
        assert d["consequences"] == ["missense_variant"]
        assert d["impacts"] == ["HIGH"]
        assert d["genes"] == ["BRCA1"]
        assert d["cadd_min"] == 15.0
        assert d["cadd_max"] == 30.0
        assert d["annotation_version"] == "VEP_v110"
        assert d["sample_ids"] == ["S1", "S2"]
        # chromosomes should be None (overridden per worker)
        assert d["chromosomes"] is None

        # Reconstruct config from dict
        cfg2 = ExportFilterConfig(**d)
        assert cfg2.consequences == ["missense_variant"]
        assert cfg2.impacts == ["HIGH"]
        assert cfg2.genes == ["BRCA1"]
        assert cfg2.cadd_min == 15.0
        assert cfg2.cadd_max == 30.0
        assert cfg2.annotation_version == "VEP_v110"
        assert cfg2.sample_ids == ["S1", "S2"]

    def test_none_config(self):
        from graphmana.export.parallel import _get_filter_config_dict

        assert _get_filter_config_dict(None) is None
