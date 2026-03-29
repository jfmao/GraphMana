"""Tests for structural variant support: parsing, CSV emission, and export filtering."""


from graphmana.filtering.export_filters import ExportFilter, ExportFilterConfig
from graphmana.ingest.vcf_parser import VariantRecord


class TestVariantRecordSVFields:
    """Test SV fields on VariantRecord."""

    def _make_record(self, **overrides):
        """Create a VariantRecord with default values."""
        defaults = dict(
            id="chr1:100:A:T",
            chr="chr1",
            pos=100,
            ref="A",
            alt="T",
            variant_type="SNP",
            ac=[0],
            an=[10],
            af=[0.0],
            het_count=[0],
            hom_alt_count=[0],
            het_exp=[0.0],
        )
        defaults.update(overrides)
        return VariantRecord(**defaults)

    def test_sv_fields_default_none(self):
        rec = self._make_record()
        assert rec.sv_type is None
        assert rec.sv_len is None
        assert rec.sv_end is None

    def test_sv_fields_populated(self):
        rec = self._make_record(
            alt="<DEL>",
            variant_type="SV",
            sv_type="DEL",
            sv_len=-500,
            sv_end=600,
        )
        assert rec.sv_type == "DEL"
        assert rec.sv_len == -500
        assert rec.sv_end == 600


class TestSVExportFilter:
    """Test SV type export filtering."""

    def test_no_sv_filter_passes_all(self):
        f = ExportFilter(ExportFilterConfig())
        assert f.variant_passes({"sv_type": "DEL"})
        assert f.variant_passes({"sv_type": None})
        assert f.variant_passes({})

    def test_sv_type_filter_pass(self):
        cfg = ExportFilterConfig(sv_types={"DEL", "DUP"})
        f = ExportFilter(cfg)
        assert f.variant_passes({"sv_type": "DEL"})
        assert f.variant_passes({"sv_type": "DUP"})

    def test_sv_type_filter_fail(self):
        cfg = ExportFilterConfig(sv_types={"DEL", "DUP"})
        f = ExportFilter(cfg)
        assert not f.variant_passes({"sv_type": "INV"})

    def test_sv_type_none_fails_when_filter_active(self):
        cfg = ExportFilterConfig(sv_types={"DEL"})
        f = ExportFilter(cfg)
        assert not f.variant_passes({"sv_type": None})
        assert not f.variant_passes({})

    def test_sv_type_combined_with_other_filters(self):
        cfg = ExportFilterConfig(
            sv_types={"DEL", "DUP"},
            maf_min=0.01,
        )
        f = ExportFilter(cfg)
        # Both pass
        assert f.variant_passes({"sv_type": "DEL", "af_total": 0.1})
        # SV type fails
        assert not f.variant_passes({"sv_type": "INV", "af_total": 0.1})
        # MAF fails
        assert not f.variant_passes({"sv_type": "DEL", "af_total": 0.001})


class TestSVCSVColumns:
    """Test SV columns in CSV emitter."""

    def test_variant_header_includes_sv_fields(self):
        from graphmana.ingest.csv_emitter import VARIANT_HEADER

        assert "sv_type" in VARIANT_HEADER
        assert "sv_len:long" in VARIANT_HEADER
        assert "sv_end:long" in VARIANT_HEADER

    def test_sv_fields_positions_in_header(self):
        from graphmana.ingest.csv_emitter import VARIANT_HEADER

        sv_type_idx = VARIANT_HEADER.index("sv_type")
        sv_len_idx = VARIANT_HEADER.index("sv_len:long")
        sv_end_idx = VARIANT_HEADER.index("sv_end:long")
        # They should be consecutive
        assert sv_len_idx == sv_type_idx + 1
        assert sv_end_idx == sv_len_idx + 1


class TestExportFilterConfigSV:
    """Test ExportFilterConfig SV fields."""

    def test_default_sv_types_none(self):
        cfg = ExportFilterConfig()
        assert cfg.sv_types is None

    def test_sv_types_set(self):
        cfg = ExportFilterConfig(sv_types={"DEL", "DUP", "INV", "INS", "BND", "CNV"})
        assert "DEL" in cfg.sv_types
        assert "CNV" in cfg.sv_types
        assert len(cfg.sv_types) == 6
