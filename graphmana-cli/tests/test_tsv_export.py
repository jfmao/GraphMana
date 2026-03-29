"""Tests for TSV export (pure function tests, no Neo4j)."""

from graphmana.export.tsv_export import AVAILABLE_COLUMNS, DEFAULT_COLUMNS


class TestDefaultColumns:
    """Verify TSV default column configuration."""

    def test_default_columns(self):
        assert "variantId" in DEFAULT_COLUMNS
        assert "chr" in DEFAULT_COLUMNS
        assert "pos" in DEFAULT_COLUMNS
        assert "ref" in DEFAULT_COLUMNS
        assert "alt" in DEFAULT_COLUMNS
        assert "variant_type" in DEFAULT_COLUMNS
        assert "af_total" in DEFAULT_COLUMNS

    def test_default_count(self):
        assert len(DEFAULT_COLUMNS) == 7

    def test_defaults_subset_of_available(self):
        for col in DEFAULT_COLUMNS:
            assert col in AVAILABLE_COLUMNS


class TestAvailableColumns:
    """Verify all available columns are listed."""

    def test_includes_core_fields(self):
        for col in ["variantId", "chr", "pos", "ref", "alt"]:
            assert col in AVAILABLE_COLUMNS

    def test_includes_stats_fields(self):
        for col in ["ac_total", "an_total", "af_total", "call_rate"]:
            assert col in AVAILABLE_COLUMNS

    def test_includes_annotation_fields(self):
        for col in ["ancestral_allele", "is_polarized"]:
            assert col in AVAILABLE_COLUMNS

    def test_includes_quality_fields(self):
        for col in ["qual", "filter"]:
            assert col in AVAILABLE_COLUMNS


class TestColumnExtraction:
    """Test extracting specific columns from variant property dicts."""

    def test_extract_defaults(self):
        props = {
            "variantId": "chr1_100_A_T",
            "chr": "chr1",
            "pos": 100,
            "ref": "A",
            "alt": "T",
            "variant_type": "SNP",
            "af_total": 0.25,
        }
        values = [str(props.get(col, ".")) for col in DEFAULT_COLUMNS]
        assert values[0] == "chr1_100_A_T"
        assert values[2] == "100"
        assert values[6] == "0.25"

    def test_missing_column(self):
        props = {"variantId": "v1", "chr": "chr1", "pos": 100}
        values = [
            str(props.get(col, ".")) if props.get(col) is not None else "."
            for col in DEFAULT_COLUMNS
        ]
        # Missing columns default to "."
        assert values[3] == "."  # ref
        assert values[4] == "."  # alt
