"""Tests for VEP parser — annotation format detection and allele resolution."""

import pytest

from graphmana.ingest.vep_parser import (
    HAS_CONSEQUENCE_HEADER,
    VEPParser,
    _parse_pred_score,
)

# ---------------------------------------------------------------------------
# Prediction score parsing
# ---------------------------------------------------------------------------


class TestParsePredScore:
    def test_deleterious_with_score(self):
        pred, score = _parse_pred_score("deleterious(0.02)")
        assert pred == "deleterious"
        assert score == "0.02"

    def test_benign_with_score(self):
        pred, score = _parse_pred_score("benign(0.95)")
        assert pred == "benign"
        assert score == "0.95"

    def test_empty_string(self):
        pred, score = _parse_pred_score("")
        assert pred == ""
        assert score == ""

    def test_prediction_only_no_score(self):
        pred, score = _parse_pred_score("tolerated")
        assert pred == "tolerated"
        assert score == ""

    def test_none_like_empty(self):
        """Empty input returns empty tuple."""
        assert _parse_pred_score("") == ("", "")


# ---------------------------------------------------------------------------
# Variant ID resolution (VEP CSQ format)
# ---------------------------------------------------------------------------


class TestResolveVariantId:
    def test_snp_direct_match(self):
        """SNP: VEP allele 'T' matches ALT 'T' directly."""
        result = VEPParser._resolve_variant_id(
            "T",
            "A",
            ["T"],
            {"T": "chr1:100:A:T"},
            {"T"},
        )
        assert result == "chr1:100:A:T"

    def test_deletion_dash(self):
        """Deletion: VEP uses '-' for the deleted bases."""
        result = VEPParser._resolve_variant_id(
            "-",
            "ACG",
            ["A"],
            {"A": "chr1:100:ACG:A"},
            {"A"},
        )
        assert result == "chr1:100:ACG:A"

    def test_insertion_trimmed(self):
        """Insertion: VEP uses only inserted bases (without ref padding)."""
        result = VEPParser._resolve_variant_id(
            "CG",
            "A",
            ["ACG"],
            {"ACG": "chr1:100:A:ACG"},
            {"ACG"},
        )
        assert result == "chr1:100:A:ACG"

    def test_no_match_returns_none(self):
        result = VEPParser._resolve_variant_id(
            "G",
            "A",
            ["T"],
            {"T": "chr1:100:A:T"},
            {"T"},
        )
        assert result is None


# ---------------------------------------------------------------------------
# ANN variant ID resolution (SnpEff format)
# ---------------------------------------------------------------------------


class TestResolveAnnVariantId:
    def test_direct_match(self):
        result = VEPParser._resolve_ann_variant_id(
            "T",
            {"T": "chr1:100:A:T"},
            {"T"},
        )
        assert result == "chr1:100:A:T"

    def test_not_in_matched_alts(self):
        """Allele exists in alt_to_vid but not in matched_alts."""
        result = VEPParser._resolve_ann_variant_id(
            "T",
            {"T": "chr1:100:A:T"},
            set(),  # empty matched_alts
        )
        assert result is None

    def test_allele_not_in_alt_to_vid(self):
        result = VEPParser._resolve_ann_variant_id(
            "G",
            {"T": "chr1:100:A:T"},
            {"T"},
        )
        assert result is None


# ---------------------------------------------------------------------------
# CSQ format parsing
# ---------------------------------------------------------------------------


class TestCSQFormatParsing:
    def test_parse_format_string(self):
        desc = (
            "Consequence annotations. Format: "
            "Allele|Consequence|IMPACT|SYMBOL|Gene|Feature_type|Feature|BIOTYPE|SIFT|PolyPhen"
        )
        field_idx = VEPParser._parse_csq_format_from_desc(desc)
        assert field_idx["allele"] == 0
        assert field_idx["consequence"] == 1
        assert field_idx["impact"] == 2
        assert field_idx["gene"] == 4

    def test_missing_format_keyword_raises(self):
        desc = "Some description without Format keyword"
        with pytest.raises(ValueError, match="Format"):
            VEPParser._parse_csq_format_from_desc(desc)

    def test_missing_required_field_raises(self):
        desc = "Format: Allele|Consequence"  # missing Gene
        with pytest.raises(ValueError, match="required field"):
            VEPParser._parse_csq_format_from_desc(desc)


# ---------------------------------------------------------------------------
# ANN format parsing
# ---------------------------------------------------------------------------


class TestANNFormat:
    def test_fixed_16_field_layout(self):
        desc = "Functional annotations: 'Allele | Annotation | ...'"
        field_idx = VEPParser._parse_ann_format(desc)
        assert field_idx["allele"] == 0
        assert field_idx["consequence"] == 1
        assert field_idx["impact"] == 2
        assert field_idx["symbol"] == 3
        assert field_idx["gene"] == 4
        assert field_idx["feature_type"] == 5
        assert field_idx["feature"] == 6
        assert field_idx["biotype"] == 7


# ---------------------------------------------------------------------------
# HAS_CONSEQUENCE header
# ---------------------------------------------------------------------------


class TestHasConsequenceHeader:
    def test_has_graphmana_fields(self):
        """GraphMana adds annotation_source and annotation_version."""
        assert "annotation_source" in HAS_CONSEQUENCE_HEADER
        assert "annotation_version" in HAS_CONSEQUENCE_HEADER
