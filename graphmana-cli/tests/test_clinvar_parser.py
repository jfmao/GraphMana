"""Tests for ClinVar VCF annotation parser."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from graphmana.annotation.parsers.clinvar import ClinVarParser, _get_info_str


class TestClinVarLoadBatch:
    """Test _load_batch with mocked connection."""

    def test_load_batch(self):
        mock_conn = MagicMock()
        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_record = {"matched": 2}
        mock_result.single.return_value = mock_record
        mock_session.run.return_value = mock_result
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)
        mock_conn.driver.session.return_value = mock_session

        parser = ClinVarParser(mock_conn)
        batch = [
            {
                "variantId": "chr1:100:A:G",
                "clinvar_id": "12345",
                "clinvar_sig": "Pathogenic",
                "clinvar_review": "criteria_provided,_single_submitter",
                "clinvar_disease": "Breast_cancer",
            },
            {
                "variantId": "chr1:200:T:C",
                "clinvar_id": "67890",
                "clinvar_sig": "Benign",
                "clinvar_review": "no_assertion_criteria_provided",
                "clinvar_disease": "not_specified",
            },
        ]
        matched = parser._load_batch(batch)
        assert matched == 2

    def test_source_name(self):
        parser = ClinVarParser(MagicMock())
        assert parser.source_name == "ClinVar"


class TestGetInfoStr:
    """Test _get_info_str helper."""

    def test_none_variant(self):
        mock_variant = MagicMock()
        mock_variant.INFO.get.return_value = None
        assert _get_info_str(mock_variant, "CLNSIG") is None

    def test_valid_value(self):
        mock_variant = MagicMock()
        mock_variant.INFO.get.return_value = "Pathogenic"
        assert _get_info_str(mock_variant, "CLNSIG") == "Pathogenic"

    def test_missing_attribute(self):
        mock_variant = MagicMock()
        mock_variant.INFO.get.side_effect = KeyError("no such field")
        assert _get_info_str(mock_variant, "MISSING_FIELD") is None
