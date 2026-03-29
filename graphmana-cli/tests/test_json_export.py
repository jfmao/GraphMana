"""Tests for JSON export format."""

import json

import numpy as np
import pytest

from graphmana.export.json_export import DEFAULT_FIELDS


class TestDefaultFields:
    """Verify JSON default field configuration."""

    def test_default_fields_present(self):
        assert "variantId" in DEFAULT_FIELDS
        assert "chr" in DEFAULT_FIELDS
        assert "pos" in DEFAULT_FIELDS
        assert "ref" in DEFAULT_FIELDS
        assert "alt" in DEFAULT_FIELDS

    def test_includes_stats(self):
        assert "af_total" in DEFAULT_FIELDS
        assert "ac_total" in DEFAULT_FIELDS
        assert "an_total" in DEFAULT_FIELDS

    def test_default_count(self):
        assert len(DEFAULT_FIELDS) == 9


class TestJSONFieldExtraction:
    """Test field extraction logic for JSON output."""

    def test_extract_default_fields(self):
        props = {
            "variantId": "chr1_100_A_T",
            "chr": "chr1",
            "pos": 100,
            "ref": "A",
            "alt": "T",
            "variant_type": "SNP",
            "af_total": 0.25,
            "ac_total": 50,
            "an_total": 200,
        }
        obj = {col: props.get(col) for col in DEFAULT_FIELDS}
        assert obj["variantId"] == "chr1_100_A_T"
        assert obj["pos"] == 100
        assert obj["af_total"] == 0.25

    def test_missing_field_is_none(self):
        props = {"variantId": "v1", "chr": "chr1"}
        obj = {col: props.get(col) for col in DEFAULT_FIELDS}
        assert obj["ref"] is None
        assert obj["alt"] is None

    def test_custom_fields(self):
        props = {
            "variantId": "v1",
            "qual": 30.0,
            "filter": "PASS",
            "call_rate": 0.99,
        }
        fields = ["variantId", "qual", "filter", "call_rate"]
        obj = {col: props.get(col) for col in fields}
        assert obj["qual"] == 30.0
        assert obj["filter"] == "PASS"


class TestJSONSerializability:
    """Test that variant data can be serialized to JSON."""

    def test_basic_variant_to_json(self):
        props = {
            "variantId": "chr1_100_A_T",
            "chr": "chr1",
            "pos": 100,
            "ref": "A",
            "alt": "T",
            "variant_type": "SNP",
            "af_total": 0.25,
            "ac_total": 50,
            "an_total": 200,
        }
        obj = {col: props.get(col) for col in DEFAULT_FIELDS}
        result = json.dumps(obj)
        parsed = json.loads(result)
        assert parsed["variantId"] == "chr1_100_A_T"
        assert parsed["pos"] == 100

    def test_numpy_types_need_conversion(self):
        """numpy int/float types are not JSON serializable by default."""
        val = np.int64(42)
        with pytest.raises(TypeError):
            json.dumps({"val": val})
        # But converted int works
        json.dumps({"val": int(val)})

    def test_none_values_serialize(self):
        obj = {"variantId": "v1", "ref": None, "alt": None}
        result = json.dumps(obj)
        parsed = json.loads(result)
        assert parsed["ref"] is None

    def test_pretty_formatting(self):
        obj = {"variantId": "v1", "pos": 100}
        pretty = json.dumps(obj, indent=2)
        assert "\n" in pretty
        compact = json.dumps(obj)
        assert "\n" not in compact


class TestJSONGenotypes:
    """Test genotype encoding for JSON output."""

    def test_genotype_dict_structure(self):
        gt_codes = np.array([0, 1, 2, 3], dtype=np.int8)
        phase_bits = np.array([0, 1, 0, 0], dtype=np.uint8)
        sample_ids = ["S1", "S2", "S3", "S4"]

        genotypes = {}
        for i, sid in enumerate(sample_ids):
            genotypes[sid] = {"gt": int(gt_codes[i]), "phase": int(phase_bits[i])}

        assert genotypes["S1"]["gt"] == 0  # HomRef
        assert genotypes["S2"]["gt"] == 1  # Het
        assert genotypes["S2"]["phase"] == 1
        assert genotypes["S3"]["gt"] == 2  # HomAlt
        assert genotypes["S4"]["gt"] == 3  # Missing

    def test_genotype_json_serializable(self):
        genotypes = {
            "S1": {"gt": 0, "phase": 0},
            "S2": {"gt": 1, "phase": 1},
        }
        result = json.dumps({"genotypes": genotypes})
        parsed = json.loads(result)
        assert parsed["genotypes"]["S1"]["gt"] == 0
        assert parsed["genotypes"]["S2"]["phase"] == 1

    def test_json_lines_format(self):
        """Each variant should be one line of JSON."""
        lines = []
        for i in range(3):
            obj = {"variantId": f"v{i}", "pos": i * 100}
            lines.append(json.dumps(obj))
        content = "\n".join(lines) + "\n"
        parsed = [json.loads(line) for line in content.strip().split("\n")]
        assert len(parsed) == 3
        assert parsed[0]["variantId"] == "v0"
        assert parsed[2]["pos"] == 200
