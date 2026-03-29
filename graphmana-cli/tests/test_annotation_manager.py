"""Tests for AnnotationManager and CSV reading helpers."""

import csv
import inspect

import pytest

from graphmana.annotation.manager import (
    DEFAULT_BATCH_SIZE,
    AnnotationManager,
    _read_edge_csv,
    _read_gene_csv,
    _to_float_or_none,
)


class TestAnnotationManagerInterface:
    """Verify AnnotationManager class interface."""

    def test_class_exists(self):
        assert AnnotationManager is not None

    def test_has_load_method(self):
        assert hasattr(AnnotationManager, "load")
        assert callable(AnnotationManager.load)

    def test_has_list_method(self):
        assert hasattr(AnnotationManager, "list")
        assert callable(AnnotationManager.list)

    def test_has_get_method(self):
        assert hasattr(AnnotationManager, "get")
        assert callable(AnnotationManager.get)

    def test_has_remove_method(self):
        assert hasattr(AnnotationManager, "remove")
        assert callable(AnnotationManager.remove)

    def test_load_signature(self):
        sig = inspect.signature(AnnotationManager.load)
        params = list(sig.parameters.keys())
        assert "input_path" in params
        assert "version" in params
        assert "mode" in params
        assert "annotation_type" in params
        assert "description" in params
        assert "batch_size" in params

    def test_remove_signature(self):
        sig = inspect.signature(AnnotationManager.remove)
        params = list(sig.parameters.keys())
        assert "version" in params
        assert "cleanup_genes" in params

    def test_default_batch_size(self):
        assert DEFAULT_BATCH_SIZE == 5000


class TestReadGeneCSV:
    """Test _read_gene_csv helper."""

    def test_reads_gene_csv(self, tmp_path):
        csv_path = tmp_path / "gene_nodes.csv"
        with open(csv_path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["geneId:ID(Gene)", ":LABEL", "symbol", "biotype"])
            w.writerow(["ENSG00000139618", "Gene", "BRCA2", "protein_coding"])
            w.writerow(["ENSG00000141510", "Gene", "TP53", "protein_coding"])

        genes = _read_gene_csv(csv_path)
        assert len(genes) == 2
        assert genes[0]["geneId"] == "ENSG00000139618"
        assert genes[0]["symbol"] == "BRCA2"
        assert genes[0]["biotype"] == "protein_coding"
        assert genes[1]["geneId"] == "ENSG00000141510"

    def test_empty_csv(self, tmp_path):
        csv_path = tmp_path / "gene_nodes.csv"
        with open(csv_path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["geneId:ID(Gene)", ":LABEL", "symbol", "biotype"])

        genes = _read_gene_csv(csv_path)
        assert genes == []


class TestReadEdgeCSV:
    """Test _read_edge_csv helper."""

    def test_reads_edge_csv(self, tmp_path):
        csv_path = tmp_path / "has_consequence_edges.csv"
        with open(csv_path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(
                [
                    ":START_ID(Variant)",
                    ":END_ID(Gene)",
                    ":TYPE",
                    "consequence",
                    "impact",
                    "feature",
                    "feature_type",
                    "sift_score:float",
                    "sift_pred",
                    "polyphen_score:float",
                    "polyphen_pred",
                    "cadd_phred:float",
                    "revel:float",
                    "annotation_source",
                    "annotation_version",
                ]
            )
            w.writerow(
                [
                    "chr1:100:A:T",
                    "ENSG00000139618",
                    "HAS_CONSEQUENCE",
                    "missense_variant",
                    "MODERATE",
                    "ENST00000380152",
                    "Transcript",
                    "0.02",
                    "deleterious",
                    "0.95",
                    "probably_damaging",
                    "25.3",
                    "0.87",
                    "VEP",
                    "v110",
                ]
            )
            w.writerow(
                [
                    "chr1:200:G:C",
                    "ENSG00000141510",
                    "HAS_CONSEQUENCE",
                    "synonymous_variant",
                    "LOW",
                    "ENST00000269305",
                    "Transcript",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "VEP",
                    "v110",
                ]
            )

        edges = _read_edge_csv(csv_path, "VEP")
        assert len(edges) == 2

        e0 = edges[0]
        assert e0["variantId"] == "chr1:100:A:T"
        assert e0["geneId"] == "ENSG00000139618"
        assert e0["consequence"] == "missense_variant"
        assert e0["sift_score"] == pytest.approx(0.02)
        assert e0["polyphen_score"] == pytest.approx(0.95)
        assert e0["cadd_phred"] == pytest.approx(25.3)
        assert e0["revel"] == pytest.approx(0.87)
        assert e0["annotation_source"] == "VEP"
        assert e0["annotation_version"] == "v110"

        e1 = edges[1]
        assert e1["sift_score"] is None
        assert e1["polyphen_score"] is None
        assert e1["cadd_phred"] is None
        assert e1["revel"] is None

    def test_detected_source_overrides(self, tmp_path):
        csv_path = tmp_path / "has_consequence_edges.csv"
        with open(csv_path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(
                [
                    ":START_ID(Variant)",
                    ":END_ID(Gene)",
                    ":TYPE",
                    "consequence",
                    "impact",
                    "feature",
                    "feature_type",
                    "sift_score:float",
                    "sift_pred",
                    "polyphen_score:float",
                    "polyphen_pred",
                    "cadd_phred:float",
                    "revel:float",
                    "annotation_source",
                    "annotation_version",
                ]
            )
            w.writerow(
                [
                    "chr1:100:A:T",
                    "ENSG00000139618",
                    "HAS_CONSEQUENCE",
                    "missense",
                    "MODERATE",
                    "feat1",
                    "Transcript",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "VEP",
                    "v1",
                ]
            )

        edges = _read_edge_csv(csv_path, "SnpEff")
        assert edges[0]["annotation_source"] == "SnpEff"


class TestToFloatOrNone:
    """Test _to_float_or_none helper."""

    def test_valid_float(self):
        assert _to_float_or_none("3.14") == pytest.approx(3.14)

    def test_integer_string(self):
        assert _to_float_or_none("42") == pytest.approx(42.0)

    def test_empty_string(self):
        assert _to_float_or_none("") is None

    def test_whitespace(self):
        assert _to_float_or_none("  ") is None

    def test_invalid(self):
        assert _to_float_or_none("abc") is None

    def test_none_input(self):
        assert _to_float_or_none(None) is None
