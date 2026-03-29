"""Tests for QC report output formatters."""

import json

from graphmana.qc.formatters import write_qc_report


class TestWriteJSON:
    """Test JSON output format."""

    def test_writes_json(self, tmp_path):
        data = {
            "variant": {
                "summary": {"n_variants": 100, "mean_call_rate": 0.98},
                "type_counts": [{"variant_type": "SNP", "count": 80}],
                "chr_counts": [{"chr": "chr22", "count": 100}],
            }
        }
        out = tmp_path / "qc.json"
        write_qc_report(data, out, fmt="json")
        assert out.exists()
        parsed = json.loads(out.read_text())
        assert parsed["variant"]["summary"]["n_variants"] == 100

    def test_empty_data(self, tmp_path):
        out = tmp_path / "qc.json"
        write_qc_report({}, out, fmt="json")
        assert out.exists()
        parsed = json.loads(out.read_text())
        assert parsed == {}


class TestWriteTSV:
    """Test TSV output format."""

    def test_writes_tsv(self, tmp_path):
        data = {
            "variant": {
                "summary": {"n_variants": 50, "mean_call_rate": 0.95},
                "type_counts": [{"variant_type": "SNP", "count": 50}],
                "chr_counts": [{"chr": "chr1", "count": 50}],
            }
        }
        out = tmp_path / "qc.tsv"
        write_qc_report(data, out, fmt="tsv")
        assert out.exists()
        content = out.read_text()
        assert "Variant QC Summary" in content
        assert "n_variants" in content
        assert "SNP" in content

    def test_sample_section(self, tmp_path):
        data = {
            "sample": {
                "n_samples": 2,
                "n_variants_scanned": 100,
                "stats": [
                    {
                        "sampleId": "S1",
                        "n_het": 10,
                        "n_hom_alt": 5,
                        "heterozygosity": 0.1,
                        "call_rate": 0.95,
                    },
                ],
            }
        }
        out = tmp_path / "qc.tsv"
        write_qc_report(data, out, fmt="tsv")
        content = out.read_text()
        assert "Sample QC" in content
        assert "S1" in content
        assert "heterozygosity" in content

    def test_batch_section(self, tmp_path):
        data = {
            "batch": {
                "population_summary": [
                    {"population": "POP1", "n_samples_total": 10, "n_samples_active": 8}
                ]
            }
        }
        out = tmp_path / "qc.tsv"
        write_qc_report(data, out, fmt="tsv")
        content = out.read_text()
        assert "Population Summary" in content
        assert "POP1" in content

    def test_all_sections(self, tmp_path):
        data = {
            "variant": {
                "summary": {"n_variants": 10},
                "type_counts": [],
                "chr_counts": [],
            },
            "sample": {"n_samples": 2, "n_variants_scanned": 10, "stats": []},
            "batch": {"population_summary": []},
        }
        out = tmp_path / "qc.tsv"
        write_qc_report(data, out, fmt="tsv")
        content = out.read_text()
        assert "Variant QC Summary" in content
        assert "Sample QC" in content
        assert "Population Summary" in content


class TestWriteHTML:
    """Test HTML output format."""

    def test_writes_html(self, tmp_path):
        data = {
            "variant": {
                "summary": {"n_variants": 100},
                "type_counts": [{"variant_type": "SNP", "count": 100}],
                "chr_counts": [],
            }
        }
        out = tmp_path / "qc.html"
        write_qc_report(data, out, fmt="html")
        assert out.exists()
        content = out.read_text()
        assert "<html>" in content
        assert "GraphMana QC Report" in content
        assert "n_variants" in content

    def test_empty_data(self, tmp_path):
        out = tmp_path / "qc.html"
        write_qc_report({}, out, fmt="html")
        assert out.exists()
        assert "<html>" in out.read_text()


class TestOutputDirCreation:
    """Test that output directory is created if needed."""

    def test_creates_parent_dir(self, tmp_path):
        out = tmp_path / "subdir" / "qc.json"
        write_qc_report(
            {"variant": {"summary": {}, "type_counts": [], "chr_counts": []}}, out, fmt="json"
        )
        assert out.exists()
