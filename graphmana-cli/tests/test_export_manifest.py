"""Tests for export manifest sidecar generation."""

import json
from pathlib import Path
from unittest.mock import MagicMock

from click.testing import CliRunner

from graphmana.cli import cli
from graphmana.export.base import BaseExporter
from graphmana.filtering.export_filters import ExportFilterConfig


class _StubExporter(BaseExporter):
    def export(self, output, **kwargs):
        return {"n_variants": 10, "n_samples": 5, "format": "stub", "chromosomes": ["chr1"]}


class TestManifestGeneration:
    """Test BaseExporter.write_manifest()."""

    def test_manifest_written(self, tmp_path):
        """write_manifest creates a .manifest.json sidecar."""
        conn = MagicMock()
        exporter = _StubExporter(conn)
        output = tmp_path / "test.vcf"
        output.touch()

        summary = {"n_variants": 100, "n_samples": 20, "format": "vcf", "chromosomes": ["chr1"]}
        manifest_path = exporter.write_manifest(output, summary)

        assert manifest_path.exists()
        assert manifest_path.name == "test.vcf.manifest.json"

    def test_manifest_content(self, tmp_path):
        """Manifest contains required fields."""
        conn = MagicMock()
        exporter = _StubExporter(conn)
        output = tmp_path / "test.tsv"
        output.touch()

        summary = {"n_variants": 50, "n_samples": 10, "format": "tsv", "chromosomes": ["chr1", "chr2"]}
        manifest_path = exporter.write_manifest(output, summary)

        data = json.loads(manifest_path.read_text())
        assert data["format"] == "tsv"
        assert data["n_variants"] == 50
        assert data["n_samples"] == 10
        assert data["chromosomes"] == ["chr1", "chr2"]
        assert "graphmana_version" in data
        assert "timestamp" in data

    def test_manifest_records_filters(self, tmp_path):
        """Manifest captures active filter settings."""
        conn = MagicMock()
        fc = ExportFilterConfig(
            populations=["AFR", "EUR"],
            maf_min=0.05,
            chromosomes=["chr1"],
        )
        exporter = _StubExporter(conn, filter_config=fc)
        output = tmp_path / "test.bed"
        output.touch()

        summary = {"n_variants": 30, "n_samples": 15, "format": "bed", "chromosomes": ["chr1"]}
        manifest_path = exporter.write_manifest(output, summary)

        data = json.loads(manifest_path.read_text())
        assert data["filters"]["populations"] == ["AFR", "EUR"]
        assert data["filters"]["maf_min"] == 0.05
        assert data["filters"]["chromosomes"] == ["chr1"]

    def test_manifest_empty_filters(self, tmp_path):
        """Manifest with no active filters has empty filters dict."""
        conn = MagicMock()
        exporter = _StubExporter(conn)
        output = tmp_path / "test.vcf"
        output.touch()

        summary = {"n_variants": 100, "n_samples": 20, "format": "vcf", "chromosomes": []}
        manifest_path = exporter.write_manifest(output, summary)

        data = json.loads(manifest_path.read_text())
        assert data["filters"] == {}


    def test_manifest_records_annotation_filters(self, tmp_path):
        """Manifest captures consequence, impact, gene, and annotation version filters."""
        conn = MagicMock()
        fc = ExportFilterConfig(
            consequences=["missense_variant", "stop_gained"],
            impacts=["HIGH"],
            genes=["BRCA1", "TP53"],
            annotation_version="VEP_110",
        )
        exporter = _StubExporter(conn, filter_config=fc)
        output = tmp_path / "test.vcf"
        output.touch()

        summary = {"n_variants": 10, "n_samples": 5, "format": "vcf", "chromosomes": []}
        manifest_path = exporter.write_manifest(output, summary)
        data = json.loads(manifest_path.read_text())

        assert data["filters"]["consequences"] == ["missense_variant", "stop_gained"]
        assert data["filters"]["impacts"] == ["HIGH"]
        assert data["filters"]["genes"] == ["BRCA1", "TP53"]
        assert data["filters"]["annotation_version"] == "VEP_110"

    def test_manifest_records_cadd_filters(self, tmp_path):
        """Manifest captures CADD score range filters."""
        conn = MagicMock()
        fc = ExportFilterConfig(cadd_min=15.0, cadd_max=30.0)
        exporter = _StubExporter(conn, filter_config=fc)
        output = tmp_path / "test.vcf"
        output.touch()

        summary = {"n_variants": 10, "n_samples": 5, "format": "vcf", "chromosomes": []}
        manifest_path = exporter.write_manifest(output, summary)
        data = json.loads(manifest_path.read_text())

        assert data["filters"]["cadd_min"] == 15.0
        assert data["filters"]["cadd_max"] == 30.0

    def test_manifest_records_sv_and_liftover_filters(self, tmp_path):
        """Manifest captures SV type and liftover status filters."""
        conn = MagicMock()
        fc = ExportFilterConfig(
            sv_types={"DEL", "DUP"},
            liftover_status="mapped",
        )
        exporter = _StubExporter(conn, filter_config=fc)
        output = tmp_path / "test.vcf"
        output.touch()

        summary = {"n_variants": 10, "n_samples": 5, "format": "vcf", "chromosomes": []}
        manifest_path = exporter.write_manifest(output, summary)
        data = json.loads(manifest_path.read_text())

        assert sorted(data["filters"]["sv_types"]) == ["DEL", "DUP"]
        assert data["filters"]["liftover_status"] == "mapped"

    def test_manifest_records_cohort_and_sample_list(self, tmp_path):
        """Manifest captures cohort name and sample ID count."""
        conn = MagicMock()
        fc = ExportFilterConfig(
            cohort="european_subset",
            sample_ids=["S1", "S2", "S3"],
        )
        exporter = _StubExporter(conn, filter_config=fc)
        output = tmp_path / "test.vcf"
        output.touch()

        summary = {"n_variants": 10, "n_samples": 3, "format": "vcf", "chromosomes": []}
        manifest_path = exporter.write_manifest(output, summary)
        data = json.loads(manifest_path.read_text())

        assert data["filters"]["cohort"] == "european_subset"
        assert data["filters"]["n_sample_ids"] == 3

    def test_manifest_records_call_rate_and_region(self, tmp_path):
        """Manifest captures call rate and region filters."""
        conn = MagicMock()
        fc = ExportFilterConfig(
            min_call_rate=0.95,
            region="chr1:1000-2000",
            variant_types={"SNP"},
            maf_max=0.5,
        )
        exporter = _StubExporter(conn, filter_config=fc)
        output = tmp_path / "test.vcf"
        output.touch()

        summary = {"n_variants": 10, "n_samples": 5, "format": "vcf", "chromosomes": ["chr1"]}
        manifest_path = exporter.write_manifest(output, summary)
        data = json.loads(manifest_path.read_text())

        assert data["filters"]["min_call_rate"] == 0.95
        assert data["filters"]["region"] == "chr1:1000-2000"
        assert data["filters"]["variant_types"] == ["SNP"]
        assert data["filters"]["maf_max"] == 0.5


class TestExportNoManifestOption:
    """Test --no-manifest CLI option."""

    def test_no_manifest_option_exists(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["export", "--help"])
        assert "--no-manifest" in result.output
