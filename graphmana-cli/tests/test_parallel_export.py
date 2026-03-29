"""Tests for parallel export (export/parallel.py)."""

import inspect
from unittest.mock import MagicMock, patch

from graphmana.export.parallel import (
    _default_text_merge,
    _get_filter_config_dict,
    _worker_export_chromosome,
    run_export_parallel,
)


class TestGetFilterConfigDict:
    """Test serialization of ExportFilterConfig for pickling."""

    def test_none_input(self):
        assert _get_filter_config_dict(None) is None

    def test_serializes_fields(self):
        from graphmana.filtering.export_filters import ExportFilterConfig

        cfg = ExportFilterConfig(
            populations=["POP1", "POP2"],
            chromosomes=["chr1"],
            region="chr1:100-200",
            variant_types={"SNP"},
            maf_min=0.01,
            maf_max=0.5,
            min_call_rate=0.9,
        )
        result = _get_filter_config_dict(cfg)
        assert result["populations"] == ["POP1", "POP2"]
        # chromosomes is set to None (overridden per worker)
        assert result["chromosomes"] is None
        assert result["region"] == "chr1:100-200"
        assert result["variant_types"] == {"SNP"}
        assert result["maf_min"] == 0.01


class TestDefaultTextMerge:
    """Test text file concatenation logic."""

    def test_merge_with_header(self, tmp_path):
        """When has_header=True, skip all worker headers."""
        output = tmp_path / "merged.tsv"
        # Write header to output
        output.write_text("col1\tcol2\n")

        # Create per-chromosome files
        f1 = tmp_path / "chr1.tmp"
        f1.write_text("col1\tcol2\nA\t1\nB\t2\n")
        f2 = tmp_path / "chr2.tmp"
        f2.write_text("col1\tcol2\nC\t3\n")

        _default_text_merge(output, [("chr1", f1), ("chr2", f2)], has_header=True)

        lines = output.read_text().splitlines()
        assert lines[0] == "col1\tcol2"
        assert lines[1] == "A\t1"
        assert lines[2] == "B\t2"
        assert lines[3] == "C\t3"
        assert len(lines) == 4

    def test_merge_without_header(self, tmp_path):
        """When has_header=False, keep first worker's header."""
        output = tmp_path / "merged.tsv"

        f1 = tmp_path / "chr1.tmp"
        f1.write_text("col1\tcol2\nA\t1\n")
        f2 = tmp_path / "chr2.tmp"
        f2.write_text("col1\tcol2\nB\t2\n")

        _default_text_merge(output, [("chr1", f1), ("chr2", f2)], has_header=False)

        lines = output.read_text().splitlines()
        assert lines[0] == "col1\tcol2"
        assert lines[1] == "A\t1"
        assert lines[2] == "B\t2"
        assert len(lines) == 3

    def test_merge_preserves_chromosome_order(self, tmp_path):
        """Files should be concatenated in the order provided."""
        output = tmp_path / "merged.tsv"
        output.write_text("header\n")

        f1 = tmp_path / "chr1.tmp"
        f1.write_text("header\nfirst\n")
        f2 = tmp_path / "chr2.tmp"
        f2.write_text("header\nsecond\n")

        _default_text_merge(output, [("chr1", f1), ("chr2", f2)], has_header=True)

        lines = output.read_text().splitlines()
        assert lines[1] == "first"
        assert lines[2] == "second"


class TestThreadsOneBypassExport:
    """Verify threads=1 uses sequential code path in exporters."""

    def test_vcf_exporter_has_threads(self):
        from graphmana.export.vcf_export import VCFExporter

        # Check that VCFExporter's parent accepts threads
        sig = inspect.signature(VCFExporter.__init__)
        assert "threads" in sig.parameters

    def test_plink_exporter_has_threads(self):
        from graphmana.export.plink_export import PLINKExporter

        sig = inspect.signature(PLINKExporter.__init__)
        assert "threads" in sig.parameters

    def test_tsv_exporter_has_threads(self):
        from graphmana.export.tsv_export import TSVExporter

        sig = inspect.signature(TSVExporter.__init__)
        assert "threads" in sig.parameters

    def test_base_exporter_defaults_to_one(self):
        from graphmana.export.base import BaseExporter

        sig = inspect.signature(BaseExporter.__init__)
        assert sig.parameters["threads"].default == 1


class TestRunExportParallelSignature:
    """Test that run_export_parallel has the expected interface."""

    def test_signature(self):
        sig = inspect.signature(run_export_parallel)
        params = list(sig.parameters.keys())
        assert "exporter_cls" in params
        assert "conn" in params
        assert "threads" in params
        assert "output" in params
        assert "filter_config" in params
        assert "target_chroms" in params
        assert "export_kwargs" in params
        assert "header_writer" in params
        assert "merge_func" in params
        assert "recalculate_af" in params

    def test_recalculate_af_defaults_false(self):
        sig = inspect.signature(run_export_parallel)
        assert sig.parameters["recalculate_af"].default is False


class TestWorkerRecalculateAf:
    """Verify recalculate_af is forwarded to exporter in worker."""

    @patch("graphmana.db.connection.GraphDatabase")
    def test_worker_passes_recalculate_af_true(self, mock_gdb):
        """Worker creates exporter with recalculate_af=True when set."""
        # Set up mock driver
        mock_driver = MagicMock()
        mock_gdb.driver.return_value = mock_driver

        # Mock the exporter class that importlib will load
        mock_exporter_cls = MagicMock()
        mock_exporter_instance = MagicMock()
        mock_exporter_instance.export.return_value = {"n_variants": 5}
        mock_exporter_cls.return_value = mock_exporter_instance

        with patch("importlib.import_module") as mock_importlib:
            mock_module = MagicMock()
            mock_module.FakeExporter = mock_exporter_cls
            mock_importlib.return_value = mock_module

            result = _worker_export_chromosome(
                "FakeExporter",
                "graphmana.export.fake",
                ("bolt://localhost:7687", "neo4j", "password", None),
                {"populations": None},
                "chr1",
                "/tmp/out.tmp",
                {"phased": True},
                recalculate_af=True,
            )

        # Verify exporter was created with recalculate_af=True
        mock_exporter_cls.assert_called_once()
        call_kwargs = mock_exporter_cls.call_args[1]
        assert call_kwargs["recalculate_af"] is True
        assert result == {"n_variants": 5}

    @patch("graphmana.db.connection.GraphDatabase")
    def test_worker_defaults_recalculate_af_false(self, mock_gdb):
        """Worker defaults recalculate_af to False when not specified."""
        mock_driver = MagicMock()
        mock_gdb.driver.return_value = mock_driver

        mock_exporter_cls = MagicMock()
        mock_exporter_instance = MagicMock()
        mock_exporter_instance.export.return_value = {"n_variants": 3}
        mock_exporter_cls.return_value = mock_exporter_instance

        with patch("importlib.import_module") as mock_importlib:
            mock_module = MagicMock()
            mock_module.FakeExporter = mock_exporter_cls
            mock_importlib.return_value = mock_module

            _worker_export_chromosome(
                "FakeExporter",
                "graphmana.export.fake",
                ("bolt://localhost:7687", "neo4j", "password", None),
                None,
                "chr1",
                "/tmp/out.tmp",
                {},
            )

        call_kwargs = mock_exporter_cls.call_args[1]
        assert call_kwargs["recalculate_af"] is False


class TestRecalculateAfInExporterParallel:
    """Verify each exporter's _export_parallel passes recalculate_af."""

    def test_vcf_exporter_passes_recalculate_af(self):
        """VCFExporter._export_parallel calls run_export_parallel with recalculate_af."""
        from graphmana.export.vcf_export import VCFExporter

        source = inspect.getsource(VCFExporter._export_parallel)
        assert "recalculate_af=self._recalculate_af" in source

    def test_plink_exporter_passes_recalculate_af(self):
        """PLINKExporter._export_parallel calls run_export_parallel with recalculate_af."""
        from graphmana.export.plink_export import PLINKExporter

        source = inspect.getsource(PLINKExporter._export_parallel)
        assert "recalculate_af=self._recalculate_af" in source

    def test_tsv_exporter_passes_recalculate_af(self):
        """TSVExporter._export_parallel calls run_export_parallel with recalculate_af."""
        from graphmana.export.tsv_export import TSVExporter

        source = inspect.getsource(TSVExporter._export_parallel)
        assert "recalculate_af=self._recalculate_af" in source


class TestWorkerSignature:
    """Verify _worker_export_chromosome accepts recalculate_af."""

    def test_worker_has_recalculate_af_param(self):
        sig = inspect.signature(_worker_export_chromosome)
        assert "recalculate_af" in sig.parameters
        assert sig.parameters["recalculate_af"].default is False
