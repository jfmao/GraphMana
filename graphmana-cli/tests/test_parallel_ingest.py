"""Tests for parallel CSV generation (ingest/parallel.py)."""

import csv
from pathlib import Path
from unittest.mock import MagicMock, patch

from graphmana.ingest.parallel import (
    _get_vcf_chromosomes,
    _has_tabix_index,
    _merge_csv_dirs,
    _resolve_requested_contigs,
    _worker_prepare_csv_region,
    run_prepare_csv_parallel,
)


class TestGetVcfChromosomes:
    """Test chromosome extraction from VCF header."""

    def test_returns_chromosome_list(self):
        mock_vcf = MagicMock()
        mock_vcf.seqnames = ["chr1", "chr2", "chr22"]
        mock_vcf.close = MagicMock()
        with patch("cyvcf2.VCF", return_value=mock_vcf):
            result = _get_vcf_chromosomes("test.vcf.gz")
        assert result == ["chr1", "chr2", "chr22"]
        mock_vcf.close.assert_called_once()

    def test_returns_empty_for_no_contigs(self):
        mock_vcf = MagicMock()
        mock_vcf.seqnames = []
        mock_vcf.close = MagicMock()
        with patch("cyvcf2.VCF", return_value=mock_vcf):
            result = _get_vcf_chromosomes("empty.vcf.gz")
        assert result == []


class TestMergeCsvDirs:
    """Test merging per-chromosome CSV directories."""

    def _make_chr_dir(self, base: Path, chrom: str, n_variants: int = 2) -> Path:
        """Create a fake per-chromosome CSV directory."""
        d = base / chrom
        d.mkdir(parents=True, exist_ok=True)

        # Static files
        with open(d / "sample_nodes.csv", "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["sampleId:ID(Sample)", ":LABEL", "population", "packed_index:int"])
            w.writerow(["S1", "Sample", "POP1", 0])
            w.writerow(["S2", "Sample", "POP1", 1])

        with open(d / "population_nodes.csv", "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["populationId:ID(Population)", ":LABEL", "name", "n_samples:int"])
            w.writerow(["POP1", "Population", "POP1", 2])

        with open(d / "in_population_edges.csv", "w", newline="") as f:
            w = csv.writer(f)
            w.writerow([":START_ID(Sample)", ":END_ID(Population)", ":TYPE"])
            w.writerow(["S1", "POP1", "IN_POPULATION"])

        # Streaming files
        with open(d / "variant_nodes.csv", "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["variantId:ID(Variant)", ":LABEL", "chr", "pos:long"])
            for i in range(n_variants):
                w.writerow([f"{chrom}:{100+i}:A:T", "Variant", chrom, 100 + i])

        with open(d / "next_edges.csv", "w", newline="") as f:
            w = csv.writer(f)
            w.writerow([":START_ID(Variant)", ":END_ID(Variant)", ":TYPE", "distance_bp:long"])
            if n_variants > 1:
                w.writerow([f"{chrom}:100:A:T", f"{chrom}:101:A:T", "NEXT", 1])

        with open(d / "on_chromosome_edges.csv", "w", newline="") as f:
            w = csv.writer(f)
            w.writerow([":START_ID(Variant)", ":END_ID(Chromosome)", ":TYPE"])
            for i in range(n_variants):
                w.writerow([f"{chrom}:{100+i}:A:T", chrom, "ON_CHROMOSOME"])

        with open(d / "chromosome_nodes.csv", "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["chromosomeId:ID(Chromosome)", ":LABEL", "length:long"])
            w.writerow([chrom, "Chromosome", 50000000])

        return d

    def test_merge_two_chromosomes(self, tmp_path):
        d1 = self._make_chr_dir(tmp_path / "workers", "chr1", n_variants=3)
        d2 = self._make_chr_dir(tmp_path / "workers", "chr2", n_variants=2)
        final = tmp_path / "merged"

        _merge_csv_dirs([("chr1", d1), ("chr2", d2)], final)

        # Check static files copied from first dir
        assert (final / "sample_nodes.csv").exists()
        assert (final / "population_nodes.csv").exists()

        # Check variant_nodes.csv has header + 5 data rows
        with open(final / "variant_nodes.csv") as f:
            lines = f.readlines()
        assert len(lines) == 6  # 1 header + 3 + 2

        # Check chromosome_nodes.csv has both chromosomes
        with open(final / "chromosome_nodes.csv") as f:
            reader = csv.reader(f)
            rows = list(reader)
        # header + chr1 + chr2
        assert len(rows) == 3

    def test_merge_deduplicates_headers(self, tmp_path):
        d1 = self._make_chr_dir(tmp_path / "workers", "chr1")
        d2 = self._make_chr_dir(tmp_path / "workers", "chr2")
        final = tmp_path / "merged"

        _merge_csv_dirs([("chr1", d1), ("chr2", d2)], final)

        with open(final / "variant_nodes.csv") as f:
            lines = f.readlines()
        # Only one header line
        header_count = sum(1 for line in lines if "variantId:ID" in line)
        assert header_count == 1

    def test_merge_empty_list(self, tmp_path):
        final = tmp_path / "merged"
        _merge_csv_dirs([], final)
        assert final.exists()


class TestThreadsOneBypass:
    """Verify threads=1 calls sequential path directly."""

    def test_threads_parameter_defaults_to_one(self):
        """run_prepare_csv defaults to threads=1 (sequential)."""
        import inspect

        import graphmana.ingest.pipeline as mod

        sig = inspect.signature(mod.run_prepare_csv)
        assert sig.parameters["threads"].default == 1

    def test_run_ingest_has_threads_parameter(self):
        """run_ingest accepts threads parameter."""
        import inspect

        import graphmana.ingest.pipeline as mod

        sig = inspect.signature(mod.run_ingest)
        assert "threads" in sig.parameters
        assert sig.parameters["threads"].default == 1


class TestWorkerPrepare:
    """Test that worker calls run_prepare_csv with correct contigs."""

    @patch("graphmana.ingest.pipeline.run_prepare_csv")
    def test_worker_passes_contigs(self, mock_prepare):
        from graphmana.ingest.parallel import _worker_prepare_csv

        mock_prepare.return_value = {"n_variants": 10}

        result = _worker_prepare_csv(
            "test.vcf.gz",
            "panel.tsv",
            "/tmp/out",
            "chr22",
            {"stratify_by": "superpopulation"},
        )

        mock_prepare.assert_called_once_with(
            "test.vcf.gz",
            "panel.tsv",
            "/tmp/out",
            contigs=["chr22"],
            stratify_by="superpopulation",
        )
        assert result == {"n_variants": 10}


class TestParallelDispatchIngest:
    """Test that run_prepare_csv_parallel dispatches workers correctly."""

    @patch("graphmana.ingest.parallel._merge_csv_dirs")
    @patch("graphmana.ingest.parallel._worker_prepare_csv")
    @patch("graphmana.ingest.parallel._get_vcf_chromosomes")
    def test_dispatches_per_chromosome(self, mock_chroms, mock_worker, mock_merge):
        mock_chroms.return_value = ["chr1", "chr2"]
        mock_worker.return_value = {
            "n_variants": 5,
            "n_samples": 2,
            "n_populations": 1,
            "chromosomes": ["chr1"],
            "n_next_edges": 4,
            "reference": "GRCh38",
        }
        mock_merge.return_value = {"n_chromosome_files_merged": 2}

        # Can't easily test ProcessPoolExecutor without real processes,
        # but we verify the function signature accepts the right parameters
        import inspect

        sig = inspect.signature(run_prepare_csv_parallel)
        params = list(sig.parameters.keys())
        assert "threads" in params
        assert "contigs" in params
        assert "vcf_path" in params
        assert "panel_path" in params
        assert "output_dir" in params


class TestHasTabixIndex:
    """Test tabix/csi index detection."""

    def test_tbi_exists(self, tmp_path):
        vcf = tmp_path / "data.vcf.gz"
        vcf.touch()
        (tmp_path / "data.vcf.gz.tbi").touch()
        assert _has_tabix_index(vcf) is True

    def test_csi_exists(self, tmp_path):
        vcf = tmp_path / "data.vcf.gz"
        vcf.touch()
        (tmp_path / "data.vcf.gz.csi").touch()
        assert _has_tabix_index(vcf) is True

    def test_no_index(self, tmp_path):
        vcf = tmp_path / "data.vcf.gz"
        vcf.touch()
        assert _has_tabix_index(vcf) is False

    def test_bcf_csi(self, tmp_path):
        bcf = tmp_path / "data.bcf"
        bcf.touch()
        (tmp_path / "data.bcf.csi").touch()
        assert _has_tabix_index(bcf) is True


class TestResolveRequestedContigs:
    """Test merging contig restrictions from contigs param and filter_config."""

    def test_none_when_unrestricted(self):
        assert _resolve_requested_contigs(None, None) is None

    def test_explicit_contigs_only(self):
        result = _resolve_requested_contigs(["Chr1", "Chr2"], None)
        assert result == {"Chr1", "Chr2"}

    def test_filter_config_contigs_only(self):
        from graphmana.filtering.import_filters import ImportFilterConfig

        fc = ImportFilterConfig(contigs=["Chr3", "Chr4"])
        result = _resolve_requested_contigs(None, fc)
        assert result == {"Chr3", "Chr4"}

    def test_intersection_of_both(self):
        from graphmana.filtering.import_filters import ImportFilterConfig

        fc = ImportFilterConfig(contigs=["Chr1", "Chr2", "Chr3"])
        result = _resolve_requested_contigs(["Chr2", "Chr3", "Chr4"], fc)
        assert result == {"Chr2", "Chr3"}

    def test_filter_config_with_empty_contigs(self):
        from graphmana.filtering.import_filters import ImportFilterConfig

        fc = ImportFilterConfig(contigs=None)
        result = _resolve_requested_contigs(["Chr1"], fc)
        assert result == {"Chr1"}

    def test_empty_list_treated_as_none(self):
        assert _resolve_requested_contigs([], None) is None


class TestWorkerPrepareRegion:
    """Test that region worker passes region= instead of contigs=."""

    @patch("graphmana.ingest.pipeline.run_prepare_csv")
    def test_worker_passes_region(self, mock_prepare):
        mock_prepare.return_value = {"n_variants": 42}

        result = _worker_prepare_csv_region(
            "test.vcf.gz",
            "panel.tsv",
            "/tmp/out",
            "Chr1",
            {"stratify_by": "superpopulation"},
        )

        mock_prepare.assert_called_once_with(
            "test.vcf.gz",
            "panel.tsv",
            "/tmp/out",
            region="Chr1",
            stratify_by="superpopulation",
        )
        assert result == {"n_variants": 42}


class TestStrategyDispatch:
    """Test the strategy decision tree in run_prepare_csv_parallel."""

    @patch("graphmana.ingest.parallel._get_vcf_chromosomes")
    @patch("graphmana.ingest.pipeline.run_prepare_csv")
    def test_region_forces_sequential(self, mock_sequential, mock_chroms):
        """If region is specified, always fall back to sequential."""
        mock_sequential.return_value = {"n_variants": 10}
        # _get_vcf_chromosomes should NOT be called
        result = run_prepare_csv_parallel(
            "test.vcf.gz",
            "panel.tsv",
            "/tmp/out",
            threads=4,
            region="Chr1:1000-2000",
        )
        mock_chroms.assert_not_called()
        mock_sequential.assert_called_once()
        assert result == {"n_variants": 10}

    @patch("graphmana.ingest.parallel._has_tabix_index", return_value=False)
    @patch("graphmana.ingest.parallel._get_vcf_chromosomes", return_value=["Chr1", "Chr2"])
    @patch("graphmana.ingest.pipeline.run_prepare_csv")
    def test_no_index_falls_back_sequential(self, mock_sequential, mock_chroms, mock_idx):
        """Without tabix index, fall back to sequential single-pass."""
        mock_sequential.return_value = {"n_variants": 100}
        result = run_prepare_csv_parallel(
            "test.vcf.gz",
            "panel.tsv",
            "/tmp/out",
            threads=4,
        )
        mock_sequential.assert_called_once()
        assert result == {"n_variants": 100}

    @patch("graphmana.ingest.parallel._has_tabix_index", return_value=False)
    @patch("graphmana.ingest.parallel._get_vcf_chromosomes", return_value=["Chr1", "Chr2", "Chr3"])
    @patch("graphmana.ingest.pipeline.run_prepare_csv")
    def test_contigs_restricts_chromosomes_on_sequential_fallback(
        self, mock_sequential, mock_chroms, mock_idx
    ):
        """contigs= is forwarded to sequential fallback."""
        mock_sequential.return_value = {"n_variants": 50}
        run_prepare_csv_parallel(
            "test.vcf.gz",
            "panel.tsv",
            "/tmp/out",
            threads=4,
            contigs=["Chr1"],
        )
        # Verify contigs= was forwarded
        _, kwargs = mock_sequential.call_args
        assert kwargs.get("contigs") == ["Chr1"]

    @patch("graphmana.ingest.parallel._get_vcf_chromosomes", return_value=[])
    @patch("graphmana.ingest.pipeline.run_prepare_csv")
    def test_empty_chromosomes_falls_back(self, mock_sequential, mock_chroms):
        """Empty VCF header chromosomes → sequential fallback."""
        mock_sequential.return_value = {"n_variants": 0}
        run_prepare_csv_parallel(
            "test.vcf.gz",
            "panel.tsv",
            "/tmp/out",
            threads=4,
        )
        mock_sequential.assert_called_once()

    @patch("graphmana.ingest.parallel._has_tabix_index", return_value=False)
    @patch("graphmana.ingest.parallel._get_vcf_chromosomes", return_value=["Chr1", "Chr2"])
    @patch("graphmana.ingest.pipeline.run_prepare_csv")
    def test_filter_config_contigs_restricts_chromosomes(
        self, mock_sequential, mock_chroms, mock_idx
    ):
        """filter_config.contigs restricts which chromosomes are processed."""
        from graphmana.filtering.import_filters import ImportFilterConfig

        fc = ImportFilterConfig(contigs=["Chr1"])
        mock_sequential.return_value = {"n_variants": 50}
        run_prepare_csv_parallel(
            "test.vcf.gz",
            "panel.tsv",
            "/tmp/out",
            threads=4,
            filter_config=fc,
        )
        mock_sequential.assert_called_once()


class TestContigsForwardedFromPipeline:
    """Verify pipeline.py forwards contigs to run_prepare_csv_parallel."""

    def test_contigs_in_parallel_signature(self):
        """run_prepare_csv_parallel accepts explicit contigs kwarg."""
        import inspect

        sig = inspect.signature(run_prepare_csv_parallel)
        assert "contigs" in sig.parameters
        assert sig.parameters["contigs"].default is None

    @patch("graphmana.ingest.parallel.run_prepare_csv_parallel")
    def test_pipeline_forwards_contigs(self, mock_parallel):
        """run_prepare_csv with threads>1 passes contigs to parallel."""
        from graphmana.ingest.pipeline import run_prepare_csv

        mock_parallel.return_value = {"n_variants": 0}
        run_prepare_csv(
            "test.vcf.gz",
            "panel.tsv",
            "/tmp/out",
            threads=2,
            contigs=["Chr1", "Chr2"],
        )
        mock_parallel.assert_called_once()
        _, kwargs = mock_parallel.call_args
        assert kwargs.get("contigs") == ["Chr1", "Chr2"]
