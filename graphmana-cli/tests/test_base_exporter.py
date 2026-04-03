"""Tests for BaseExporter shared functionality: filter chain, sample loading, FAST PATH helpers."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from graphmana.export.base import BaseExporter, ExportSummary
from graphmana.filtering.export_filters import ExportFilterConfig


# ---------------------------------------------------------------------------
# Concrete stub for testing (BaseExporter is abstract)
# ---------------------------------------------------------------------------


class _StubExporter(BaseExporter):
    """Minimal concrete exporter for testing BaseExporter methods."""

    def export(self, output, **kwargs) -> ExportSummary:
        return {"n_variants": 0, "n_samples": 0, "format": "stub", "chromosomes": []}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

ALL_SAMPLES = [
    {"sampleId": "S1", "population": "AFR", "packed_index": 0, "sex": 0},
    {"sampleId": "S2", "population": "AFR", "packed_index": 1, "sex": 0},
    {"sampleId": "S3", "population": "EUR", "packed_index": 2, "sex": 1},
    {"sampleId": "S4", "population": "EUR", "packed_index": 3, "sex": 1},
    {"sampleId": "S5", "population": "EAS", "packed_index": 4, "sex": 0},
]


def _make_exporter(filter_config=None, samples=None):
    """Create a _StubExporter with mocked Neo4j session."""
    conn = MagicMock()
    mock_session = MagicMock()
    mock_session.__enter__ = MagicMock(return_value=mock_session)
    mock_session.__exit__ = MagicMock(return_value=False)

    data = samples if samples is not None else ALL_SAMPLES
    mock_result = MagicMock()
    mock_result.__iter__ = MagicMock(return_value=iter(data))
    mock_session.run.return_value = mock_result
    conn.driver.session.return_value = mock_session

    fc = filter_config or ExportFilterConfig()
    return _StubExporter(conn, filter_config=fc)


# ---------------------------------------------------------------------------
# _get_sample_count
# ---------------------------------------------------------------------------


class TestGetSampleCount:
    """Test _get_sample_count lightweight counting."""

    def test_count_without_population_filter(self):
        """Returns count from COUNT query when no population filter."""
        conn = MagicMock()
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)
        mock_result = MagicMock()
        mock_record = MagicMock()
        mock_record.__getitem__ = MagicMock(return_value=42)
        mock_result.single.return_value = mock_record
        mock_session.run.return_value = mock_result
        conn.driver.session.return_value = mock_session

        exporter = _StubExporter(conn)
        assert exporter._get_sample_count() == 42

    def test_count_uses_cached_samples(self):
        """If samples already loaded, uses len() instead of querying."""
        conn = MagicMock()
        exporter = _StubExporter(conn)
        exporter._samples = [{"sampleId": "S1"}, {"sampleId": "S2"}]

        assert exporter._get_sample_count() == 2
        # No session calls should have been made
        conn.driver.session.assert_not_called()


# ---------------------------------------------------------------------------
# _load_samples with filter chains
# ---------------------------------------------------------------------------


class TestLoadSamplesFilterChain:
    """Test _load_samples with population, cohort, and sample_id filters."""

    def test_no_filters_returns_all_samples(self):
        """No filters → all samples returned."""
        exporter = _make_exporter()
        samples = exporter._load_samples()
        assert len(samples) == 5

    def test_population_filter_applied(self):
        """Population filter selects only matching populations."""
        fc = ExportFilterConfig(populations=["AFR"])
        exporter = _make_exporter(filter_config=fc)
        # The population filter is pushed to the Cypher query, so the mock
        # returns pre-filtered results. Simulate that:
        afr_samples = [s for s in ALL_SAMPLES if s["population"] == "AFR"]
        exporter = _make_exporter(filter_config=fc, samples=afr_samples)
        samples = exporter._load_samples()
        assert len(samples) == 2
        assert all(s["population"] == "AFR" for s in samples)

    def test_sample_id_filter_narrows_results(self):
        """Sample ID filter keeps only listed samples."""
        fc = ExportFilterConfig(sample_ids=["S1", "S5"])
        exporter = _make_exporter(filter_config=fc)
        samples = exporter._load_samples()
        assert len(samples) == 2
        ids = {s["sampleId"] for s in samples}
        assert ids == {"S1", "S5"}

    def test_sample_id_filter_with_no_match(self):
        """Sample ID filter with no matches returns empty list."""
        fc = ExportFilterConfig(sample_ids=["S99"])
        exporter = _make_exporter(filter_config=fc)
        samples = exporter._load_samples()
        assert samples == []

    @patch("graphmana.cohort.manager.CohortManager")
    def test_cohort_filter_narrows_results(self, mock_cohort_cls):
        """Cohort filter keeps only cohort members."""
        mock_mgr = MagicMock()
        mock_mgr.resolve_sample_ids.return_value = ["S1", "S3"]
        mock_cohort_cls.return_value = mock_mgr

        fc = ExportFilterConfig(cohort="my_cohort")
        exporter = _make_exporter(filter_config=fc)
        samples = exporter._load_samples()

        assert len(samples) == 2
        ids = {s["sampleId"] for s in samples}
        assert ids == {"S1", "S3"}

    @patch("graphmana.cohort.manager.CohortManager")
    def test_cohort_plus_sample_id_intersects(self, mock_cohort_cls):
        """Cohort + sample_id filters intersect (both must pass)."""
        mock_mgr = MagicMock()
        mock_mgr.resolve_sample_ids.return_value = ["S1", "S2", "S3"]
        mock_cohort_cls.return_value = mock_mgr

        fc = ExportFilterConfig(cohort="my_cohort", sample_ids=["S2", "S4"])
        exporter = _make_exporter(filter_config=fc)
        samples = exporter._load_samples()

        # Cohort keeps S1, S2, S3; then sample_ids keeps S2 (S4 not in cohort)
        assert len(samples) == 1
        assert samples[0]["sampleId"] == "S2"

    def test_samples_are_cached(self):
        """Second call to _load_samples returns cached result."""
        exporter = _make_exporter()
        first = exporter._load_samples()
        second = exporter._load_samples()
        assert first is second


# ---------------------------------------------------------------------------
# _load_chromosomes
# ---------------------------------------------------------------------------


class TestLoadChromosomes:
    """Test chromosome loading and caching."""

    def test_chromosomes_loaded(self):
        conn = MagicMock()
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)
        chrom_data = [{"chr": "chr1", "length": 248956422}, {"chr": "chr22", "length": 50818468}]
        mock_result = MagicMock()
        mock_result.__iter__ = MagicMock(return_value=iter(chrom_data))
        mock_session.run.return_value = mock_result
        conn.driver.session.return_value = mock_session

        exporter = _StubExporter(conn)
        chroms = exporter._load_chromosomes()
        assert len(chroms) == 2
        assert chroms[0]["chr"] == "chr1"

    def test_chromosomes_cached(self):
        conn = MagicMock()
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)
        mock_result = MagicMock()
        mock_result.__iter__ = MagicMock(return_value=iter([{"chr": "chr1", "length": 100}]))
        mock_session.run.return_value = mock_result
        conn.driver.session.return_value = mock_session

        exporter = _StubExporter(conn)
        first = exporter._load_chromosomes()
        second = exporter._load_chromosomes()
        assert first is second


# ---------------------------------------------------------------------------
# _get_target_chromosomes with filters
# ---------------------------------------------------------------------------


class TestTargetChromosomes:
    """Test chromosome filtering via ExportFilterConfig."""

    def _make_exporter_with_chroms(self, filter_config=None):
        """Exporter with mock chromosomes already loaded."""
        conn = MagicMock()
        exporter = _StubExporter(conn, filter_config=filter_config or ExportFilterConfig())
        exporter._chromosomes = [
            {"chr": "chr1", "length": 248956422},
            {"chr": "chr2", "length": 242193529},
            {"chr": "chr22", "length": 50818468},
        ]
        return exporter

    def test_no_filter_returns_all(self):
        exporter = self._make_exporter_with_chroms()
        assert exporter._get_target_chromosomes() == ["chr1", "chr2", "chr22"]

    def test_chromosome_filter(self):
        fc = ExportFilterConfig(chromosomes=["chr1", "chr22"])
        exporter = self._make_exporter_with_chroms(fc)
        assert exporter._get_target_chromosomes() == ["chr1", "chr22"]

    def test_region_filter_selects_single_chromosome(self):
        fc = ExportFilterConfig(region="chr2:1000-2000")
        exporter = self._make_exporter_with_chroms(fc)
        assert exporter._get_target_chromosomes() == ["chr2"]

    def test_region_filter_unknown_chromosome(self):
        fc = ExportFilterConfig(region="chrX:1000-2000")
        exporter = self._make_exporter_with_chroms(fc)
        assert exporter._get_target_chromosomes() == []


# ---------------------------------------------------------------------------
# _unpack_variant_genotypes
# ---------------------------------------------------------------------------


class TestUnpackVariantGenotypes:
    """Test genotype unpacking from variant property dicts."""

    def test_unpack_basic(self):
        """Unpack gt/phase/ploidy for a subset of samples."""
        from graphmana.ingest.genotype_packer import vectorized_gt_pack

        # 4 samples: HomRef, Het, HomAlt, Missing (cyvcf2 codes)
        cyvcf2_codes = np.array([0, 1, 3, 2], dtype=np.int8)
        gt_packed = bytes(vectorized_gt_pack(cyvcf2_codes))
        phase_packed = bytes([0b0010])  # sample 1 phased
        ploidy_packed = bytes([0b0000])

        conn = MagicMock()
        exporter = _StubExporter(conn)

        props = {
            "gt_packed": gt_packed,
            "phase_packed": phase_packed,
            "ploidy_packed": ploidy_packed,
        }
        indices = np.array([0, 2], dtype=np.int64)  # S1 and S3
        gt, phase, ploidy = exporter._unpack_variant_genotypes(props, indices)

        assert len(gt) == 2
        assert gt[0] == 0  # HomRef
        assert gt[1] == 2  # HomAlt (packed code 2)

    def test_unpack_missing_gt_packed(self):
        """Missing gt_packed returns all-missing genotypes."""
        conn = MagicMock()
        exporter = _StubExporter(conn)

        props = {"gt_packed": None, "phase_packed": None, "ploidy_packed": None}
        indices = np.array([0, 1, 2], dtype=np.int64)
        gt, phase, ploidy = exporter._unpack_variant_genotypes(props, indices)

        assert len(gt) == 3
        assert all(g == 3 for g in gt)  # all missing


# ---------------------------------------------------------------------------
# recalculate_af flag
# ---------------------------------------------------------------------------


class TestRecalculateAf:
    """Test _maybe_recalculate_af behavior."""

    def test_disabled_returns_props_unchanged(self):
        """When recalculate_af=False, props are returned unchanged."""
        conn = MagicMock()
        exporter = _StubExporter(conn, recalculate_af=False)

        props = {"af_total": 0.5, "ac_total": 10}
        result = exporter._maybe_recalculate_af(
            props, np.array([0, 1]), np.zeros(2, dtype=np.uint8)
        )
        assert result is props

    def test_enabled_calls_recalculation(self):
        """When recalculate_af=True, AF is recalculated from genotypes."""
        conn = MagicMock()
        exporter = _StubExporter(conn, recalculate_af=True)

        props = {"af_total": 0.5, "ac_total": 10}
        gt_codes = np.array([0, 1, 1, 2], dtype=np.int8)  # 0+1+1+2=4 ALT alleles out of 8
        ploidy = np.zeros(4, dtype=np.uint8)

        result = exporter._maybe_recalculate_af(props, gt_codes, ploidy)

        # Should have updated values
        assert "ac_total" in result
        assert "an_total" in result
        assert "af_total" in result
        assert result["af_total"] == pytest.approx(4 / 8)
