"""Tests for incremental sample addition logic.

Uses mocking to test IncrementalIngester and pipeline integration without
requiring a live Neo4j instance.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from graphmana.ingest.array_ops import extend_gt_packed, merge_pop_stats
from graphmana.ingest.genotype_packer import (
    GT_REMAP,
    unpack_genotypes,
    unpack_phase,
    vectorized_gt_pack,
)
from graphmana.ingest.incremental import IncrementalIngester, _VariantData

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_pop_map(sample_ids, pop_ids, sample_to_pop, n_samples_per_pop=None):
    """Create a mock PopulationMap."""
    pm = MagicMock()
    pm.sample_ids = sample_ids
    pm.pop_ids = pop_ids
    pm.sample_to_pop = sample_to_pop
    pm.sample_to_sex = {s: 0 for s in sample_ids}
    pm.sample_packed_index = {s: i for i, s in enumerate(sample_ids)}
    if n_samples_per_pop is None:
        n_samples_per_pop = {}
        for s, p in sample_to_pop.items():
            n_samples_per_pop[p] = n_samples_per_pop.get(p, 0) + 1
    pm.n_samples_per_pop = n_samples_per_pop
    return pm


def _make_conn():
    """Create a mock GraphManaConnection."""
    conn = MagicMock()
    conn.execute_read = MagicMock(return_value=MagicMock(single=MagicMock(return_value=None)))
    conn.execute_write = MagicMock()
    conn.execute_write_tx = MagicMock()
    return conn


def _make_variant_data(
    variant_id="chr1:100:A:T",
    chrom="chr1",
    pos=100,
    n_new=2,
    gt_types=None,
    pop_ids=None,
):
    """Create a _VariantData for testing."""
    if gt_types is None:
        gt_types = np.array([1, 0], dtype=np.int8)[:n_new]
    if pop_ids is None:
        pop_ids = ["POP1"]
    return _VariantData(
        variant_id=variant_id,
        chr=chrom,
        pos=pos,
        ref="A",
        alt="T",
        variant_type="SNP",
        gt_types=gt_types,
        phase_bits=np.zeros(n_new, dtype=np.uint8),
        ploidy_bits=np.zeros(n_new, dtype=np.uint8),
        pop_ids=pop_ids,
        ac=[1],
        an=[2 * n_new],
        af=[1 / (2 * n_new)] if n_new > 0 else [0.0],
        het_count=[1],
        hom_alt_count=[0],
        het_exp=[0.0],
        ac_total=1,
        an_total=2 * n_new,
        af_total=1 / (2 * n_new) if n_new > 0 else 0.0,
        call_rate=1.0,
    )


# ---------------------------------------------------------------------------
# TestIncrementalIngesterInit
# ---------------------------------------------------------------------------


class TestIncrementalIngesterInit:
    """Test IncrementalIngester initialization and configuration."""

    def test_basic_init(self):
        """IncrementalIngester initializes with correct parameters."""
        conn = _make_conn()
        pm = _make_pop_map(["S3", "S4"], ["POP1"], {"S3": "POP1", "S4": "POP1"})

        ingester = IncrementalIngester(
            conn=conn,
            pop_map_new=pm,
            n_existing=2,
            existing_sample_ids={"S1", "S2"},
            existing_pop_ids=["POP1"],
            packed_index_offset=2,
            dataset_id="ds1",
            source_file="test.vcf.gz",
            n_total_samples=4,
        )

        assert ingester.n_existing == 2
        assert ingester.packed_index_offset == 2
        assert ingester.n_total_samples == 4
        assert ingester.n_variants_extended == 0

    def test_packed_index_offset(self):
        """Packed index offset correctly starts after existing samples."""
        conn = _make_conn()
        pm = _make_pop_map(["S5"], ["POP1"], {"S5": "POP1"})

        ingester = IncrementalIngester(
            conn=conn,
            pop_map_new=pm,
            n_existing=10,
            existing_sample_ids={f"S{i}" for i in range(10)},
            existing_pop_ids=["POP1"],
            packed_index_offset=10,
            dataset_id="ds1",
            source_file="test.vcf.gz",
            n_total_samples=11,
        )

        assert ingester.packed_index_offset == 10


# ---------------------------------------------------------------------------
# TestExtendExistingVariants
# ---------------------------------------------------------------------------


class TestExtendExistingVariants:
    """Test the variant extension logic via _extend_variant_batch."""

    def test_extend_preserves_existing_genotypes(self):
        """Extending variants preserves existing genotype data."""
        # Existing: 4 samples with known genotypes
        existing_cyvcf2 = np.array([0, 1, 3, 2], dtype=np.int8)
        existing_gt_packed = vectorized_gt_pack(existing_cyvcf2)

        # New: 2 samples (Het, HomRef)
        new_gt = np.array([1, 0], dtype=np.int8)

        result = extend_gt_packed(existing_gt_packed, 4, new_gt)
        unpacked = unpack_genotypes(result, 6)

        # Verify existing samples unchanged
        expected_existing = GT_REMAP[existing_cyvcf2].astype(np.int8)
        np.testing.assert_array_equal(unpacked[:4], expected_existing)

        # Verify new samples correct
        expected_new = GT_REMAP[new_gt].astype(np.int8)
        np.testing.assert_array_equal(unpacked[4:], expected_new)

    def test_merge_stats_during_extension(self):
        """Population stats are correctly merged during extension."""
        merged = merge_pop_stats(
            existing_pop_ids=["POP1"],
            existing_ac=[10],
            existing_an=[40],
            existing_het_count=[8],
            existing_hom_alt_count=[1],
            new_pop_ids=["POP1"],
            new_ac=[2],
            new_an=[8],
            new_het_count=[2],
            new_hom_alt_count=[0],
        )

        assert merged["ac"] == [12]
        assert merged["an"] == [48]
        assert pytest.approx(merged["af"][0]) == 12 / 48


# ---------------------------------------------------------------------------
# TestCreateNewVariants
# ---------------------------------------------------------------------------


class TestCreateNewVariants:
    """Test creation of brand-new variant nodes."""

    def test_homref_padding_correct(self):
        """New variant has HomRef padding for existing samples."""
        from graphmana.ingest.array_ops import pad_gt_for_new_variant

        # 4 existing samples, 2 new (Het, HomAlt)
        new_gt = np.array([1, 3], dtype=np.int8)
        result = pad_gt_for_new_variant(4, new_gt)
        unpacked = unpack_genotypes(result, 6)

        # First 4 should be HomRef (0)
        np.testing.assert_array_equal(unpacked[:4], np.zeros(4, dtype=np.int8))
        # Next 2 should be Het (1) and HomAlt (2)
        assert unpacked[4] == 1  # Het
        assert unpacked[5] == 2  # HomAlt

    def test_phase_padding_correct(self):
        """New variant has zero phase padding for existing samples."""
        from graphmana.ingest.array_ops import pad_phase_for_new_variant

        new_phase = np.array([1, 0], dtype=np.uint8)
        result = pad_phase_for_new_variant(4, new_phase)
        unpacked = unpack_phase(result, 6)

        # First 4 should be 0
        np.testing.assert_array_equal(unpacked[:4], np.zeros(4, dtype=np.uint8))
        # Next 2 match new_phase
        np.testing.assert_array_equal(unpacked[4:], new_phase)


# ---------------------------------------------------------------------------
# TestAutoDetectMode
# ---------------------------------------------------------------------------


class TestAutoDetectMode:
    """Test import mode auto-detection."""

    def test_empty_db_detects_initial(self):
        """Empty database returns 'initial' mode."""
        from graphmana.ingest.pipeline import _detect_import_mode

        with patch("graphmana.db.connection.GraphDatabase") as mock_gdb:
            mock_driver = MagicMock()
            mock_session = MagicMock()
            mock_result = MagicMock()
            mock_result.__iter__ = MagicMock(return_value=iter([{"c": 0}]))
            mock_result.consume.return_value = MagicMock()
            mock_session.run.return_value = mock_result
            mock_session.__enter__ = MagicMock(return_value=mock_session)
            mock_session.__exit__ = MagicMock(return_value=False)
            mock_driver.session.return_value = mock_session
            mock_driver.verify_connectivity = MagicMock()
            mock_driver.close = MagicMock()
            mock_gdb.driver.return_value = mock_driver

            mode = _detect_import_mode("bolt://localhost:7687", "neo4j", "pass")
            assert mode == "initial"

    def test_populated_db_detects_incremental(self):
        """Database with variants returns 'incremental' mode."""
        from graphmana.ingest.pipeline import _detect_import_mode

        with patch("graphmana.db.connection.GraphDatabase") as mock_gdb:
            mock_driver = MagicMock()
            mock_session = MagicMock()
            mock_result = MagicMock()
            mock_result.__iter__ = MagicMock(return_value=iter([{"c": 1000}]))
            mock_result.consume.return_value = MagicMock()
            mock_session.run.return_value = mock_result
            mock_session.__enter__ = MagicMock(return_value=mock_session)
            mock_session.__exit__ = MagicMock(return_value=False)
            mock_driver.session.return_value = mock_session
            mock_driver.verify_connectivity = MagicMock()
            mock_driver.close = MagicMock()
            mock_gdb.driver.return_value = mock_driver

            mode = _detect_import_mode("bolt://localhost:7687", "neo4j", "pass")
            assert mode == "incremental"

    def test_connection_failure_defaults_initial(self):
        """Failed connection defaults to 'initial' mode."""
        from graphmana.ingest.pipeline import _detect_import_mode

        with patch("graphmana.db.connection.GraphDatabase") as mock_gdb:
            mock_gdb.driver.side_effect = Exception("Connection refused")

            mode = _detect_import_mode("bolt://localhost:7687", "neo4j", "pass")
            assert mode == "initial"


# ---------------------------------------------------------------------------
# TestDuplicateSamples
# ---------------------------------------------------------------------------


class TestDuplicateSamples:
    """Test duplicate sample handling in incremental mode."""

    def test_duplicate_error_raises(self):
        """on_duplicate='error' raises ValueError for duplicates."""
        from graphmana.ingest.pipeline import run_incremental

        with patch("graphmana.db.connection.GraphDatabase") as mock_gdb:
            mock_driver = MagicMock()
            mock_session = MagicMock()

            # Return existing sample IDs — must be iterable for _EagerResult
            mock_result = MagicMock()
            mock_result.__iter__ = MagicMock(
                return_value=iter([{"ids": ["S1", "S2"]}])
            )
            mock_result.consume.return_value = MagicMock()
            mock_session.run.return_value = mock_result
            mock_session.__enter__ = MagicMock(return_value=mock_session)
            mock_session.__exit__ = MagicMock(return_value=False)
            mock_driver.session.return_value = mock_session
            mock_driver.verify_connectivity = MagicMock()
            mock_driver.close = MagicMock()
            mock_gdb.driver.return_value = mock_driver

            with patch("graphmana.ingest.pipeline.VCFParser") as mock_parser_cls:
                mock_parser = MagicMock()
                mock_pm = _make_pop_map(["S1", "S3"], ["POP1"], {"S1": "POP1", "S3": "POP1"})
                mock_parser.pop_map = mock_pm
                mock_parser_cls.return_value = mock_parser

                with pytest.raises(ValueError, match="Duplicate samples"):
                    run_incremental(
                        "test.vcf.gz",
                        "panel.tsv",
                        neo4j_uri="bolt://localhost:7687",
                        on_duplicate="error",
                    )


# ---------------------------------------------------------------------------
# TestIncrementalIngesterRun
# ---------------------------------------------------------------------------


class TestIncrementalIngesterRun:
    """Test the full run method with mocked dependencies."""

    def test_creates_sample_nodes(self):
        """Run creates Sample nodes with correct packed_index values."""
        conn = _make_conn()
        pm = _make_pop_map(["S3", "S4"], ["POP1"], {"S3": "POP1", "S4": "POP1"})

        ingester = IncrementalIngester(
            conn=conn,
            pop_map_new=pm,
            n_existing=2,
            existing_sample_ids={"S1", "S2"},
            existing_pop_ids=["POP1"],
            packed_index_offset=2,
            dataset_id="ds1",
            source_file="test.vcf.gz",
            n_total_samples=4,
        )

        # Call _create_sample_nodes directly
        ingester._create_sample_nodes()

        assert ingester.n_samples_created == 2
        # Verify execute_write was called with CREATE_SAMPLE_BATCH
        calls = conn.execute_write.call_args_list
        found_sample_create = False
        for call in calls:
            args, _ = call
            if len(args) > 1 and "samples" in args[1]:
                found_sample_create = True
                samples = args[1]["samples"]
                # Check packed_index values
                indices = {s["sampleId"]: s["packed_index"] for s in samples}
                assert indices["S3"] == 2  # offset + 0
                assert indices["S4"] == 3  # offset + 1
                break
        assert found_sample_create

    def test_update_populations_new_pop(self):
        """Run creates new Population nodes for populations not in DB."""
        conn = _make_conn()
        pm = _make_pop_map(
            ["S3", "S4"],
            ["NEWPOP", "POP1"],
            {"S3": "POP1", "S4": "NEWPOP"},
        )

        # Mock reading existing population counts for overlap
        mock_result = MagicMock()
        mock_result.__iter__ = MagicMock(return_value=iter([{"pid": "POP1", "n": 2}]))
        conn.execute_read.return_value = mock_result

        ingester = IncrementalIngester(
            conn=conn,
            pop_map_new=pm,
            n_existing=2,
            existing_sample_ids={"S1", "S2"},
            existing_pop_ids=["POP1"],
            packed_index_offset=2,
            dataset_id="ds1",
            source_file="test.vcf.gz",
            n_total_samples=4,
        )

        ingester._update_populations()

        assert ingester.n_populations_created == 1
        # Verify MERGE_POPULATION was called for NEWPOP
        merge_calls = [c for c in conn.execute_write.call_args_list if "MERGE" in str(c)]
        assert len(merge_calls) >= 1


# ---------------------------------------------------------------------------
# TestVariantDataCreation
# ---------------------------------------------------------------------------


class TestVariantDataCreation:
    """Test _VariantData creation and usage."""

    def test_variant_data_fields(self):
        """_VariantData stores all required fields."""
        vd = _make_variant_data()
        assert vd.variant_id == "chr1:100:A:T"
        assert vd.chr == "chr1"
        assert vd.pos == 100
        assert vd.ref == "A"
        assert vd.alt == "T"
        assert len(vd.gt_types) == 2
        assert len(vd.phase_bits) == 2
        assert len(vd.ploidy_bits) == 2

    def test_variant_data_with_custom_genotypes(self):
        """_VariantData with specific genotype codes."""
        gt = np.array([0, 1, 3, 2], dtype=np.int8)
        vd = _make_variant_data(n_new=4, gt_types=gt)
        np.testing.assert_array_equal(vd.gt_types, gt)


# ---------------------------------------------------------------------------
# TestCollectVariantsByChr
# ---------------------------------------------------------------------------


class TestStreamVariantsByChr:
    """Test chromosome-grouped variant streaming."""

    def _make_ingester(self):
        conn = _make_conn()
        pm = _make_pop_map(["S1"], ["POP1"], {"S1": "POP1"})
        return IncrementalIngester(
            conn=conn,
            pop_map_new=pm,
            n_existing=0,
            existing_sample_ids=set(),
            existing_pop_ids=[],
            packed_index_offset=0,
            dataset_id="ds1",
            source_file="test.vcf.gz",
            n_total_samples=1,
        )

    def _make_record(self, vid, chrom, pos, ref="A", alt="T", gt_code=0):
        from graphmana.ingest.vcf_parser import VariantRecord

        return VariantRecord(
            id=vid,
            chr=chrom,
            pos=pos,
            ref=ref,
            alt=alt,
            variant_type="SNP",
            ac=[0],
            an=[2],
            af=[0.0],
            het_count=[0],
            hom_alt_count=[0],
            het_exp=[0.0],
            ac_total=0,
            an_total=2,
            af_total=0.0,
            call_rate=1.0,
            gt_packed=vectorized_gt_pack(np.array([gt_code], dtype=np.int8)),
            phase_packed=bytes(1),
            ploidy_packed=b"",
        )

    def test_groups_by_chromosome(self):
        """Variants from parser are streamed grouped by chromosome."""
        ingester = self._make_ingester()
        rec1 = self._make_record("chr1:100:A:T", "chr1", 100)
        rec2 = self._make_record("chr2:200:G:C", "chr2", 200, ref="G", alt="C")

        result = dict(ingester._stream_variants_by_chr([rec1, rec2], filter_chain=None))

        assert "chr1" in result
        assert "chr2" in result
        assert "chr1:100:A:T" in result["chr1"]
        assert "chr2:200:G:C" in result["chr2"]

    def test_single_chromosome_yields_once(self):
        """All variants on the same chromosome yield as a single batch."""
        ingester = self._make_ingester()
        recs = [
            self._make_record("chr1:100:A:T", "chr1", 100),
            self._make_record("chr1:200:G:C", "chr1", 200, ref="G", alt="C"),
            self._make_record("chr1:300:C:A", "chr1", 300, ref="C", alt="A"),
        ]

        results = list(ingester._stream_variants_by_chr(recs, filter_chain=None))

        assert len(results) == 1
        assert results[0][0] == "chr1"
        assert len(results[0][1]) == 3

    def test_empty_parser_yields_nothing(self):
        """Empty parser produces no yields."""
        ingester = self._make_ingester()
        results = list(ingester._stream_variants_by_chr([], filter_chain=None))
        assert results == []

    def test_generator_yields_incrementally(self):
        """Generator yields chromosomes one at a time, not all at once."""
        ingester = self._make_ingester()
        recs = [
            self._make_record("chr1:100:A:T", "chr1", 100),
            self._make_record("chr2:200:G:C", "chr2", 200, ref="G", alt="C"),
            self._make_record("chr3:300:C:A", "chr3", 300, ref="C", alt="A"),
        ]

        gen = ingester._stream_variants_by_chr(recs, filter_chain=None)

        chrom1, variants1 = next(gen)
        assert chrom1 == "chr1"
        assert len(variants1) == 1

        chrom2, variants2 = next(gen)
        assert chrom2 == "chr2"
        assert len(variants2) == 1

        chrom3, variants3 = next(gen)
        assert chrom3 == "chr3"
        assert len(variants3) == 1


# ---------------------------------------------------------------------------
# TestCLIIngestOnDuplicate
# ---------------------------------------------------------------------------


class TestCLIIngestOnDuplicate:
    """Test the --on-duplicate CLI option is registered."""

    def test_on_duplicate_option_exists(self):
        """The ingest command has --on-duplicate option."""
        from click.testing import CliRunner

        from graphmana.cli import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["ingest", "--help"])
        assert "--on-duplicate" in result.output
        assert "error" in result.output
        assert "skip" in result.output

    def test_mode_option_exists(self):
        """The ingest command has --mode option with all choices."""
        from click.testing import CliRunner

        from graphmana.cli import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["ingest", "--help"])
        assert "--mode" in result.output
        assert "auto" in result.output
        assert "initial" in result.output
        assert "incremental" in result.output


# ---------------------------------------------------------------------------
# TestRunOrchestration
# ---------------------------------------------------------------------------


class TestRunOrchestration:
    """Test the full IncrementalIngester.run() orchestration."""

    def test_run_returns_summary_dict(self):
        """run() returns a summary dict with all expected keys."""
        conn = _make_conn()
        pm = _make_pop_map(["S3"], ["POP1"], {"S3": "POP1"})

        # Mock execute_read to return empty chromosome list
        from graphmana.db.connection import _EagerResult

        conn.execute_read.return_value = _EagerResult([])

        ingester = IncrementalIngester(
            conn=conn,
            pop_map_new=pm,
            n_existing=2,
            existing_sample_ids={"S1", "S2"},
            existing_pop_ids=["POP1"],
            packed_index_offset=2,
            dataset_id="ds1",
            source_file="test.vcf.gz",
            n_total_samples=3,
        )

        # Empty parser — no variants to process
        summary = ingester.run([], chunk_size=100)

        assert "n_variants_extended" in summary
        assert "n_variants_homref_extended" in summary
        assert "n_variants_created" in summary
        assert "n_samples_created" in summary
        assert "n_populations_created" in summary
        assert "n_total_samples" in summary
        assert summary["n_total_samples"] == 3

    def test_run_homref_extends_missing_chromosomes(self):
        """Chromosomes in DB but not in new VCF get HomRef-extended."""
        conn = _make_conn()
        pm = _make_pop_map(["S3"], ["POP1"], {"S3": "POP1"})

        from graphmana.db.connection import _EagerResult

        # First call: FETCH_CHROMOSOMES returns chr1 and chr2
        # Subsequent calls: return empty results for variant IDs, population counts, etc.
        call_count = [0]
        chr_result = _EagerResult([{"chr": "chr1"}, {"chr": "chr2"}])
        empty_result = _EagerResult([])

        def side_effect(query, params=None):
            call_count[0] += 1
            if call_count[0] == 1:
                return chr_result
            return empty_result

        conn.execute_read.side_effect = side_effect

        ingester = IncrementalIngester(
            conn=conn,
            pop_map_new=pm,
            n_existing=2,
            existing_sample_ids={"S1", "S2"},
            existing_pop_ids=["POP1"],
            packed_index_offset=2,
            dataset_id="ds1",
            source_file="test.vcf.gz",
            n_total_samples=3,
        )

        # Empty parser — no new variants, so both chr1 and chr2 need HomRef extension
        summary = ingester.run([], chunk_size=100)

        # Both chromosomes should have been processed (even with 0 variants each)
        assert summary["n_variants_extended"] == 0
        assert summary["n_samples_created"] == 1


# ---------------------------------------------------------------------------
# TestServerSidePath
# ---------------------------------------------------------------------------


class TestServerSidePath:
    """Test server-side Java procedure detection and fallback."""

    def test_check_server_side_available(self):
        """Detects when graphmana.extendVariants procedure exists."""
        conn = _make_conn()
        pm = _make_pop_map(["S3"], ["POP1"], {"S3": "POP1"})

        from graphmana.db.connection import _EagerResult

        conn.execute_read.return_value = _EagerResult(
            [{"name": "graphmana.extendVariants"}]
        )

        ingester = IncrementalIngester(
            conn=conn,
            pop_map_new=pm,
            n_existing=2,
            existing_sample_ids={"S1", "S2"},
            existing_pop_ids=["POP1"],
            packed_index_offset=2,
            dataset_id="ds1",
            source_file="test.vcf.gz",
            n_total_samples=3,
        )

        assert ingester._check_server_side() is True
        assert ingester._server_side_available is True

    def test_check_server_side_not_available(self):
        """Falls back to Python path when procedure not found."""
        conn = _make_conn()
        pm = _make_pop_map(["S3"], ["POP1"], {"S3": "POP1"})

        from graphmana.db.connection import _EagerResult

        conn.execute_read.return_value = _EagerResult([])

        ingester = IncrementalIngester(
            conn=conn,
            pop_map_new=pm,
            n_existing=2,
            existing_sample_ids={"S1", "S2"},
            existing_pop_ids=["POP1"],
            packed_index_offset=2,
            dataset_id="ds1",
            source_file="test.vcf.gz",
            n_total_samples=3,
        )

        assert ingester._check_server_side() is False
        assert ingester._server_side_available is False

    def test_check_server_side_caches_result(self):
        """Procedure check is cached after first call."""
        conn = _make_conn()
        pm = _make_pop_map(["S3"], ["POP1"], {"S3": "POP1"})

        from graphmana.db.connection import _EagerResult

        conn.execute_read.return_value = _EagerResult([])

        ingester = IncrementalIngester(
            conn=conn,
            pop_map_new=pm,
            n_existing=2,
            existing_sample_ids={"S1", "S2"},
            existing_pop_ids=["POP1"],
            packed_index_offset=2,
            dataset_id="ds1",
            source_file="test.vcf.gz",
            n_total_samples=3,
        )

        ingester._check_server_side()
        ingester._check_server_side()
        # Should only query once
        assert conn.execute_read.call_count == 1

    def test_check_server_side_exception_fallback(self):
        """Exception during SHOW PROCEDURES falls back to Python path."""
        conn = _make_conn()
        pm = _make_pop_map(["S3"], ["POP1"], {"S3": "POP1"})

        conn.execute_read.side_effect = Exception("Neo4j error")

        ingester = IncrementalIngester(
            conn=conn,
            pop_map_new=pm,
            n_existing=2,
            existing_sample_ids={"S1", "S2"},
            existing_pop_ids=["POP1"],
            packed_index_offset=2,
            dataset_id="ds1",
            source_file="test.vcf.gz",
            n_total_samples=3,
        )

        assert ingester._check_server_side() is False

    def test_server_side_extend_batches_correctly(self):
        """Server-side extend sends chunked CALL statements."""
        conn = _make_conn()
        pm = _make_pop_map(["S3", "S4"], ["POP1"], {"S3": "POP1", "S4": "POP1"})

        # Mock execute_write to return indexable result with [0] access
        mock_result = MagicMock()
        mock_result.__getitem__ = MagicMock(
            return_value={"extended": 2, "failed": 0}
        )
        conn.execute_write.return_value = mock_result

        ingester = IncrementalIngester(
            conn=conn,
            pop_map_new=pm,
            n_existing=2,
            existing_sample_ids={"S1", "S2"},
            existing_pop_ids=["POP1"],
            packed_index_offset=2,
            dataset_id="ds1",
            source_file="test.vcf.gz",
            n_total_samples=4,
        )

        vd1 = _make_variant_data("chr1:100:A:T", "chr1", 100, n_new=2)
        vd2 = _make_variant_data("chr1:200:G:C", "chr1", 200, n_new=2)

        n_ext, n_fail = ingester._server_side_extend(
            "chr1", {"chr1:100:A:T": vd1, "chr1:200:G:C": vd2}, chunk_size=10
        )

        assert n_ext == 2
        assert n_fail == 0
        conn.execute_write.assert_called_once()

    def test_server_side_homref(self):
        """Server-side HomRef extend sends correct parameters."""
        conn = _make_conn()
        pm = _make_pop_map(["S3", "S4"], ["POP1"], {"S3": "POP1", "S4": "POP1"})

        mock_result = MagicMock()
        mock_result.__getitem__ = MagicMock(
            return_value={"extended": 100, "failed": 0}
        )
        conn.execute_write.return_value = mock_result

        ingester = IncrementalIngester(
            conn=conn,
            pop_map_new=pm,
            n_existing=10,
            existing_sample_ids={f"S{i}" for i in range(10)},
            existing_pop_ids=["POP1"],
            packed_index_offset=10,
            dataset_id="ds1",
            source_file="test.vcf.gz",
            n_total_samples=12,
        )

        n_ext, n_fail = ingester._server_side_homref("chr1", 2)

        assert n_ext == 100
        assert n_fail == 0
        # Verify the call includes correct population AN
        call_args = conn.execute_write.call_args
        params = call_args[1] if call_args[1] else call_args[0][1]
        assert params["nExisting"] == 10
        assert params["nNew"] == 2
        assert params["newPopIds"] == ["POP1"]
        assert params["newPopAn"] == [4]  # 2 * 2 samples in POP1


# ---------------------------------------------------------------------------
# TestProvenanceRecording
# ---------------------------------------------------------------------------


class TestProvenanceRecording:
    """Test provenance recording in incremental pipeline."""

    @patch("graphmana.ingest.pipeline.VCFParser")
    @patch("graphmana.db.connection.GraphDatabase")
    def test_provenance_recorded_on_success(self, mock_gdb, mock_parser_cls):
        """Successful incremental import records provenance."""
        from graphmana.ingest.pipeline import run_incremental

        # Setup mock driver/session
        mock_driver = MagicMock()
        mock_session = MagicMock()

        # Various queries return different results
        call_idx = [0]
        responses = {
            0: [{"ids": ["S1", "S2"]}],  # FETCH_EXISTING_SAMPLE_IDS
            1: [{"max_idx": 1}],  # FETCH_MAX_PACKED_INDEX
            2: [{"ids": ["POP1"]}],  # existing pop IDs
        }

        def run_side_effect(query, params=None):
            result = MagicMock()
            idx = call_idx[0]
            data = responses.get(idx, [])
            result.__iter__ = MagicMock(return_value=iter(data))
            result.consume.return_value = MagicMock()
            call_idx[0] += 1
            return result

        mock_session.run.side_effect = run_side_effect
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)
        mock_driver.session.return_value = mock_session
        mock_driver.verify_connectivity = MagicMock()
        mock_driver.close = MagicMock()
        mock_gdb.driver.return_value = mock_driver

        # Setup mock parser
        mock_parser = MagicMock()
        mock_pm = _make_pop_map(["S3"], ["POP1"], {"S3": "POP1"})
        mock_parser.pop_map = mock_pm
        mock_parser.__iter__ = MagicMock(return_value=iter([]))
        mock_parser_cls.return_value = mock_parser

        with patch("graphmana.ingest.incremental.IncrementalIngester") as mock_ingester_cls:
            mock_ingester = MagicMock()
            mock_ingester.run.return_value = {
                "n_variants_extended": 10,
                "n_variants_created": 0,
                "n_samples_created": 1,
            }
            mock_ingester_cls.return_value = mock_ingester

            with patch("graphmana.provenance.manager.ProvenanceManager") as mock_prov_cls:
                mock_prov = MagicMock()
                mock_prov_cls.return_value = mock_prov

                summary = run_incremental(
                    "test.vcf.gz",
                    "panel.tsv",
                    neo4j_uri="bolt://localhost:7687",
                )

                assert summary["provenance_recorded"] is True
                mock_prov.record_ingestion.assert_called_once()
                call_kwargs = mock_prov.record_ingestion.call_args
                # Verify mode is "incremental"
                assert call_kwargs[1]["mode"] == "incremental" or (
                    len(call_kwargs[0]) > 2 and call_kwargs[0][2] == "incremental"
                )

    @patch("graphmana.ingest.pipeline.VCFParser")
    @patch("graphmana.db.connection.GraphDatabase")
    def test_provenance_failure_does_not_block_import(self, mock_gdb, mock_parser_cls):
        """Failed provenance recording sets flag but returns summary."""
        from graphmana.ingest.pipeline import run_incremental

        mock_driver = MagicMock()
        mock_session = MagicMock()

        call_idx = [0]
        responses = {
            0: [{"ids": ["S1"]}],
            1: [{"max_idx": 0}],
            2: [{"ids": ["POP1"]}],
        }

        def run_side_effect(query, params=None):
            result = MagicMock()
            idx = call_idx[0]
            data = responses.get(idx, [])
            result.__iter__ = MagicMock(return_value=iter(data))
            result.consume.return_value = MagicMock()
            call_idx[0] += 1
            return result

        mock_session.run.side_effect = run_side_effect
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)
        mock_driver.session.return_value = mock_session
        mock_driver.verify_connectivity = MagicMock()
        mock_driver.close = MagicMock()
        mock_gdb.driver.return_value = mock_driver

        mock_parser = MagicMock()
        mock_pm = _make_pop_map(["S2"], ["POP1"], {"S2": "POP1"})
        mock_parser.pop_map = mock_pm
        mock_parser.__iter__ = MagicMock(return_value=iter([]))
        mock_parser_cls.return_value = mock_parser

        with patch("graphmana.ingest.incremental.IncrementalIngester") as mock_ingester_cls:
            mock_ingester = MagicMock()
            mock_ingester.run.return_value = {
                "n_variants_extended": 5,
                "n_variants_created": 0,
                "n_samples_created": 1,
            }
            mock_ingester_cls.return_value = mock_ingester

            with patch("graphmana.provenance.manager.ProvenanceManager") as mock_prov_cls:
                mock_prov = MagicMock()
                mock_prov.record_ingestion.side_effect = Exception("DB write failed")
                mock_prov_cls.return_value = mock_prov

                summary = run_incremental(
                    "test.vcf.gz",
                    "panel.tsv",
                    neo4j_uri="bolt://localhost:7687",
                )

                # Import still succeeds
                assert summary["n_variants_extended"] == 5
                # But provenance flag is False
                assert summary["provenance_recorded"] is False


# ---------------------------------------------------------------------------
# TestByteBoundaryEdgeCases
# ---------------------------------------------------------------------------


class TestByteBoundaryEdgeCases:
    """Test packed array operations at byte boundaries."""

    def test_extend_at_4_sample_boundary(self):
        """4 existing samples = exactly 1 byte of gt_packed; extend to 5."""
        existing_cyvcf2 = np.array([0, 1, 3, 2], dtype=np.int8)
        existing_gt = vectorized_gt_pack(existing_cyvcf2)
        assert len(existing_gt) == 1  # 4 samples = 1 byte

        new_gt = np.array([1], dtype=np.int8)  # 1 new sample (Het)
        result = extend_gt_packed(existing_gt, 4, new_gt)

        unpacked = unpack_genotypes(result, 5)
        expected_existing = GT_REMAP[existing_cyvcf2].astype(np.int8)
        np.testing.assert_array_equal(unpacked[:4], expected_existing)
        expected_new = GT_REMAP[new_gt].astype(np.int8)
        np.testing.assert_array_equal(unpacked[4:], expected_new)

    def test_extend_at_8_sample_phase_boundary(self):
        """8 existing samples = exactly 1 byte of phase_packed; extend to 9."""
        from graphmana.ingest.array_ops import extend_phase_packed

        existing_phase = bytes([0b10101010])  # 8 samples, alternating phase
        new_phase = np.array([1], dtype=np.uint8)
        result = extend_phase_packed(existing_phase, 8, new_phase)

        unpacked = unpack_phase(result, 9)
        # Existing 8 samples preserved
        for i in range(8):
            assert unpacked[i] == ((0b10101010 >> i) & 1)
        # New sample has phase=1
        assert unpacked[8] == 1

    def test_extend_single_sample(self):
        """Extend from 1 existing sample to 2."""
        existing_cyvcf2 = np.array([1], dtype=np.int8)  # Het
        existing_gt = vectorized_gt_pack(existing_cyvcf2)

        new_gt = np.array([3], dtype=np.int8)  # HomAlt in cyvcf2
        result = extend_gt_packed(existing_gt, 1, new_gt)

        unpacked = unpack_genotypes(result, 2)
        assert unpacked[0] == GT_REMAP[1]  # Het
        assert unpacked[1] == GT_REMAP[3]  # HomAlt
