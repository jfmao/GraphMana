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


class TestCollectVariantsByChr:
    """Test chromosome-grouped variant collection."""

    def test_groups_by_chromosome(self):
        """Variants from parser are grouped by chromosome."""
        from graphmana.ingest.vcf_parser import VariantRecord

        conn = _make_conn()
        pm = _make_pop_map(["S1"], ["POP1"], {"S1": "POP1"})

        ingester = IncrementalIngester(
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

        # Create mock records with different chromosomes
        rec1 = VariantRecord(
            id="chr1:100:A:T",
            chr="chr1",
            pos=100,
            ref="A",
            alt="T",
            variant_type="SNP",
            ac=[1],
            an=[2],
            af=[0.5],
            het_count=[1],
            hom_alt_count=[0],
            het_exp=[0.5],
            ac_total=1,
            an_total=2,
            af_total=0.5,
            call_rate=1.0,
            gt_packed=vectorized_gt_pack(np.array([1], dtype=np.int8)),
            phase_packed=bytes(1),
            ploidy_packed=b"",
        )
        rec2 = VariantRecord(
            id="chr2:200:G:C",
            chr="chr2",
            pos=200,
            ref="G",
            alt="C",
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
            gt_packed=vectorized_gt_pack(np.array([0], dtype=np.int8)),
            phase_packed=bytes(1),
            ploidy_packed=b"",
        )

        mock_parser = [rec1, rec2]

        result = ingester._collect_variants_by_chr(mock_parser, filter_chain=None)

        assert "chr1" in result
        assert "chr2" in result
        assert "chr1:100:A:T" in result["chr1"]
        assert "chr2:200:G:C" in result["chr2"]


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
