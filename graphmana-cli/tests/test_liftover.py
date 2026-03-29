"""Tests for liftover chain_parser and lifter modules."""

import csv
import inspect
from unittest.mock import MagicMock

import pytest

from graphmana.liftover.chain_parser import (
    LiftoverConverter,
    LiftoverResult,
    UnmappedVariant,
    complement,
    reverse_complement,
)
from graphmana.liftover.lifter import GraphLiftover

# ---------------------------------------------------------------------------
# complement / reverse_complement
# ---------------------------------------------------------------------------


class TestComplement:
    """Test single-base complement."""

    def test_a_to_t(self):
        assert complement("A") == "T"

    def test_t_to_a(self):
        assert complement("T") == "A"

    def test_c_to_g(self):
        assert complement("C") == "G"

    def test_g_to_c(self):
        assert complement("G") == "C"

    def test_lowercase(self):
        assert complement("a") == "t"
        assert complement("t") == "a"
        assert complement("c") == "g"
        assert complement("g") == "c"

    def test_invalid_base_raises(self):
        with pytest.raises(KeyError):
            complement("X")


class TestReverseComplement:
    """Test reverse complement of nucleotide sequences."""

    def test_single_base_a(self):
        assert reverse_complement("A") == "T"

    def test_single_base_c(self):
        assert reverse_complement("C") == "G"

    def test_multi_base_atg(self):
        assert reverse_complement("ATG") == "CAT"

    def test_multi_base_gattaca(self):
        assert reverse_complement("GATTACA") == "TGTAATC"

    def test_empty_string(self):
        assert reverse_complement("") == ""

    def test_lowercase(self):
        assert reverse_complement("atg") == "cat"

    def test_single_repeat(self):
        assert reverse_complement("AAAA") == "TTTT"

    def test_palindrome(self):
        # ATAT → reverse complement = ATAT
        assert reverse_complement("ATAT") == "ATAT"

    def test_round_trip(self):
        seq = "ACGTACGT"
        assert reverse_complement(reverse_complement(seq)) == seq


# ---------------------------------------------------------------------------
# LiftoverResult / UnmappedVariant dataclasses
# ---------------------------------------------------------------------------


class TestLiftoverResult:
    """Test LiftoverResult frozen dataclass."""

    def test_creation(self):
        r = LiftoverResult(
            new_chr="chr1",
            new_pos=12345,
            strand="+",
            score=1.0,
            new_ref="A",
            new_alt="G",
            new_variant_id="chr1-12345-A-G",
        )
        assert r.new_chr == "chr1"
        assert r.new_pos == 12345
        assert r.strand == "+"
        assert r.new_ref == "A"
        assert r.new_alt == "G"
        assert r.new_variant_id == "chr1-12345-A-G"

    def test_frozen(self):
        r = LiftoverResult("chr1", 100, "+", 1.0, "A", "G", "chr1-100-A-G")
        with pytest.raises(AttributeError):
            r.new_pos = 200


class TestUnmappedVariant:
    """Test UnmappedVariant frozen dataclass."""

    def test_creation(self):
        u = UnmappedVariant("chr1-100-A-G", "chr1", 100, "A", "G", "unmapped")
        assert u.variant_id == "chr1-100-A-G"
        assert u.chr == "chr1"
        assert u.pos == 100
        assert u.ref == "A"
        assert u.alt == "G"
        assert u.reason == "unmapped"

    def test_frozen(self):
        u = UnmappedVariant("chr1-100-A-G", "chr1", 100, "A", "G", "unmapped")
        with pytest.raises(AttributeError):
            u.reason = "ambiguous"

    def test_reason_values(self):
        for reason in ("unmapped", "ambiguous", "collision"):
            u = UnmappedVariant("v1", "chr1", 1, "A", "G", reason)
            assert u.reason == reason


# ---------------------------------------------------------------------------
# LiftoverConverter interface
# ---------------------------------------------------------------------------


class TestLiftoverConverterInterface:
    """Test LiftoverConverter class structure and error handling."""

    def test_class_exists(self):
        assert LiftoverConverter is not None

    def test_has_convert_method(self):
        assert hasattr(LiftoverConverter, "convert")
        assert callable(LiftoverConverter.convert)

    def test_convert_signature(self):
        sig = inspect.signature(LiftoverConverter.convert)
        params = list(sig.parameters.keys())
        assert "variant_id" in params
        assert "chr" in params
        assert "pos" in params
        assert "ref" in params
        assert "alt" in params

    def test_file_not_found(self):
        with pytest.raises(FileNotFoundError):
            LiftoverConverter("/nonexistent/path/to/chain.file")


# ---------------------------------------------------------------------------
# GraphLiftover
# ---------------------------------------------------------------------------


class TestGraphLiftoverInterface:
    """Test GraphLiftover class structure."""

    def test_class_exists(self):
        assert GraphLiftover is not None

    def test_has_run_method(self):
        assert hasattr(GraphLiftover, "run")
        assert callable(GraphLiftover.run)

    def test_run_signature(self):
        sig = inspect.signature(GraphLiftover.run)
        params = list(sig.parameters.keys())
        assert "dry_run" in params
        assert "reject_file" in params
        assert "update_annotations" in params
        assert "batch_size" in params


class TestGraphLiftoverDryRun:
    """Test dry-run mode — no DB writes should occur."""

    def _make_mock_conn(self, chromosomes, variants_by_chr):
        """Build a mock connection that returns specified chromosomes and variants."""
        conn = MagicMock()

        def session_run(query, params=None):
            result = MagicMock()
            # Route based on query content
            if "v.variantId AS variantId" in query:
                # FETCH_VARIANT_COORDS_BY_CHR
                records = variants_by_chr.get(params["chr"], [])
                result.__iter__ = lambda s: iter(records)
                return result
            if "Chromosome" in query and params is None:
                # FETCH_CHROMOSOMES
                records = [{"chr": c, "length": 1000000} for c in chromosomes]
                result.__iter__ = lambda s: iter(records)
                return result
            result.__iter__ = lambda s: iter([])
            return result

        session_mock = MagicMock()
        session_mock.run = session_run
        session_mock.__enter__ = lambda s: session_mock
        session_mock.__exit__ = MagicMock(return_value=False)
        conn.driver.session.return_value = session_mock

        return conn

    def _make_mock_converter(self, mapping):
        """Build a mock converter that returns pre-defined results."""
        converter = MagicMock(spec=LiftoverConverter)

        def convert(variant_id, chr, pos, ref, alt):
            if variant_id in mapping:
                return mapping[variant_id]
            return UnmappedVariant(variant_id, chr, pos, ref, alt, "unmapped")

        converter.convert = convert
        return converter

    def test_dry_run_returns_summary_no_writes(self):
        variants = {
            "chr1": [
                {"variantId": "chr1-100-A-G", "chr": "chr1", "pos": 100, "ref": "A", "alt": "G"},
                {"variantId": "chr1-200-T-C", "chr": "chr1", "pos": 200, "ref": "T", "alt": "C"},
            ]
        }
        mapping = {
            "chr1-100-A-G": LiftoverResult("chr1", 150, "+", 1.0, "A", "G", "chr1-150-A-G"),
        }
        # chr1-200-T-C not in mapping → unmapped

        conn = self._make_mock_conn(["chr1"], variants)
        converter = self._make_mock_converter(mapping)

        lifter = GraphLiftover(conn, converter, "GRCh38")
        result = lifter.run(dry_run=True)

        assert result["dry_run"] is True
        assert result["mapped"] == 1
        assert result["unmapped"] == 1
        assert result["ambiguous"] == 0
        assert result["collision"] == 0
        assert result["total_variants"] == 2
        assert result["target_reference"] == "GRCh38"

    def test_dry_run_empty_database(self):
        conn = self._make_mock_conn([], {})
        converter = MagicMock(spec=LiftoverConverter)

        lifter = GraphLiftover(conn, converter, "GRCh38")
        result = lifter.run(dry_run=True)

        assert result["mapped"] == 0
        assert result["unmapped"] == 0
        assert result["total_variants"] == 0


# ---------------------------------------------------------------------------
# Collision detection
# ---------------------------------------------------------------------------


class TestCollisionDetection:
    """Test that self-collisions are correctly detected."""

    def _make_mock_conn(self, chromosomes, variants_by_chr):
        conn = MagicMock()

        def session_run(query, params=None):
            result = MagicMock()
            if "v.variantId AS variantId" in query:
                records = variants_by_chr.get(params["chr"], [])
                result.__iter__ = lambda s: iter(records)
                return result
            if "Chromosome" in query and params is None:
                records = [{"chr": c, "length": 1000000} for c in chromosomes]
                result.__iter__ = lambda s: iter(records)
                return result
            result.__iter__ = lambda s: iter([])
            return result

        session_mock = MagicMock()
        session_mock.run = session_run
        session_mock.__enter__ = lambda s: session_mock
        session_mock.__exit__ = MagicMock(return_value=False)
        conn.driver.session.return_value = session_mock

        return conn

    def test_two_variants_same_target_one_collision(self):
        """Two source variants mapping to the same target ID → 1 mapped + 1 collision."""
        variants = {
            "chr1": [
                {"variantId": "chr1-100-A-G", "chr": "chr1", "pos": 100, "ref": "A", "alt": "G"},
                {"variantId": "chr1-200-A-G", "chr": "chr1", "pos": 200, "ref": "A", "alt": "G"},
            ]
        }
        # Both map to the SAME new variant ID
        same_result = LiftoverResult("chr1", 150, "+", 1.0, "A", "G", "chr1-150-A-G")
        converter = MagicMock(spec=LiftoverConverter)
        converter.convert = MagicMock(return_value=same_result)

        conn = self._make_mock_conn(["chr1"], variants)
        lifter = GraphLiftover(conn, converter, "GRCh38")
        result = lifter.run(dry_run=True)

        assert result["mapped"] == 1
        assert result["collision"] == 1
        assert result["total_variants"] == 2


# ---------------------------------------------------------------------------
# Reject file
# ---------------------------------------------------------------------------


class TestRejectFile:
    """Test reject file TSV output."""

    def test_reject_file_content(self, tmp_path):
        reject_path = tmp_path / "rejected.tsv"
        unmapped = [
            UnmappedVariant("chr1-100-A-G", "chr1", 100, "A", "G", "unmapped"),
            UnmappedVariant("chr2-200-T-C", "chr2", 200, "T", "C", "ambiguous"),
        ]
        GraphLiftover._write_reject_file(reject_path, unmapped)

        assert reject_path.exists()

        with open(reject_path) as f:
            reader = csv.reader(f, delimiter="\t")
            rows = list(reader)

        # Header + 2 data rows
        assert len(rows) == 3
        assert rows[0] == ["variantId", "chr", "pos", "ref", "alt", "reason"]
        assert rows[1] == ["chr1-100-A-G", "chr1", "100", "A", "G", "unmapped"]
        assert rows[2] == ["chr2-200-T-C", "chr2", "200", "T", "C", "ambiguous"]

    def test_empty_reject_file(self, tmp_path):
        reject_path = tmp_path / "rejected.tsv"
        GraphLiftover._write_reject_file(reject_path, [])

        with open(reject_path) as f:
            reader = csv.reader(f, delimiter="\t")
            rows = list(reader)

        assert len(rows) == 1  # Header only
        assert rows[0] == ["variantId", "chr", "pos", "ref", "alt", "reason"]
