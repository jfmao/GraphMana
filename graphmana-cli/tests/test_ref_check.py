"""Tests for reference allele verification."""

import tempfile
from pathlib import Path

from click.testing import CliRunner

from graphmana.cli import cli
from graphmana.qc.ref_check import (
    fetch_ref_base_indexed,
    load_fasta_index,
    load_fasta_sequence,
)


class TestRefCheckCommandHelp:
    """Test ref-check command registration."""

    def test_ref_check_in_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["ref-check", "--help"])
        assert result.exit_code == 0
        assert "--fasta" in result.output

    def test_ref_check_has_output_option(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["ref-check", "--help"])
        assert "--output" in result.output

    def test_ref_check_has_chromosomes_option(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["ref-check", "--help"])
        assert "--chromosomes" in result.output

    def test_ref_check_has_max_mismatches(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["ref-check", "--help"])
        assert "--max-mismatches" in result.output


class TestFastaSequenceLoading:
    """Test FASTA sequence loading without index."""

    def test_load_simple_fasta(self):
        with tempfile.NamedTemporaryFile(suffix=".fa", mode="w", delete=False) as f:
            f.write(">chr1\n")
            f.write("ACGTACGTACGT\n")
            f.write("NNNNACGTACGT\n")
            fasta_path = Path(f.name)

        seq = load_fasta_sequence(fasta_path, "chr1")
        assert seq is not None
        assert seq.startswith("ACGTACGT")
        assert len(seq) == 24
        fasta_path.unlink()

    def test_load_missing_chromosome(self):
        with tempfile.NamedTemporaryFile(suffix=".fa", mode="w", delete=False) as f:
            f.write(">chr1\nACGT\n")
            fasta_path = Path(f.name)

        seq = load_fasta_sequence(fasta_path, "chr99")
        assert seq is None
        fasta_path.unlink()

    def test_load_multi_chromosome(self):
        with tempfile.NamedTemporaryFile(suffix=".fa", mode="w", delete=False) as f:
            f.write(">chr1\nACGTACGT\n")
            f.write(">chr2\nTTTTAAAA\n")
            fasta_path = Path(f.name)

        seq1 = load_fasta_sequence(fasta_path, "chr1")
        seq2 = load_fasta_sequence(fasta_path, "chr2")
        assert seq1 == "ACGTACGT"
        assert seq2 == "TTTTAAAA"
        fasta_path.unlink()


class TestFastaIndex:
    """Test .fai index loading and indexed lookup."""

    def test_load_fai_index(self):
        with tempfile.NamedTemporaryFile(suffix=".fa", mode="w", delete=False) as f:
            f.write(">chr1\nACGTACGTAC\n")
            fasta_path = Path(f.name)

        # Create .fai
        fai_path = Path(str(fasta_path) + ".fai")
        # chr1: length=10, offset=6 (after ">chr1\n"), line_bases=10, line_width=11
        fai_path.write_text("chr1\t10\t6\t10\t11\n")

        index = load_fasta_index(fasta_path)
        assert "chr1" in index
        assert index["chr1"][0] == 10  # length

        # Test indexed lookup
        base = fetch_ref_base_indexed(fasta_path, index, "chr1", 1)  # 1-based
        assert base == "A"

        base = fetch_ref_base_indexed(fasta_path, index, "chr1", 4)
        assert base == "T"

        # Multi-base lookup
        bases = fetch_ref_base_indexed(fasta_path, index, "chr1", 1, length=4)
        assert bases == "ACGT"

        fasta_path.unlink()
        fai_path.unlink()

    def test_missing_fai_returns_empty(self):
        index = load_fasta_index(Path("/nonexistent/file.fa"))
        assert index == {}

    def test_indexed_lookup_out_of_range(self):
        with tempfile.NamedTemporaryFile(suffix=".fa", mode="w", delete=False) as f:
            f.write(">chr1\nACGT\n")
            fasta_path = Path(f.name)
        fai_path = Path(str(fasta_path) + ".fai")
        fai_path.write_text("chr1\t4\t6\t4\t5\n")

        index = load_fasta_index(fasta_path)
        base = fetch_ref_base_indexed(fasta_path, index, "chr1", 100)
        assert base is None

        fasta_path.unlink()
        fai_path.unlink()


# ---------------------------------------------------------------------------
# Integration tests for check_ref_alleles()
# ---------------------------------------------------------------------------


class TestCheckRefAlleles:
    """Test the main check_ref_alleles() function with mocked Neo4j."""

    def _make_fasta(self, tmp_path, chrom="chr1", seq="ACGTACGTNN"):
        fasta = tmp_path / "ref.fa"
        fasta.write_text(f">{chrom}\n{seq}\n")
        return fasta

    def _make_conn(self, chroms_and_variants):
        """Create mock conn that returns chroms and variant records.

        chroms_and_variants: dict mapping chrom -> list of (vid, pos, ref)
        """
        from unittest.mock import MagicMock

        conn = MagicMock()
        call_count = [0]
        chrom_list = list(chroms_and_variants.keys())

        def mock_read(query, params=None):
            result = MagicMock()
            if "Chromosome" in query:
                result.__iter__ = MagicMock(
                    return_value=iter([{"chr": c} for c in chrom_list])
                )
            elif "Variant" in query:
                chrom = params["chr"] if params else chrom_list[0]
                variants = chroms_and_variants.get(chrom, [])
                result.__iter__ = MagicMock(
                    return_value=iter([
                        {"vid": v[0], "pos": v[1], "ref": v[2]} for v in variants
                    ])
                )
            else:
                result.__iter__ = MagicMock(return_value=iter([]))
            return result

        conn.execute_read = mock_read
        return conn

    def test_all_matching(self, tmp_path):
        """All ref alleles match the FASTA."""
        from graphmana.qc.ref_check import check_ref_alleles

        fasta = self._make_fasta(tmp_path, seq="ACGTACGT")
        conn = self._make_conn({
            "chr1": [("v1", 1, "A"), ("v2", 2, "C"), ("v3", 3, "G")]
        })

        result = check_ref_alleles(conn, fasta, chromosomes=["chr1"])
        assert result["n_checked"] == 3
        assert result["n_matched"] == 3
        assert result["n_mismatched"] == 0
        assert result["mismatches"] == []

    def test_mismatch_detected(self, tmp_path):
        """Mismatched ref alleles are reported."""
        from graphmana.qc.ref_check import check_ref_alleles

        fasta = self._make_fasta(tmp_path, seq="ACGTACGT")
        conn = self._make_conn({
            "chr1": [("v1", 1, "A"), ("v2", 2, "T")]  # pos 2 is C, not T
        })

        result = check_ref_alleles(conn, fasta, chromosomes=["chr1"])
        assert result["n_checked"] == 2
        assert result["n_matched"] == 1
        assert result["n_mismatched"] == 1
        assert result["mismatches"][0]["stored_ref"] == "T"
        assert result["mismatches"][0]["genome_ref"] == "C"

    def test_multi_base_ref(self, tmp_path):
        """Multi-base REF alleles (indels) are checked correctly."""
        from graphmana.qc.ref_check import check_ref_alleles

        fasta = self._make_fasta(tmp_path, seq="ACGTACGT")
        conn = self._make_conn({
            "chr1": [("v1", 1, "ACG")]  # 3-base ref at pos 1
        })

        result = check_ref_alleles(conn, fasta, chromosomes=["chr1"])
        assert result["n_matched"] == 1

    def test_max_mismatches_stops_early(self, tmp_path):
        """Early exit when max_mismatches is reached."""
        from graphmana.qc.ref_check import check_ref_alleles

        fasta = self._make_fasta(tmp_path, seq="AAAAAAAAAA")
        conn = self._make_conn({
            "chr1": [
                ("v1", 1, "T"), ("v2", 2, "T"), ("v3", 3, "T"),
                ("v4", 4, "T"), ("v5", 5, "T"),
            ]
        })

        result = check_ref_alleles(
            conn, fasta, chromosomes=["chr1"], max_mismatches=2
        )
        assert result["n_mismatched"] == 2
        assert result["stopped_early"] is True

    def test_missing_chromosome_in_fasta(self, tmp_path):
        """Chromosomes not in FASTA are skipped gracefully."""
        from graphmana.qc.ref_check import check_ref_alleles

        fasta = self._make_fasta(tmp_path, chrom="chr1", seq="ACGT")
        conn = self._make_conn({
            "chr99": [("v1", 1, "A")]  # chr99 not in FASTA
        })

        result = check_ref_alleles(conn, fasta, chromosomes=["chr99"])
        assert result["n_checked"] == 0

    def test_multi_chromosome(self, tmp_path):
        """Variants across multiple chromosomes are checked."""
        from graphmana.qc.ref_check import check_ref_alleles

        fasta = tmp_path / "ref.fa"
        fasta.write_text(">chr1\nACGT\n>chr2\nTTTT\n")
        conn = self._make_conn({
            "chr1": [("v1", 1, "A")],
            "chr2": [("v2", 1, "T")],
        })

        result = check_ref_alleles(conn, fasta, chromosomes=["chr1", "chr2"])
        assert result["n_checked"] == 2
        assert result["n_matched"] == 2

    def test_empty_database(self, tmp_path):
        """No variants to check returns zero counts."""
        from graphmana.qc.ref_check import check_ref_alleles

        fasta = self._make_fasta(tmp_path, seq="ACGT")
        conn = self._make_conn({"chr1": []})

        result = check_ref_alleles(conn, fasta, chromosomes=["chr1"])
        assert result["n_checked"] == 0
        assert result["n_matched"] == 0
        assert result["n_mismatched"] == 0
