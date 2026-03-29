"""Tests for VCF export (pure function tests, no Neo4j)."""

import gzip
import tempfile
from pathlib import Path

from graphmana.export.vcf_export import (
    BGZFWriter,
    _BGZF_EOF,
    _resolve_output_type,
    format_gt,
    format_variant_line,
)


class TestFormatGt:
    """Test VCF GT string formatting."""

    # Unphased diploid
    def test_homref_unphased(self):
        assert format_gt(0, 0, False, False) == "0/0"

    def test_het_unphased(self):
        assert format_gt(1, 0, False, False) == "0/1"

    def test_homalt_unphased(self):
        assert format_gt(2, 0, False, False) == "1/1"

    def test_missing_unphased(self):
        assert format_gt(3, 0, False, False) == "./."

    # Phased diploid
    def test_homref_phased(self):
        assert format_gt(0, 0, False, True) == "0|0"

    def test_het_phased_alt_second(self):
        """Phase=1 → ALT on second haplotype → 0|1."""
        assert format_gt(1, 1, False, True) == "0|1"

    def test_het_phased_alt_first(self):
        """Phase=0 → ALT on first haplotype → 1|0."""
        assert format_gt(1, 0, False, True) == "1|0"

    def test_homalt_phased(self):
        assert format_gt(2, 0, False, True) == "1|1"

    def test_missing_phased(self):
        assert format_gt(3, 0, False, True) == ".|."

    # Haploid
    def test_haploid_ref(self):
        assert format_gt(0, 0, True, False) == "0"

    def test_haploid_alt(self):
        assert format_gt(1, 0, True, False) == "1"

    def test_haploid_homalt(self):
        assert format_gt(2, 0, True, False) == "1"

    def test_haploid_missing(self):
        assert format_gt(3, 0, True, False) == "."

    # Haploid ignores phased flag
    def test_haploid_phased_flag_ignored(self):
        assert format_gt(0, 0, True, True) == "0"
        assert format_gt(1, 1, True, True) == "1"


class TestFormatVariantLine:
    """Test VCF data line formatting."""

    def test_basic_line(self):
        props = {
            "chr": "chr1",
            "pos": 100,
            "variantId": "chr1_100_A_T",
            "ref": "A",
            "alt": "T",
            "qual": 30.0,
            "filter": "PASS",
            "ac_total": 5,
            "an_total": 10,
            "af_total": 0.5,
            "variant_type": "SNP",
        }
        gt_strings = ["0/0", "0/1", "1/1"]
        line = format_variant_line(props, gt_strings)
        fields = line.split("\t")
        assert fields[0] == "chr1"
        assert fields[1] == "100"
        assert fields[2] == "chr1_100_A_T"
        assert fields[3] == "A"
        assert fields[4] == "T"
        assert fields[5] == "30.0"
        assert fields[6] == "PASS"
        assert "AC=5" in fields[7]
        assert "AN=10" in fields[7]
        assert "AF=0.5" in fields[7]
        assert "VT=SNP" in fields[7]
        assert fields[8] == "GT"
        assert fields[9] == "0/0"
        assert fields[10] == "0/1"
        assert fields[11] == "1/1"

    def test_missing_qual(self):
        props = {"chr": "chr1", "pos": 100, "variantId": ".", "ref": "A", "alt": "T"}
        line = format_variant_line(props, ["0/0"])
        fields = line.split("\t")
        assert fields[5] == "."

    def test_missing_filter(self):
        props = {"chr": "chr1", "pos": 100, "variantId": ".", "ref": "A", "alt": "T"}
        line = format_variant_line(props, ["0/0"])
        fields = line.split("\t")
        assert fields[6] == "."

    def test_no_info(self):
        props = {"chr": "chr1", "pos": 100, "variantId": ".", "ref": "A", "alt": "T"}
        line = format_variant_line(props, ["0/0"])
        fields = line.split("\t")
        assert fields[7] == "."

    def test_empty_gt_strings(self):
        props = {"chr": "chr1", "pos": 100, "variantId": ".", "ref": "A", "alt": "T"}
        line = format_variant_line(props, [])
        fields = line.split("\t")
        assert len(fields) == 9  # CHROM through FORMAT, no samples


class TestVcfHeader:
    """Test VCF header generation via VCFExporter._write_header."""

    def test_header_structure(self):
        from io import StringIO

        from graphmana.export.vcf_export import VCFExporter

        # We can't instantiate VCFExporter without a connection, but we can
        # test _write_header as an unbound method by creating a minimal instance
        # Instead, test the header output directly
        buf = StringIO()
        sample_ids = ["S1", "S2", "S3"]
        chromosomes = [{"chr": "chr1", "length": 100000}, {"chr": "chr2", "length": None}]

        from unittest.mock import MagicMock

        exporter = MagicMock(spec=VCFExporter)
        exporter._write_default_header = VCFExporter._write_default_header.__get__(exporter)
        exporter._write_chrom_line = VCFExporter._write_chrom_line
        exporter._write_default_header(buf, sample_ids, chromosomes)
        header = buf.getvalue()

        assert header.startswith("##fileformat=VCFv4.3\n")
        assert "##source=GraphMana\n" in header
        assert "##contig=<ID=chr1,length=100000>\n" in header
        assert "##contig=<ID=chr2>\n" in header
        assert "##INFO=<ID=AC" in header
        assert "##INFO=<ID=AN" in header
        assert "##INFO=<ID=AF" in header
        assert "##FORMAT=<ID=GT" in header

        lines = header.strip().split("\n")
        last_line = lines[-1]
        assert last_line.startswith("#CHROM\tPOS\tID")
        assert last_line.endswith("S1\tS2\tS3")


class TestResolveOutputType:
    """Test output type resolution from flag and file extension."""

    def test_explicit_v(self):
        assert _resolve_output_type(Path("out.vcf.gz"), "v") == "v"

    def test_explicit_z(self):
        assert _resolve_output_type(Path("out.vcf"), "z") == "z"

    def test_explicit_b(self):
        assert _resolve_output_type(Path("out.vcf"), "b") == "b"

    def test_auto_vcf(self):
        assert _resolve_output_type(Path("out.vcf"), None) == "v"

    def test_auto_vcf_gz(self):
        assert _resolve_output_type(Path("out.vcf.gz"), None) == "z"

    def test_auto_gz(self):
        assert _resolve_output_type(Path("output.gz"), None) == "z"

    def test_auto_bcf(self):
        assert _resolve_output_type(Path("out.bcf"), None) == "b"

    def test_auto_no_extension(self):
        assert _resolve_output_type(Path("output"), None) == "v"


class TestBGZFWriter:
    """Test BGZF compressed output."""

    def test_small_output_gzip_readable(self):
        """BGZF output must be readable by Python gzip module."""
        with tempfile.NamedTemporaryFile(suffix=".vcf.gz", delete=False) as tmp:
            path = tmp.name
            with BGZFWriter(tmp) as w:
                w.write("line1\n")
                w.write("line2\n")

        with gzip.open(path, "rt") as f:
            lines = f.readlines()
        assert lines == ["line1\n", "line2\n"]
        Path(path).unlink()

    def test_eof_marker_present(self):
        """BGZF output must end with the standard EOF marker."""
        with tempfile.NamedTemporaryFile(suffix=".vcf.gz", delete=False) as tmp:
            path = tmp.name
            with BGZFWriter(tmp) as w:
                w.write("data\n")

        with open(path, "rb") as f:
            data = f.read()
        assert data.endswith(_BGZF_EOF)
        Path(path).unlink()

    def test_multi_block(self):
        """Data larger than one BGZF block must still be fully readable."""
        with tempfile.NamedTemporaryFile(suffix=".vcf.gz", delete=False) as tmp:
            path = tmp.name
            with BGZFWriter(tmp) as w:
                for i in range(3000):
                    w.write(f"chr1\t{i}\tA\tT\n")

        with gzip.open(path, "rt") as f:
            lines = f.readlines()
        assert len(lines) == 3000
        assert lines[0] == "chr1\t0\tA\tT\n"
        assert lines[-1] == "chr1\t2999\tA\tT\n"
        Path(path).unlink()

    def test_empty_output(self):
        """Empty BGZF file must still have the EOF marker."""
        with tempfile.NamedTemporaryFile(suffix=".vcf.gz", delete=False) as tmp:
            path = tmp.name
            with BGZFWriter(tmp) as w:
                pass  # Write nothing

        with open(path, "rb") as f:
            data = f.read()
        assert data == _BGZF_EOF
        Path(path).unlink()

    def test_vcf_header_and_data(self):
        """Full VCF content must round-trip through BGZF."""
        with tempfile.NamedTemporaryFile(suffix=".vcf.gz", delete=False) as tmp:
            path = tmp.name
            with BGZFWriter(tmp) as w:
                w.write("##fileformat=VCFv4.3\n")
                w.write("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\n")
                w.write("chr1\t100\t.\tA\tT\t.\t.\t.\tGT\t0/1\n")

        with gzip.open(path, "rt") as f:
            lines = f.readlines()
        assert len(lines) == 3
        assert lines[0].startswith("##fileformat")
        assert lines[1].startswith("#CHROM")
        assert "0/1" in lines[2]
        Path(path).unlink()
