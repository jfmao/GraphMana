"""Tests for VCF header preservation and qual/filter roundtrip (v0.5 item 10)."""

import csv

import numpy as np
import pytest

from graphmana.ingest.csv_emitter import (
    VARIANT_HEADER,
    VCFHEADER_HEADER,
    CSVEmitter,
)
from graphmana.ingest.population_map import PopulationMap
from graphmana.ingest.vcf_parser import VariantRecord

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def pop_map():
    """Minimal PopulationMap with 4 samples in 2 populations."""
    return PopulationMap(
        sample_ids=["S1", "S2", "S3", "S4"],
        pop_ids=["PopA", "PopB"],
        sample_to_pop={"S1": "PopA", "S2": "PopA", "S3": "PopB", "S4": "PopB"},
        pop_to_indices={
            "PopA": np.array([0, 1], dtype=np.int32),
            "PopB": np.array([2, 3], dtype=np.int32),
        },
        n_samples_per_pop={"PopA": 2, "PopB": 2},
        sample_packed_index={"S1": 0, "S2": 1, "S3": 2, "S4": 3},
        n_vcf_samples=4,
        sample_to_sex={"S1": 1, "S2": 2, "S3": 0, "S4": 1},
    )


def _make_variant(*, qual=None, filter_status=None):
    return VariantRecord(
        id="chr22:100:A:T",
        chr="chr22",
        pos=100,
        ref="A",
        alt="T",
        variant_type="SNP",
        ac=[5, 3],
        an=[4, 4],
        af=[0.625, 0.375],
        het_count=[2, 1],
        hom_alt_count=[1, 1],
        het_exp=[0.46875, 0.46875],
        ac_total=8,
        an_total=8,
        af_total=0.5,
        call_rate=1.0,
        qual=qual,
        filter_status=filter_status,
        gt_packed=b"\x00",
        phase_packed=b"\x00",
    )


def _read_csv(path):
    with open(path) as f:
        reader = csv.reader(f)
        header = next(reader)
        rows = list(reader)
    return header, rows


SAMPLE_RAW_HEADER = (
    "##fileformat=VCFv4.3\n"
    "##source=GATK\n"
    "##reference=GRCh38\n"
    '##INFO=<ID=AC,Number=A,Type=Integer,Description="Allele count">\n'
    '##INFO=<ID=AN,Number=1,Type=Integer,Description="Total alleles">\n'
    '##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">\n'
    '##FILTER=<ID=LowQual,Description="Low quality">'
)


# ---------------------------------------------------------------------------
# TestQualFilterInVariantCSV
# ---------------------------------------------------------------------------


class TestQualFilterInVariantCSV:
    def test_qual_filter_columns_exist(self):
        assert "qual:float" in VARIANT_HEADER
        assert "filter" in VARIANT_HEADER

    def test_qual_float_stored(self, tmp_path, pop_map):
        v = _make_variant(qual=30.5)
        emitter = CSVEmitter(tmp_path, pop_map)
        emitter.write_static_nodes()
        emitter.process_chunk([v])
        emitter.finalize()

        _, rows = _read_csv(tmp_path / "variant_nodes.csv")
        qual_idx = VARIANT_HEADER.index("qual:float")
        assert rows[0][qual_idx] == "30.5"

    def test_qual_none_stored_as_empty(self, tmp_path, pop_map):
        v = _make_variant(qual=None)
        emitter = CSVEmitter(tmp_path, pop_map)
        emitter.write_static_nodes()
        emitter.process_chunk([v])
        emitter.finalize()

        _, rows = _read_csv(tmp_path / "variant_nodes.csv")
        qual_idx = VARIANT_HEADER.index("qual:float")
        assert rows[0][qual_idx] == ""

    def test_filter_pass_when_none(self, tmp_path, pop_map):
        v = _make_variant(filter_status=None)
        emitter = CSVEmitter(tmp_path, pop_map)
        emitter.write_static_nodes()
        emitter.process_chunk([v])
        emitter.finalize()

        _, rows = _read_csv(tmp_path / "variant_nodes.csv")
        filt_idx = VARIANT_HEADER.index("filter")
        assert rows[0][filt_idx] == "PASS"

    def test_filter_string_stored(self, tmp_path, pop_map):
        v = _make_variant(filter_status="LowQual")
        emitter = CSVEmitter(tmp_path, pop_map)
        emitter.write_static_nodes()
        emitter.process_chunk([v])
        emitter.finalize()

        _, rows = _read_csv(tmp_path / "variant_nodes.csv")
        filt_idx = VARIANT_HEADER.index("filter")
        assert rows[0][filt_idx] == "LowQual"


# ---------------------------------------------------------------------------
# TestVCFHeaderCSV
# ---------------------------------------------------------------------------


class TestVCFHeaderCSV:
    def test_creates_file(self, tmp_path, pop_map):
        emitter = CSVEmitter(tmp_path, pop_map, dataset_id="test_ds", source_file="test.vcf.gz")
        emitter.write_static_nodes()
        emitter.write_vcf_header_node(SAMPLE_RAW_HEADER)

        assert (tmp_path / "vcf_header_nodes.csv").exists()

    def test_correct_header_row(self, tmp_path, pop_map):
        emitter = CSVEmitter(tmp_path, pop_map, dataset_id="test_ds")
        emitter.write_static_nodes()
        emitter.write_vcf_header_node(SAMPLE_RAW_HEADER)

        header, _ = _read_csv(tmp_path / "vcf_header_nodes.csv")
        assert header == VCFHEADER_HEADER

    def test_dataset_id_and_source_file(self, tmp_path, pop_map):
        emitter = CSVEmitter(tmp_path, pop_map, dataset_id="ds1", source_file="my.vcf.gz")
        emitter.write_static_nodes()
        emitter.write_vcf_header_node(SAMPLE_RAW_HEADER)

        _, rows = _read_csv(tmp_path / "vcf_header_nodes.csv")
        assert len(rows) == 1
        row = rows[0]
        id_idx = VCFHEADER_HEADER.index("dataset_id:ID(VCFHeader)")
        src_idx = VCFHEADER_HEADER.index("source_file")
        assert row[id_idx] == "ds1"
        assert row[src_idx] == "my.vcf.gz"

    def test_parsed_fields(self, tmp_path, pop_map):
        emitter = CSVEmitter(tmp_path, pop_map, dataset_id="ds1")
        emitter.write_static_nodes()
        emitter.write_vcf_header_node(SAMPLE_RAW_HEADER)

        _, rows = _read_csv(tmp_path / "vcf_header_nodes.csv")
        row = rows[0]

        ff_idx = VCFHEADER_HEADER.index("file_format")
        ref_idx = VCFHEADER_HEADER.index("reference")
        caller_idx = VCFHEADER_HEADER.index("caller")
        info_idx = VCFHEADER_HEADER.index("info_fields:string[]")
        fmt_idx = VCFHEADER_HEADER.index("format_fields:string[]")
        filt_idx = VCFHEADER_HEADER.index("filter_fields:string[]")

        assert row[ff_idx] == "VCFv4.3"
        assert row[ref_idx] == "GRCh38"
        assert row[caller_idx] == "GATK"
        assert row[info_idx] == "AC;AN"
        assert row[fmt_idx] == "GT"
        assert row[filt_idx] == "LowQual"

    def test_newlines_escaped(self, tmp_path, pop_map):
        emitter = CSVEmitter(tmp_path, pop_map, dataset_id="ds1")
        emitter.write_static_nodes()
        emitter.write_vcf_header_node(SAMPLE_RAW_HEADER)

        _, rows = _read_csv(tmp_path / "vcf_header_nodes.csv")
        ht_idx = VCFHEADER_HEADER.index("header_text")
        header_text = rows[0][ht_idx]
        # Should contain escaped \n, not actual newlines
        assert "\n" not in header_text
        assert "\\n" in header_text

    def test_empty_header_no_file(self, tmp_path, pop_map):
        emitter = CSVEmitter(tmp_path, pop_map, dataset_id="ds1")
        emitter.write_static_nodes()
        emitter.write_vcf_header_node("")

        assert not (tmp_path / "vcf_header_nodes.csv").exists()


# ---------------------------------------------------------------------------
# TestVCFHeaderInLoader
# ---------------------------------------------------------------------------


class TestVCFHeaderInLoader:
    def test_vcf_header_included_when_present(self, tmp_path):
        from graphmana.ingest.loader import _build_import_command

        # Create required files + vcf_header_nodes.csv
        for name in [
            "variant_nodes.csv",
            "sample_nodes.csv",
            "population_nodes.csv",
            "chromosome_nodes.csv",
            "next_edges.csv",
            "on_chromosome_edges.csv",
            "in_population_edges.csv",
            "vcf_header_nodes.csv",
        ]:
            (tmp_path / name).write_text("header\n")

        neo4j_admin = tmp_path / "bin" / "neo4j-admin"
        neo4j_admin.parent.mkdir()
        neo4j_admin.touch()

        cmd = _build_import_command(neo4j_admin, tmp_path)
        cmd_str = " ".join(cmd)
        assert "VCFHeader=" in cmd_str
        assert "vcf_header_nodes.csv" in cmd_str

    def test_vcf_header_absent_no_error(self, tmp_path):
        from graphmana.ingest.loader import _build_import_command

        for name in [
            "variant_nodes.csv",
            "sample_nodes.csv",
            "population_nodes.csv",
            "chromosome_nodes.csv",
            "next_edges.csv",
            "on_chromosome_edges.csv",
            "in_population_edges.csv",
        ]:
            (tmp_path / name).write_text("header\n")

        neo4j_admin = tmp_path / "bin" / "neo4j-admin"
        neo4j_admin.parent.mkdir()
        neo4j_admin.touch()

        cmd = _build_import_command(neo4j_admin, tmp_path)
        cmd_str = " ".join(cmd)
        assert "VCFHeader=" not in cmd_str


# ---------------------------------------------------------------------------
# TestVCFExporterHeader
# ---------------------------------------------------------------------------


class TestVCFExporterHeader:
    def test_default_header_without_vcfheader(self):
        from io import StringIO

        from graphmana.export.vcf_export import VCFExporter

        f = StringIO()
        VCFExporter._write_chrom_line(f, ["S1", "S2"])
        line = f.getvalue()
        assert line.startswith("#CHROM")
        assert "S1" in line
        assert "S2" in line

    def test_preserved_header_uses_original_lines(self):
        from io import StringIO
        from unittest.mock import MagicMock

        from graphmana.export.vcf_export import VCFExporter

        # Simulate a VCFHeader dict as stored in Neo4j
        escaped = SAMPLE_RAW_HEADER.replace("\\", "\\\\").replace("\n", "\\n")
        vcf_header = {"header_text": escaped}

        # Create a minimal mock to test the instance method
        exporter = MagicMock(spec=VCFExporter)
        exporter._write_preserved_header = VCFExporter._write_preserved_header.__get__(exporter)
        exporter._write_chrom_line = VCFExporter._write_chrom_line

        f = StringIO()
        exporter._write_preserved_header(f, ["S1"], vcf_header)
        output = f.getvalue()

        assert "##fileformat=VCFv4.3" in output
        assert "##source=GATK" in output
        assert "##source=GraphMana-export" in output
        assert "#CHROM" in output
        assert "S1" in output

    def test_chrom_line_uses_export_samples(self):
        from io import StringIO
        from unittest.mock import MagicMock

        from graphmana.export.vcf_export import VCFExporter

        escaped = SAMPLE_RAW_HEADER.replace("\\", "\\\\").replace("\n", "\\n")
        vcf_header = {"header_text": escaped}

        exporter = MagicMock(spec=VCFExporter)
        exporter._write_preserved_header = VCFExporter._write_preserved_header.__get__(exporter)
        exporter._write_chrom_line = VCFExporter._write_chrom_line

        f = StringIO()
        exporter._write_preserved_header(f, ["ExportS1", "ExportS2"], vcf_header)
        output = f.getvalue()

        chrom_line = [ln for ln in output.split("\n") if ln.startswith("#CHROM")][0]
        assert "ExportS1" in chrom_line
        assert "ExportS2" in chrom_line


# ---------------------------------------------------------------------------
# TestQualFilterRoundtrip
# ---------------------------------------------------------------------------


class TestQualFilterRoundtrip:
    def test_qual_in_variant_line(self):
        from graphmana.export.vcf_export import format_variant_line

        props = {
            "chr": "chr22",
            "pos": 100,
            "variantId": "chr22:100:A:T",
            "ref": "A",
            "alt": "T",
            "qual": 30.0,
            "filter": "PASS",
            "ac_total": 8,
            "an_total": 8,
            "af_total": 0.5,
            "variant_type": "SNP",
        }
        line = format_variant_line(props, ["0/0"])
        fields = line.split("\t")
        assert fields[5] == "30.0"  # QUAL
        assert fields[6] == "PASS"  # FILTER

    def test_missing_qual_in_variant_line(self):
        from graphmana.export.vcf_export import format_variant_line

        props = {
            "chr": "chr22",
            "pos": 100,
            "variantId": "chr22:100:A:T",
            "ref": "A",
            "alt": "T",
            "qual": None,
            "filter": None,
            "ac_total": 8,
            "an_total": 8,
            "af_total": 0.5,
            "variant_type": "SNP",
        }
        line = format_variant_line(props, ["0/0"])
        fields = line.split("\t")
        assert fields[5] == "."  # QUAL missing
        assert fields[6] == "."  # FILTER missing
