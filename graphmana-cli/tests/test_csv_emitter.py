"""Tests for CSV emitter — node and relationship CSV generation."""

import csv

import numpy as np
import pytest

from graphmana.ingest.csv_emitter import (
    CHR_LENGTHS,
    IN_POPULATION_HEADER,
    NEXT_HEADER,
    ON_CHROMOSOME_HEADER,
    POPULATION_HEADER,
    SAMPLE_HEADER,
    VARIANT_HEADER,
    CSVEmitter,
    _fmt_float,
    _harmonic,
    _harmonic2,
)
from graphmana.ingest.population_map import PopulationMap
from graphmana.ingest.vcf_parser import VariantRecord

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def pop_map():
    """A minimal PopulationMap with 4 samples in 2 populations."""
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


def _make_variant(
    *,
    vid="chr22:100:A:T",
    chrom="chr22",
    pos=100,
    ref="A",
    alt="T",
    vtype="SNP",
    qual=None,
    filter_status=None,
    gt_packed=b"\x00",
    phase_packed=b"\x00",
    ploidy_packed=b"",
):
    return VariantRecord(
        id=vid,
        chr=chrom,
        pos=pos,
        ref=ref,
        alt=alt,
        variant_type=vtype,
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
        gt_packed=gt_packed,
        phase_packed=phase_packed,
        ploidy_packed=ploidy_packed,
    )


def _read_csv(path):
    """Read a CSV and return (header, rows)."""
    with open(path) as f:
        reader = csv.reader(f)
        header = next(reader)
        rows = list(reader)
    return header, rows


# ---------------------------------------------------------------------------
# Header format tests
# ---------------------------------------------------------------------------


class TestCSVHeaders:
    def test_variant_header_has_id_column(self):
        assert VARIANT_HEADER[0] == "variantId:ID(Variant)"

    def test_variant_header_has_label(self):
        assert ":LABEL" in VARIANT_HEADER

    def test_sample_header_has_graphmana_fields(self):
        """GraphMana adds source_dataset, source_file, ingestion_date."""
        assert "source_dataset" in SAMPLE_HEADER
        assert "source_file" in SAMPLE_HEADER
        assert "ingestion_date" in SAMPLE_HEADER

    def test_population_header_has_harmonic(self):
        assert "a_n:float" in POPULATION_HEADER
        assert "a_n2:float" in POPULATION_HEADER

    def test_next_header_has_type(self):
        assert ":TYPE" in NEXT_HEADER
        assert "distance_bp:long" in NEXT_HEADER

    def test_on_chromosome_header(self):
        assert ":START_ID(Variant)" in ON_CHROMOSOME_HEADER
        assert ":END_ID(Chromosome)" in ON_CHROMOSOME_HEADER

    def test_in_population_header(self):
        assert ":START_ID(Sample)" in IN_POPULATION_HEADER
        assert ":END_ID(Population)" in IN_POPULATION_HEADER


# ---------------------------------------------------------------------------
# Static node tests
# ---------------------------------------------------------------------------


class TestStaticNodes:
    def test_sample_nodes(self, tmp_path, pop_map):
        emitter = CSVEmitter(tmp_path, pop_map)
        emitter.write_static_nodes()

        header, rows = _read_csv(tmp_path / "sample_nodes.csv")
        assert header == SAMPLE_HEADER
        assert len(rows) == 4
        # Check S1
        s1 = rows[0]
        assert s1[0] == "S1"
        assert s1[1] == "Sample"
        assert s1[2] == "PopA"
        assert s1[3] == "0"  # packed_index
        assert s1[4] == "1"  # sex=male

    def test_population_nodes(self, tmp_path, pop_map):
        emitter = CSVEmitter(tmp_path, pop_map)
        emitter.write_static_nodes()

        header, rows = _read_csv(tmp_path / "population_nodes.csv")
        assert header == POPULATION_HEADER
        assert len(rows) == 2

        # PopA has 2 samples → 2n-1 = 3 → a_n = 1 + 1/2 + 1/3 = 11/6
        pop_a = rows[0]
        assert pop_a[0] == "PopA"
        assert pop_a[3] == "2"
        a_n = float(pop_a[4])
        assert abs(a_n - (1 + 0.5 + 1 / 3.0)) < 1e-6

    def test_in_population_edges(self, tmp_path, pop_map):
        emitter = CSVEmitter(tmp_path, pop_map)
        emitter.write_static_nodes()

        header, rows = _read_csv(tmp_path / "in_population_edges.csv")
        assert header == IN_POPULATION_HEADER
        assert len(rows) == 4
        assert rows[0] == ["S1", "PopA", "IN_POPULATION"]
        assert rows[2] == ["S3", "PopB", "IN_POPULATION"]


# ---------------------------------------------------------------------------
# Variant node tests
# ---------------------------------------------------------------------------


class TestVariantNodes:
    def test_single_variant(self, tmp_path, pop_map):
        emitter = CSVEmitter(tmp_path, pop_map)
        emitter.write_static_nodes()

        v = _make_variant()
        emitter.process_chunk([v])
        emitter.finalize()

        header, rows = _read_csv(tmp_path / "variant_nodes.csv")
        assert header == VARIANT_HEADER
        assert len(rows) == 1
        row = rows[0]
        assert row[0] == "chr22:100:A:T"
        assert row[1] == "Variant"
        assert row[2] == "chr22"
        assert row[3] == "100"

    def test_signed_java_bytes_boundary(self, tmp_path, pop_map):
        """Verify signed Java byte conversion: 0→0, 127→127, 128→-128, 255→-1."""
        gt = bytes([0, 127, 128, 255])
        v = _make_variant(gt_packed=gt)
        emitter = CSVEmitter(tmp_path, pop_map)
        emitter.write_static_nodes()
        emitter.process_chunk([v])
        emitter.finalize()

        _, rows = _read_csv(tmp_path / "variant_nodes.csv")
        row = rows[0]
        # gt_packed is the column after ploidy_packed; find the index
        gt_idx = VARIANT_HEADER.index("gt_packed:byte[]")
        gt_str = row[gt_idx]
        values = [int(x) for x in gt_str.split(";")]
        assert values == [0, 127, -128, -1]

    def test_population_arrays(self, tmp_path, pop_map):
        v = _make_variant()
        emitter = CSVEmitter(tmp_path, pop_map)
        emitter.write_static_nodes()
        emitter.process_chunk([v])
        emitter.finalize()

        _, rows = _read_csv(tmp_path / "variant_nodes.csv")
        row = rows[0]
        pop_ids_idx = VARIANT_HEADER.index("pop_ids:string[]")
        assert row[pop_ids_idx] == "PopA;PopB"

        ac_idx = VARIANT_HEADER.index("ac:int[]")
        assert row[ac_idx] == "5;3"

    def test_empty_packed_arrays(self, tmp_path, pop_map):
        v = _make_variant(gt_packed=b"", phase_packed=b"", ploidy_packed=b"")
        emitter = CSVEmitter(tmp_path, pop_map)
        emitter.write_static_nodes()
        emitter.process_chunk([v])
        emitter.finalize()

        _, rows = _read_csv(tmp_path / "variant_nodes.csv")
        gt_idx = VARIANT_HEADER.index("gt_packed:byte[]")
        assert rows[0][gt_idx] == ""


# ---------------------------------------------------------------------------
# NEXT chain tests
# ---------------------------------------------------------------------------


class TestNextChain:
    def test_consecutive_variants(self, tmp_path, pop_map):
        v1 = _make_variant(vid="chr22:100:A:T", pos=100)
        v2 = _make_variant(vid="chr22:200:G:C", pos=200)

        emitter = CSVEmitter(tmp_path, pop_map)
        emitter.write_static_nodes()
        emitter.process_chunk([v1, v2])
        emitter.finalize()

        _, rows = _read_csv(tmp_path / "next_edges.csv")
        assert len(rows) == 1
        assert rows[0][0] == "chr22:100:A:T"  # start
        assert rows[0][1] == "chr22:200:G:C"  # end
        assert rows[0][2] == "NEXT"
        assert rows[0][3] == "100"  # distance_bp

    def test_first_variant_no_next(self, tmp_path, pop_map):
        v = _make_variant(vid="chr22:100:A:T", pos=100)
        emitter = CSVEmitter(tmp_path, pop_map)
        emitter.write_static_nodes()
        emitter.process_chunk([v])
        emitter.finalize()

        _, rows = _read_csv(tmp_path / "next_edges.csv")
        assert len(rows) == 0

    def test_separate_chains_per_chromosome(self, tmp_path, pop_map):
        v1 = _make_variant(vid="chr21:100:A:T", chrom="chr21", pos=100)
        v2 = _make_variant(vid="chr22:50:G:C", chrom="chr22", pos=50)
        v3 = _make_variant(vid="chr21:200:C:G", chrom="chr21", pos=200)

        emitter = CSVEmitter(tmp_path, pop_map)
        emitter.write_static_nodes()
        emitter.process_chunk([v1, v2, v3])
        emitter.finalize()

        _, rows = _read_csv(tmp_path / "next_edges.csv")
        # Only chr21 has a NEXT edge (v1→v3)
        assert len(rows) == 1
        assert rows[0][0] == "chr21:100:A:T"
        assert rows[0][1] == "chr21:200:C:G"


# ---------------------------------------------------------------------------
# ON_CHROMOSOME tests
# ---------------------------------------------------------------------------


class TestOnChromosome:
    def test_edges_match_variants(self, tmp_path, pop_map):
        v1 = _make_variant(vid="chr22:100:A:T")
        v2 = _make_variant(vid="chr22:200:G:C", pos=200)

        emitter = CSVEmitter(tmp_path, pop_map)
        emitter.write_static_nodes()
        emitter.process_chunk([v1, v2])
        emitter.finalize()

        _, rows = _read_csv(tmp_path / "on_chromosome_edges.csv")
        assert len(rows) == 2
        assert rows[0] == ["chr22:100:A:T", "chr22", "ON_CHROMOSOME"]
        assert rows[1] == ["chr22:200:G:C", "chr22", "ON_CHROMOSOME"]


# ---------------------------------------------------------------------------
# Chromosome node tests
# ---------------------------------------------------------------------------


class TestChromosomeNodes:
    def test_from_chr_lengths(self, tmp_path, pop_map):
        v = _make_variant()
        emitter = CSVEmitter(tmp_path, pop_map)
        emitter.write_static_nodes()
        emitter.process_chunk([v])
        emitter.finalize()

        _, rows = _read_csv(tmp_path / "chromosome_nodes.csv")
        assert len(rows) == 1
        assert rows[0][0] == "chr22"
        assert rows[0][1] == "Chromosome"
        assert int(rows[0][2]) == CHR_LENGTHS["chr22"]

    def test_contig_lengths_override(self, tmp_path, pop_map):
        emitter = CSVEmitter(
            tmp_path,
            pop_map,
            contig_lengths={"chr22": 99999},
        )
        emitter.write_static_nodes()
        v = _make_variant()
        emitter.process_chunk([v])
        emitter.finalize()

        _, rows = _read_csv(tmp_path / "chromosome_nodes.csv")
        assert int(rows[0][2]) == 99999


# ---------------------------------------------------------------------------
# Finalize tests
# ---------------------------------------------------------------------------


class TestFinalize:
    def test_all_seven_files_created(self, tmp_path, pop_map):
        emitter = CSVEmitter(tmp_path, pop_map)
        emitter.write_static_nodes()
        v = _make_variant()
        emitter.process_chunk([v])
        emitter.finalize()

        expected_files = [
            "variant_nodes.csv",
            "sample_nodes.csv",
            "population_nodes.csv",
            "chromosome_nodes.csv",
            "next_edges.csv",
            "on_chromosome_edges.csv",
            "in_population_edges.csv",
        ]
        for name in expected_files:
            assert (tmp_path / name).exists(), f"{name} not found"

    def test_properties(self, tmp_path, pop_map):
        emitter = CSVEmitter(tmp_path, pop_map)
        emitter.write_static_nodes()
        v1 = _make_variant(vid="chr22:100:A:T", pos=100)
        v2 = _make_variant(vid="chr22:200:G:C", pos=200)
        emitter.process_chunk([v1, v2])
        emitter.finalize()

        assert emitter.n_variants == 2
        assert emitter.n_next == 1
        assert emitter.n_on_chrom == 2
        assert "chr22" in emitter.chromosomes_seen


# ---------------------------------------------------------------------------
# Helper function tests
# ---------------------------------------------------------------------------


class TestHelpers:
    def test_harmonic_1(self):
        assert _harmonic(1) == 1.0

    def test_harmonic_3(self):
        expected = 1.0 + 0.5 + 1 / 3.0
        assert abs(_harmonic(3) - expected) < 1e-10

    def test_harmonic2_1(self):
        assert _harmonic2(1) == 1.0

    def test_harmonic2_2(self):
        expected = 1.0 + 0.25
        assert abs(_harmonic2(2) - expected) < 1e-10

    def test_fmt_float(self):
        assert _fmt_float(0.0) == "0"
        assert _fmt_float(1.0) == "1"
        assert _fmt_float(0.123456789) == "0.12345679"
