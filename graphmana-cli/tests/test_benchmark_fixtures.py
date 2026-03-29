"""Unit tests for benchmarks/generate_fixtures.py.

Tests the synthetic data generator without requiring Neo4j.
"""

from __future__ import annotations

import gzip
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "benchmarks"))

from generate_fixtures import generate_population_map, generate_vcf


class TestGenerateVCF:
    def test_produces_correct_sample_count(self, tmp_path):
        vcf_path = tmp_path / "test.vcf.gz"
        sample_ids = generate_vcf(vcf_path, n_samples=50, n_variants=100)
        assert len(sample_ids) == 50
        assert sample_ids[0] == "SAMPLE_00000"
        assert sample_ids[49] == "SAMPLE_00049"

    def test_produces_correct_variant_count(self, tmp_path):
        vcf_path = tmp_path / "test.vcf.gz"
        generate_vcf(vcf_path, n_samples=10, n_variants=200)

        n_data_lines = 0
        with gzip.open(vcf_path, "rt") as f:
            for line in f:
                if not line.startswith("#"):
                    n_data_lines += 1
        assert n_data_lines == 200

    def test_header_format(self, tmp_path):
        vcf_path = tmp_path / "test.vcf.gz"
        generate_vcf(vcf_path, n_samples=5, n_variants=10)

        with gzip.open(vcf_path, "rt") as f:
            first_line = f.readline()
            assert first_line.startswith("##fileformat=VCF")

            # Find header line
            for line in f:
                if line.startswith("#CHROM"):
                    fields = line.strip().split("\t")
                    # 9 fixed columns + 5 samples
                    assert len(fields) == 14
                    assert fields[0] == "#CHROM"
                    assert fields[8] == "FORMAT"
                    assert fields[9] == "SAMPLE_00000"
                    break

    def test_data_line_format(self, tmp_path):
        vcf_path = tmp_path / "test.vcf.gz"
        generate_vcf(vcf_path, n_samples=10, n_variants=5, chrom="22")

        with gzip.open(vcf_path, "rt") as f:
            for line in f:
                if not line.startswith("#"):
                    fields = line.strip().split("\t")
                    assert fields[0] == "22"  # CHROM
                    assert int(fields[1]) > 0  # POS
                    assert fields[5].isdigit()  # QUAL
                    assert fields[6] == "PASS"  # FILTER
                    assert "AC=" in fields[7]  # INFO
                    assert fields[8] == "GT"  # FORMAT
                    # Genotypes
                    for gt in fields[9:]:
                        assert gt in ("0/0", "0/1", "1/1", "./.")
                    break

    def test_deterministic_with_seed(self, tmp_path):
        path1 = tmp_path / "a.vcf.gz"
        path2 = tmp_path / "b.vcf.gz"
        generate_vcf(path1, n_samples=10, n_variants=50, seed=123)
        generate_vcf(path2, n_samples=10, n_variants=50, seed=123)

        with gzip.open(path1, "rb") as f1, gzip.open(path2, "rb") as f2:
            assert f1.read() == f2.read()

    def test_plain_vcf(self, tmp_path):
        vcf_path = tmp_path / "test.vcf"  # No .gz
        sample_ids = generate_vcf(vcf_path, n_samples=5, n_variants=10)
        assert len(sample_ids) == 5
        content = vcf_path.read_text()
        assert content.startswith("##fileformat=VCF")


class TestGeneratePopulationMap:
    def test_all_samples_assigned(self, tmp_path):
        pop_path = tmp_path / "popmap.tsv"
        sample_ids = [f"S{i}" for i in range(100)]
        generate_population_map(pop_path, sample_ids, n_populations=5)

        lines = pop_path.read_text().strip().split("\n")
        assert lines[0] == "sample\tpopulation"
        data_lines = lines[1:]
        assert len(data_lines) == 100

        assigned_samples = {line.split("\t")[0] for line in data_lines}
        assert assigned_samples == set(sample_ids)

    def test_populations_balanced(self, tmp_path):
        pop_path = tmp_path / "popmap.tsv"
        sample_ids = [f"S{i}" for i in range(100)]
        generate_population_map(pop_path, sample_ids, n_populations=5)

        lines = pop_path.read_text().strip().split("\n")[1:]
        from collections import Counter

        pop_counts = Counter(line.split("\t")[1] for line in lines)
        assert len(pop_counts) == 5
        # Each pop should have 20 samples (100/5)
        for count in pop_counts.values():
            assert count == 20

    def test_deterministic_with_seed(self, tmp_path):
        path1 = tmp_path / "a.tsv"
        path2 = tmp_path / "b.tsv"
        sample_ids = [f"S{i}" for i in range(50)]
        generate_population_map(path1, sample_ids, seed=42)
        generate_population_map(path2, sample_ids, seed=42)
        assert path1.read_text() == path2.read_text()
