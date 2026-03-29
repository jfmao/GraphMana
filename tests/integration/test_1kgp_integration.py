"""Integration tests with real 1000 Genomes Project data.

Tier 1 (chr22): ~1.07M variants, 2,504 samples — runs in ~20-30 min
Tier 2 (full genome): ~70M variants, 2,504 samples — runs in ~2-4 hours

Note: The 1KGP CCDG VCF has 3,202 samples but the panel file
(integrated_call_samples_v3.20130502.ALL.panel) only maps 2,504 of them
to populations. The remaining 698 are related/trio samples excluded from
population assignment by GraphMana's population map parser.

All chr22 test classes share a SINGLE session-scoped Neo4j instance
(prepare-csv + import runs once, not per class).

Prerequisites:
  - Neo4j installed at /usr/share/neo4j
  - Port 7687 free (stop any running Neo4j first)
  - 1KGP data at /mnt/data/GraphPop/data/raw/1000g/
  - graphmana CLI installed

Usage:
  pytest tests/integration/test_1kgp_integration.py -v -k "TestChr22" -s --timeout=3600
  pytest tests/integration/test_1kgp_integration.py -v -k "TestFullGenome" -s --timeout=36000
"""

import gzip
import random
import shutil
import subprocess
import tempfile
import time
from pathlib import Path

import pytest

from .conftest import (
    CHR22_VCF,
    FULL_GENOME_VCF_DIR,
    POPULATION_PANEL,
    Neo4jTestInstance,
    _check_prerequisites,
    _port_is_free,
    _prepare_csv,
    BOLT_PORT,
    DEFAULT_THREADS,
)

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

REQUIRED_CSV_FILES = [
    "variant_nodes.csv",
    "sample_nodes.csv",
    "population_nodes.csv",
    "chromosome_nodes.csv",
    "next_edges.csv",
    "on_chromosome_edges.csv",
    "in_population_edges.csv",
]


def _count_csv_data_rows(csv_path: Path) -> int:
    """Count data rows (excluding header) in a CSV file.

    Uses wc -l for large files (much faster than Python line iteration).
    For full genome CSVs (~215 GB), wc -l can take 10-15 minutes.
    """
    result = subprocess.run(
        ["wc", "-l", str(csv_path)],
        capture_output=True, text=True, timeout=1800,
    )
    total_lines = int(result.stdout.strip().split()[0])
    return total_lines - 1  # subtract header


def _run_export(fmt: str, output: str, extra_args: list[str] | None = None,
                timeout: int = 3600) -> subprocess.CompletedProcess:
    """Run graphmana export."""
    cmd = [
        "graphmana", "export",
        "--format", fmt,
        "--output", output,
        "--neo4j-uri", f"bolt://localhost:{BOLT_PORT}",
    ]
    if extra_args:
        cmd.extend(extra_args)
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


def _cypher_query(query: str) -> list:
    """Run a Cypher query against the test instance (no auth)."""
    from neo4j import GraphDatabase

    driver = GraphDatabase.driver(f"bolt://localhost:{BOLT_PORT}", auth=None)
    with driver.session() as session:
        result = session.run(query)
        records = [dict(r) for r in result]
    driver.close()
    return records


# ---------------------------------------------------------------------------
# Tier 1: chr22 integration tests (all share session-scoped Neo4j)
# ---------------------------------------------------------------------------


class TestChr22PrepareCSV:
    """Test prepare-csv output (uses session fixture, no Neo4j needed)."""

    def test_chr22_all_csv_files_created(self, chr22_csv_dir):
        for name in REQUIRED_CSV_FILES:
            assert (chr22_csv_dir / name).exists(), f"Missing {name}"

    def test_chr22_variant_count(self, chr22_csv_dir):
        n = _count_csv_data_rows(chr22_csv_dir / "variant_nodes.csv")
        assert 1_000_000 <= n <= 1_100_000, f"Unexpected variant count: {n}"
        print(f"  variant_nodes.csv: {n:,} variants")

    def test_chr22_sample_count(self, chr22_csv_dir):
        n = _count_csv_data_rows(chr22_csv_dir / "sample_nodes.csv")
        assert n == 2504, f"Expected 2504 samples, got {n}"

    def test_chr22_population_count(self, chr22_csv_dir):
        n = _count_csv_data_rows(chr22_csv_dir / "population_nodes.csv")
        assert n == 26, f"Expected 26 populations, got {n}"

    def test_chr22_chromosome_count(self, chr22_csv_dir):
        n = _count_csv_data_rows(chr22_csv_dir / "chromosome_nodes.csv")
        assert n == 1, f"Expected 1 chromosome, got {n}"

    def test_chr22_gt_packed_length(self, chr22_csv_dir):
        """gt_packed includes all VCF samples (3202), not just panel-mapped ones.
        ceil(3202/4) = 801 bytes (but only 2504 mapped to populations)."""
        with open(chr22_csv_dir / "variant_nodes.csv") as f:
            header = f.readline().strip().split(",")
            gt_idx = None
            for i, col in enumerate(header):
                if "gt_packed" in col:
                    gt_idx = i
                    break
            assert gt_idx is not None, "gt_packed column not found"
            line = f.readline().strip()
            fields = line.split(",")
            gt_field = fields[gt_idx]
            n_bytes = gt_field.count(";") + 1
            assert n_bytes == 801, f"Expected 801 bytes in gt_packed, got {n_bytes}"

    def test_chr22_variant_spot_check(self, chr22_csv_dir):
        with open(chr22_csv_dir / "variant_nodes.csv") as f:
            header = f.readline()
            lines = [f.readline() for _ in range(5)]
        for line in lines:
            fields = line.split(",")
            variant_id = fields[0]
            assert variant_id.startswith("chr22:"), f"Bad variantId: {variant_id}"
            parts = variant_id.split(":")
            assert len(parts) == 4, f"variantId should have 4 parts: {variant_id}"


class TestChr22LoadAndQuery:
    """Test Neo4j database content (uses session-scoped Neo4j instance)."""

    def test_chr22_variant_count_in_db(self, chr22_neo4j):
        result = _cypher_query("MATCH (v:Variant) RETURN count(v) AS cnt")
        n = result[0]["cnt"]
        assert 1_000_000 <= n <= 1_100_000, f"Unexpected variant count in DB: {n}"
        print(f"  Variants in DB: {n:,}")

    def test_chr22_sample_count_in_db(self, chr22_neo4j):
        result = _cypher_query("MATCH (s:Sample) RETURN count(s) AS cnt")
        assert result[0]["cnt"] == 2504

    def test_chr22_population_count_in_db(self, chr22_neo4j):
        result = _cypher_query("MATCH (p:Population) RETURN count(p) AS cnt")
        assert result[0]["cnt"] == 26

    def test_chr22_chromosome_count_in_db(self, chr22_neo4j):
        result = _cypher_query("MATCH (c:Chromosome) RETURN count(c) AS cnt")
        assert result[0]["cnt"] == 1

    def test_chr22_next_chain_exists(self, chr22_neo4j):
        result = _cypher_query("MATCH ()-[r:NEXT]->() RETURN count(r) AS cnt")
        assert result[0]["cnt"] > 900_000

    def test_chr22_on_chromosome_edges(self, chr22_neo4j):
        result = _cypher_query(
            "MATCH ()-[r:ON_CHROMOSOME]->() RETURN count(r) AS cnt"
        )
        assert result[0]["cnt"] > 1_000_000

    def test_chr22_in_population_edges(self, chr22_neo4j):
        result = _cypher_query(
            "MATCH ()-[r:IN_POPULATION]->() RETURN count(r) AS cnt"
        )
        assert result[0]["cnt"] == 2504

    def test_chr22_population_names(self, chr22_neo4j):
        result = _cypher_query(
            "MATCH (p:Population) RETURN p.populationId AS pid ORDER BY pid"
        )
        pops = [r["pid"] for r in result]
        assert len(pops) == 26
        for expected in ["YRI", "CEU", "CHB", "GBR", "JPT"]:
            assert expected in pops, f"{expected} not in populations: {pops}"

    def test_chr22_sample_has_packed_index(self, chr22_neo4j):
        result = _cypher_query(
            "MATCH (s:Sample) WHERE s.packed_index IS NOT NULL "
            "RETURN count(s) AS cnt"
        )
        assert result[0]["cnt"] == 2504

    def test_chr22_variant_has_gt_packed(self, chr22_neo4j):
        result = _cypher_query(
            "MATCH (v:Variant) WHERE v.gt_packed IS NOT NULL "
            "RETURN count(v) AS cnt"
        )
        assert result[0]["cnt"] > 1_000_000

    def test_chr22_variant_has_population_arrays(self, chr22_neo4j):
        result = _cypher_query(
            "MATCH (v:Variant) "
            "WHERE v.pop_ids IS NOT NULL AND v.ac IS NOT NULL "
            "RETURN count(v) AS cnt"
        )
        assert result[0]["cnt"] > 1_000_000


class TestChr22Exports:
    """Test exports from shared Neo4j instance."""

    # -- FAST PATH exports --

    def test_chr22_export_treemix(self, chr22_neo4j, chr22_export_dir):
        out = str(chr22_export_dir / "chr22.treemix.gz")
        t0 = time.time()
        result = _run_export("treemix", out)
        wall = time.time() - t0
        assert result.returncode == 0, f"TreeMix export failed:\n{result.stderr[-1000:]}"
        print(f"  TreeMix: {wall:.1f}s")

        with gzip.open(out, "rt") as f:
            header = f.readline().strip().split()
            assert len(header) == 26, f"Expected 26 pops, got {len(header)}"
            n_lines = sum(1 for _ in f)
        assert n_lines > 1_000_000
        print(f"  TreeMix: {n_lines:,} variants, 26 populations")

    def test_chr22_export_bed(self, chr22_neo4j, chr22_export_dir):
        out = str(chr22_export_dir / "chr22.bed")
        result = _run_export("bed", out)
        assert result.returncode == 0, f"BED export failed:\n{result.stderr[-1000:]}"

        with open(out) as f:
            lines = f.readlines()
        assert len(lines) > 1_000_000
        for line in lines[:100]:
            fields = line.strip().split("\t")
            assert fields[0] == "chr22"
            start, end = int(fields[1]), int(fields[2])
            assert start < end
            assert start >= 0

    def test_chr22_export_tsv(self, chr22_neo4j, chr22_export_dir):
        out = str(chr22_export_dir / "chr22.tsv")
        result = _run_export("tsv", out)
        assert result.returncode == 0, f"TSV export failed:\n{result.stderr[-1000:]}"

        with open(out) as f:
            header = f.readline()
            n_lines = sum(1 for _ in f)
        assert n_lines > 1_000_000
        assert "variantId" in header

    def test_chr22_export_sfs_dadi(self, chr22_neo4j, chr22_export_dir):
        out = str(chr22_export_dir / "chr22_sfs.fs")
        # Use 2 populations (3+ population SFS not yet implemented)
        result = _run_export("sfs-dadi", out, [
            "--sfs-populations", "YRI",
            "--sfs-populations", "CEU",
            "--sfs-projection", "20",
            "--sfs-projection", "20",
            "--sfs-folded",
        ])
        assert result.returncode == 0, f"SFS export failed:\n{result.stderr[-1000:]}"

        content = Path(out).read_text()
        assert len(content) > 100, "SFS file too small"

    # -- FULL PATH exports --

    def test_chr22_export_vcf(self, chr22_neo4j, chr22_export_dir):
        out = str(chr22_export_dir / "chr22.vcf.gz")
        t0 = time.time()
        result = _run_export("vcf", out, ["--output-type", "z"])
        wall = time.time() - t0
        assert result.returncode == 0, f"VCF export failed:\n{result.stderr[-1000:]}"
        print(f"  VCF: {wall:.1f}s")

        stats = subprocess.run(
            ["bcftools", "stats", out],
            capture_output=True, text=True
        )
        assert stats.returncode == 0, "bcftools stats failed on exported VCF"
        for line in stats.stdout.splitlines():
            if line.startswith("SN") and "number of samples" in line:
                n_samples = int(line.strip().split("\t")[-1])
                assert n_samples >= 2504, f"Expected >= 2504 samples, got {n_samples}"
            if line.startswith("SN") and "number of records" in line:
                n_records = int(line.strip().split("\t")[-1])
                assert n_records > 1_000_000, f"Only {n_records} records"
                print(f"  VCF: {n_records:,} records, {n_samples} samples")

    def test_chr22_export_plink(self, chr22_neo4j, chr22_export_dir):
        out = str(chr22_export_dir / "chr22_plink")
        t0 = time.time()
        result = _run_export("plink", out, ["--filter-variant-type", "SNP"])
        wall = time.time() - t0
        assert result.returncode == 0, f"PLINK export failed:\n{result.stderr[-1000:]}"
        print(f"  PLINK: {wall:.1f}s")

        bed_file = Path(out + ".bed")
        bim_file = Path(out + ".bim")
        fam_file = Path(out + ".fam")

        assert bed_file.exists(), ".bed file missing"
        assert bim_file.exists(), ".bim file missing"
        assert fam_file.exists(), ".fam file missing"

        with open(bed_file, "rb") as f:
            magic = f.read(3)
        assert magic == bytes([0x6C, 0x1B, 0x01]), f"Bad PLINK magic: {magic.hex()}"

        with open(fam_file) as f:
            n_fam = sum(1 for _ in f)
        assert n_fam >= 2504, f"Expected >= 2504 fam entries, got {n_fam}"

        with open(bim_file) as f:
            n_bim = sum(1 for _ in f)
        assert n_bim > 900_000, f"Only {n_bim} SNPs in bim"
        print(f"  PLINK: {n_bim:,} SNPs, {n_fam} samples")

    def test_chr22_export_eigenstrat(self, chr22_neo4j, chr22_export_dir):
        out = str(chr22_export_dir / "chr22_eigen")
        t0 = time.time()
        result = _run_export("eigenstrat", out)
        wall = time.time() - t0
        assert result.returncode == 0, f"EIGENSTRAT failed:\n{result.stderr[-1000:]}"
        print(f"  EIGENSTRAT: {wall:.1f}s")

        geno_file = Path(out + ".geno")
        snp_file = Path(out + ".snp")
        ind_file = Path(out + ".ind")

        assert geno_file.exists()
        assert snp_file.exists()
        assert ind_file.exists()

        with open(ind_file) as f:
            n_ind = sum(1 for _ in f)
        assert n_ind >= 2504, f"Expected >= 2504 ind entries, got {n_ind}"


class TestChr22VCFRoundtrip:
    """Validate VCF roundtrip: source VCF → GraphMana → exported VCF.

    Uses the shared session-scoped Neo4j instance.
    """

    @pytest.fixture(autouse=True, scope="class")
    def roundtrip_setup(self, chr22_neo4j, chr22_export_dir):
        cls = type(self)
        cls.export_dir = chr22_export_dir

        # Export uncompressed VCF for easy querying
        cls.exported_vcf = str(chr22_export_dir / "roundtrip.vcf")
        r = _run_export("vcf", cls.exported_vcf, ["--output-type", "v"])
        if r.returncode != 0:
            pytest.fail(f"VCF export for roundtrip failed:\n{r.stderr[-1000:]}")
        yield

    def test_chr22_variant_identity_roundtrip(self):
        """Verify variant identity (CHROM/POS/REF/ALT) matches source VCF."""
        exp = subprocess.run(
            ["bcftools", "query", "-f", "%CHROM\t%POS\t%REF\t%ALT\n",
             self.exported_vcf],
            capture_output=True, text=True, timeout=300
        )
        exp_lines = [l for l in exp.stdout.strip().split("\n") if l]
        assert len(exp_lines) > 1_000_000, f"Only {len(exp_lines)} exported variants"

        random.seed(42)
        sample_lines = random.sample(exp_lines, 10)
        sample_positions = [line.split("\t")[1] for line in sample_lines]

        mismatches = []
        for pos in sample_positions:
            src = subprocess.run(
                ["bcftools", "query", "-f", "%CHROM\t%POS\t%REF\t%ALT\n",
                 "-r", f"chr22:{pos}-{pos}", str(CHR22_VCF)],
                capture_output=True, text=True, timeout=60
            )
            src_ids = set(src.stdout.strip().split("\n"))
            exp_at_pos = {l for l in exp_lines if l.split("\t")[1] == pos}

            if not exp_at_pos.issubset(src_ids):
                mismatches.append(
                    f"pos={pos}: exported={exp_at_pos} not in source={src_ids}"
                )

        assert len(mismatches) == 0, (
            f"Variant identity roundtrip failures:\n" + "\n".join(mismatches)
        )
        print(f"  Roundtrip: {len(sample_positions)} positions verified OK")

    def test_chr22_exported_vcf_sample_count(self):
        result = subprocess.run(
            ["bcftools", "query", "-l", self.exported_vcf],
            capture_output=True, text=True, timeout=60
        )
        n_samples = len(result.stdout.strip().split("\n"))
        assert n_samples >= 2504, f"Expected >= 2504 samples, got {n_samples}"

    def test_chr22_exported_vcf_valid(self):
        result = subprocess.run(
            ["bcftools", "stats", self.exported_vcf],
            capture_output=True, text=True, timeout=300
        )
        assert result.returncode == 0, "bcftools stats failed"
        for line in result.stdout.splitlines():
            if line.startswith("SN") and "number of records" in line:
                n = int(line.strip().split("\t")[-1])
                assert n > 1_000_000, f"Only {n} records"
                print(f"  Exported VCF: {n:,} records")


# ---------------------------------------------------------------------------
# Tier 2: Full genome integration tests
# ---------------------------------------------------------------------------


def _get_full_genome_vcfs() -> list[Path]:
    """Get all 22 autosome VCFs in chromosome order."""
    vcfs = []
    for i in range(1, 23):
        pattern = f"*chr{i}.filtered.*vcf.gz"
        matches = list(FULL_GENOME_VCF_DIR.glob(pattern))
        if matches:
            vcfs.append(matches[0])
    return vcfs


class TestFullGenome:
    """Full genome integration test: prepare-csv + load + export.

    Single class with one fixture that runs prepare-csv (8 threads, multi-file
    parallel), imports into Neo4j, then validates CSV content and exports.
    """

    @pytest.fixture(autouse=True, scope="class")
    def full_genome_setup(self, tmp_path_factory):
        _check_prerequisites(FULL_GENOME_VCF_DIR / "1kGP_high_coverage_Illumina.chr1.filtered.SNV_INDEL_SV_phased_panel.vcf.gz")
        vcfs = _get_full_genome_vcfs()
        if len(vcfs) < 22:
            pytest.skip(f"Only {len(vcfs)}/22 chromosome VCFs found")

        free_gb = shutil.disk_usage("/mnt/data").free / (1024 ** 3)
        if free_gb < 300:
            pytest.skip(f"Insufficient disk: {free_gb:.0f} GB free, need 300+ GB")

        cls = type(self)

        # Step 1: prepare-csv (multi-file parallel, 8 workers)
        cls.csv_dir = Path(tempfile.mkdtemp(
            prefix="graphmana_wgs_csv_", dir="/mnt/data/GraphMana/results"
        ))
        t0 = time.time()
        result = _prepare_csv(vcfs, POPULATION_PANEL, cls.csv_dir, threads=8)
        cls.prepare_time = time.time() - t0
        if result.returncode != 0:
            shutil.rmtree(cls.csv_dir, ignore_errors=True)
            pytest.fail(
                f"prepare-csv full genome failed (exit {result.returncode}):\n"
                f"STDERR: {result.stderr[-3000:]}"
            )
        print(f"\n  [full genome] prepare-csv: {cls.prepare_time:.0f}s "
              f"({cls.prepare_time/60:.1f} min)")

        # Step 2: Neo4j import + start
        neo4j_base = Path(tempfile.mkdtemp(
            prefix="graphmana_wgs_neo4j_", dir="/mnt/data/GraphMana/results"
        ))
        cls.neo4j_base = neo4j_base
        cls.instance = Neo4jTestInstance(
            neo4j_base, heap="4g", pagecache="20g"
        )
        cls.export_dir = neo4j_base / "exports"
        cls.export_dir.mkdir(exist_ok=True)

        try:
            t0 = time.time()
            cls.instance.import_csv(cls.csv_dir)
            cls.import_time = time.time() - t0
            print(f"  [full genome] neo4j-admin import: {cls.import_time:.0f}s")

            cls.instance.start(timeout=300)
            print(f"  [full genome] Neo4j started")
            yield
        finally:
            cls.instance.stop()
            shutil.rmtree(cls.csv_dir, ignore_errors=True)
            shutil.rmtree(neo4j_base, ignore_errors=True)

    # -- CSV validation tests --

    def test_full_genome_all_csv_files(self):
        for name in REQUIRED_CSV_FILES:
            assert (self.csv_dir / name).exists(), f"Missing {name}"

    def test_full_genome_variant_count(self):
        # Use file size as proxy — wc -l on 215 GB takes 30+ min and
        # competes with Neo4j for disk I/O. Each variant row is ~3 KB,
        # so 70M variants ≈ 200-220 GB.
        size_gb = self.csv_dir.joinpath("variant_nodes.csv").stat().st_size / (1024**3)
        assert size_gb > 180, f"variant CSV too small: {size_gb:.0f} GB"
        assert size_gb < 250, f"variant CSV too large: {size_gb:.0f} GB"
        print(f"  Full genome variant CSV: {size_gb:.1f} GB")

    def test_full_genome_sample_count(self):
        n = _count_csv_data_rows(self.csv_dir / "sample_nodes.csv")
        assert n == 2504

    def test_full_genome_chromosome_count(self):
        n = _count_csv_data_rows(self.csv_dir / "chromosome_nodes.csv")
        assert n == 22

    # -- Neo4j database validation --

    def test_full_genome_variant_count_db(self):
        result = _cypher_query("MATCH (v:Variant) RETURN count(v) AS cnt")
        n = result[0]["cnt"]
        assert n > 60_000_000, f"Too few variants: {n:,}"
        print(f"  Full genome variants in DB: {n:,}")

    def test_full_genome_chromosome_count_db(self):
        result = _cypher_query("MATCH (c:Chromosome) RETURN count(c) AS cnt")
        assert result[0]["cnt"] == 22

    # -- Export tests --

    def test_full_genome_treemix_2chrom(self):
        """TreeMix FAST PATH export of 2 chromosomes from full genome DB."""
        out = str(self.export_dir / "wgs_2chr.treemix.gz")
        t0 = time.time()
        result = _run_export("treemix", out, [
            "--chromosomes", "chr21",
            "--chromosomes", "chr22",
        ], timeout=7200)
        wall = time.time() - t0
        assert result.returncode == 0, f"TreeMix failed:\n{result.stderr[-1000:]}"
        print(f"  TreeMix (chr21+chr22): {wall:.1f}s")

        with gzip.open(out, "rt") as f:
            header = f.readline().strip().split()
            assert len(header) == 26, f"Expected 26 pops, got {len(header)}"
            first_line = f.readline().strip()
            fields = first_line.split()
            assert len(fields) == 26
            assert "," in fields[0], f"Bad TreeMix format: {fields[0]}"

        file_size_mb = Path(out).stat().st_size / (1024 * 1024)
        assert file_size_mb > 1, f"TreeMix file too small: {file_size_mb:.1f} MB"
        print(f"  TreeMix: {file_size_mb:.1f} MB, 26 populations")

    def test_full_genome_bed_2chrom(self):
        """BED FAST PATH export of 2 chromosomes from full genome DB."""
        out = str(self.export_dir / "wgs_2chr.bed")
        t0 = time.time()
        result = _run_export("bed", out, [
            "--chromosomes", "chr21",
            "--chromosomes", "chr22",
        ], timeout=7200)
        wall = time.time() - t0
        assert result.returncode == 0, f"BED failed:\n{result.stderr[-1000:]}"
        print(f"  BED (chr21+chr22): {wall:.1f}s")

        # Verify both chromosomes present
        chroms = set()
        with open(out) as f:
            for line in f:
                chroms.add(line.split("\t")[0])
        assert "chr21" in chroms, "chr21 missing from BED"
        assert "chr22" in chroms, "chr22 missing from BED"
        print(f"  BED chromosomes: {sorted(chroms)}")

    def test_full_genome_vcf_single_chrom(self):
        out = str(self.export_dir / "wgs_chr22_export.vcf.gz")
        t0 = time.time()
        result = _run_export("vcf", out, [
            "--output-type", "z",
            "--chromosomes", "chr22",
        ], timeout=7200)
        wall = time.time() - t0
        assert result.returncode == 0, f"VCF chr22 failed:\n{result.stderr[-1000:]}"
        print(f"  Full genome VCF chr22: {wall:.1f}s")

        stats = subprocess.run(
            ["bcftools", "stats", out],
            capture_output=True, text=True
        )
        for line in stats.stdout.splitlines():
            if line.startswith("SN") and "number of records" in line:
                n = int(line.strip().split("\t")[-1])
                assert n > 1_000_000, f"Only {n} records for chr22"
                print(f"  VCF chr22 from full DB: {n:,} records")

    def test_full_genome_timing_report(self):
        print(f"\n  === Full Genome Timing ===")
        print(f"  prepare-csv (8 threads): {self.prepare_time:.0f}s "
              f"({self.prepare_time/60:.1f} min)")
        print(f"  neo4j-admin import:      {self.import_time:.0f}s "
              f"({self.import_time/60:.1f} min)")
