"""Integration test and benchmark with real 1000 Genomes Project data.

Runs a complete GraphMana pipeline against 1kGP 30x high-coverage data
(3,202 samples, GRCh38) and records wall time, CPU time, peak RSS for
every operation. Two scales:

  - chr22:  ~1.07M variants, fast (~15-30 min total)
  - genome: all 22 autosomes (~85M variants), hours

Each run produces a JSONL results file plus a Markdown summary table
suitable for publication figures.

Prerequisites:
  - Running Neo4j instance with known credentials
  - 1kGP data symlinked at data/1000g/ (VCFs + population map)
  - graphmana CLI installed

Usage::

    # chr22 only (recommended first run)
    python benchmarks/bench_integration_1kgp.py --scale chr22

    # Full genome (hours, needs ~350 GB disk)
    python benchmarks/bench_integration_1kgp.py --scale genome

    # Custom threads and label
    python benchmarks/bench_integration_1kgp.py --scale chr22 --threads 8 --label v1_chr22

    # Include annotation loading (gnomAD VEP on chr22)
    python benchmarks/bench_integration_1kgp.py --scale chr22 --annotate

    # Skip ingest (database already loaded from previous run)
    python benchmarks/bench_integration_1kgp.py --scale chr22 --skip-ingest
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from conftest import (
    NEO4J_PASSWORD,
    NEO4J_URI,
    NEO4J_USER,
    RESULTS_DIR,
)
from measurement import (
    SubprocessMetrics,
    format_table,
    load_results,
    measure_subprocess,
    write_result,
)

# ---------------------------------------------------------------------------
# Data paths (relative to project root)
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "1000g"
VCF_DIR = DATA_DIR / "vcf"
POPMAP = DATA_DIR / "population_map.ped"
ANNOTATION_VCF = DATA_DIR / "annotation" / "gnomad.joint.v4.1.sites.chr22_VEP.vcf.gz"

CHR22_VCF = (
    VCF_DIR
    / "1kGP_high_coverage_Illumina.chr22.filtered.SNV_INDEL_SV_phased_panel.vcf.gz"
)

# All 22 autosome VCFs
ALL_VCFS = sorted(
    VCF_DIR.glob(
        "1kGP_high_coverage_Illumina.chr*.filtered.SNV_INDEL_SV_phased_panel.vcf.gz"
    )
)

# Export formats grouped by access path
FAST_FORMATS = ["treemix", "sfs-dadi", "sfs-fsc", "bed", "json", "tsv"]
FULL_FORMATS = [
    "vcf",
    "plink",
    "plink2",
    "eigenstrat",
    "beagle",
    "structure",
    "genepop",
    "hap",
]

# File extensions for output naming
FORMAT_EXT: dict[str, str] = {
    "vcf": ".vcf.gz",
    "plink": ".bed",
    "plink2": ".pgen",
    "eigenstrat": ".geno",
    "treemix": ".treemix.gz",
    "sfs-dadi": ".sfs",
    "sfs-fsc": "_jointMAFpop1_0.obs",
    "bed": ".bed.out",
    "tsv": ".tsv",
    "beagle": ".beagle.gz",
    "structure": ".str",
    "genepop": ".gen",
    "hap": ".hap",
    "json": ".jsonl",
    "zarr": ".zarr",
    "gds": ".gds",
    "bgen": ".bgen",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _neo4j_args() -> list[str]:
    return [
        "--neo4j-uri",
        NEO4J_URI,
        "--neo4j-user",
        NEO4J_USER,
        "--neo4j-password",
        NEO4J_PASSWORD,
    ]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _metrics_dict(m: SubprocessMetrics) -> dict:
    return {
        "elapsed_s": round(m.wall_s, 3),
        "user_s": round(m.user_s, 3),
        "sys_s": round(m.sys_s, 3),
        "cpu_s": round(m.cpu_s, 3),
        "cpu_pct": m.cpu_pct,
        "peak_rss_mb": round(m.peak_rss_mb, 1),
    }


def _file_size_mb(path: Path) -> float:
    if path.is_dir():
        return sum(f.stat().st_size for f in path.rglob("*") if f.is_file()) / (
            1024 * 1024
        )
    if path.exists():
        return path.stat().st_size / (1024 * 1024)
    return 0.0


def _dir_size_mb(path: Path) -> float:
    if not path.exists():
        return 0.0
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file()) / (1024 * 1024)


def _count_variants_in_db() -> int:
    """Query variant count from Neo4j via graphmana status --json."""
    cmd = ["graphmana", "status", "--json", *_neo4j_args()]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if proc.returncode == 0:
            import json

            data = json.loads(proc.stdout)
            return data.get("n_variants", 0)
    except Exception:
        pass
    return 0


# ---------------------------------------------------------------------------
# Phase 1: Ingest
# ---------------------------------------------------------------------------


def bench_prepare_csv(
    vcf_files: list[Path],
    threads: int,
    label: str,
    scale: str,
    results_path: Path,
) -> Path:
    """Run prepare-csv and return the CSV output directory."""
    csv_dir = Path(tempfile.mkdtemp(prefix="graphmana_1kgp_csv_"))

    input_args: list[str] = []
    for v in vcf_files:
        input_args.extend(["--input", str(v)])

    cmd = [
        "graphmana",
        "prepare-csv",
        *input_args,
        "--population-map",
        str(POPMAP),
        "--output-dir",
        str(csv_dir),
        "--threads",
        str(threads),
    ]

    print(
        f"\n[prepare-csv] scale={scale} threads={threads} files={len(vcf_files)}",
        flush=True,
    )
    stdout, m = measure_subprocess(cmd, timeout=36000)  # 10h for genome

    csv_size_mb = _dir_size_mb(csv_dir)

    record = {
        "label": label,
        "scale": scale,
        "operation": "prepare_csv",
        "n_vcf_files": len(vcf_files),
        "threads": threads,
        "csv_size_mb": round(csv_size_mb, 1),
        **_metrics_dict(m),
        "timestamp": _now(),
    }
    write_result(results_path, record)
    print(
        f"  done: {m.wall_s:.1f}s wall, {m.peak_rss_mb:.0f} MB RSS, {csv_size_mb:.0f} MB CSV"
    )

    return csv_dir


def bench_load_csv(
    csv_dir: Path,
    label: str,
    scale: str,
    results_path: Path,
) -> None:
    """Run load-csv to import pre-generated CSVs into Neo4j."""
    cmd = [
        "graphmana",
        "load-csv",
        "--csv-dir",
        str(csv_dir),
        *_neo4j_args(),
    ]

    print(f"\n[load-csv] scale={scale}", flush=True)
    stdout, m = measure_subprocess(cmd, timeout=36000)

    record = {
        "label": label,
        "scale": scale,
        "operation": "load_csv",
        **_metrics_dict(m),
        "timestamp": _now(),
    }
    write_result(results_path, record)
    print(f"  done: {m.wall_s:.1f}s wall, {m.peak_rss_mb:.0f} MB RSS")


def bench_ingest_single_step(
    vcf_files: list[Path],
    threads: int,
    label: str,
    scale: str,
    results_path: Path,
) -> None:
    """Run single-step ingest (VCF -> Neo4j) for comparison."""
    input_args: list[str] = []
    for v in vcf_files:
        input_args.extend(["--input", str(v)])

    cmd = [
        "graphmana",
        "ingest",
        *input_args,
        "--population-map",
        str(POPMAP),
        "--threads",
        str(threads),
        *_neo4j_args(),
    ]

    print(f"\n[ingest] scale={scale} threads={threads}", flush=True)
    stdout, m = measure_subprocess(cmd, timeout=36000)

    record = {
        "label": label,
        "scale": scale,
        "operation": "ingest_single_step",
        "n_vcf_files": len(vcf_files),
        "threads": threads,
        **_metrics_dict(m),
        "timestamp": _now(),
    }
    write_result(results_path, record)
    print(f"  done: {m.wall_s:.1f}s wall, {m.peak_rss_mb:.0f} MB RSS")


# ---------------------------------------------------------------------------
# Phase 2: Verification
# ---------------------------------------------------------------------------


def bench_status(label: str, scale: str, results_path: Path) -> dict:
    """Run graphmana status --json and record timing."""
    cmd = ["graphmana", "status", "--json", *_neo4j_args()]

    print(f"\n[status] scale={scale}", flush=True)
    stdout, m = measure_subprocess(cmd, timeout=60)

    import json

    status_data = json.loads(stdout) if stdout.strip() else {}

    record = {
        "label": label,
        "scale": scale,
        "operation": "status",
        "n_variants": status_data.get("n_variants", 0),
        "n_samples": status_data.get("n_samples", 0),
        "n_populations": status_data.get("n_populations", 0),
        "n_chromosomes": status_data.get("n_chromosomes", 0),
        **_metrics_dict(m),
        "timestamp": _now(),
    }
    write_result(results_path, record)
    print(
        f"  {status_data.get('n_variants', '?')} variants, "
        f"{status_data.get('n_samples', '?')} samples, "
        f"{status_data.get('n_populations', '?')} populations"
    )
    return status_data


# ---------------------------------------------------------------------------
# Phase 3: Export (all formats)
# ---------------------------------------------------------------------------


def bench_export(
    fmt: str,
    threads: int,
    label: str,
    scale: str,
    results_path: Path,
    *,
    extra_args: list[str] | None = None,
) -> None:
    """Benchmark a single export format."""
    path_type = "fast" if fmt in FAST_FORMATS else "full"

    with tempfile.TemporaryDirectory(prefix="graphmana_1kgp_export_") as tmpdir:
        ext = FORMAT_EXT.get(fmt, ".out")
        output_file = Path(tmpdir) / f"export_1kgp{ext}"

        cmd = [
            "graphmana",
            "export",
            "--format",
            fmt,
            "--output",
            str(output_file),
            "--threads",
            str(threads),
            *_neo4j_args(),
        ]
        if extra_args:
            cmd.extend(extra_args)

        print(f"  export {fmt} ({path_type}) t={threads}", flush=True)

        try:
            stdout, m = measure_subprocess(cmd, timeout=36000)
        except RuntimeError as e:
            print(f"    FAILED: {e}")
            record = {
                "label": label,
                "scale": scale,
                "operation": f"export_{fmt}",
                "format": fmt,
                "path_type": path_type,
                "threads": threads,
                "status": "failed",
                "error": str(e)[:200],
                "timestamp": _now(),
            }
            write_result(results_path, record)
            return

        # Measure output size (some formats produce multiple files)
        output_size_mb = _dir_size_mb(Path(tmpdir))

        record = {
            "label": label,
            "scale": scale,
            "operation": f"export_{fmt}",
            "format": fmt,
            "path_type": path_type,
            "threads": threads,
            "status": "ok",
            "output_size_mb": round(output_size_mb, 2),
            **_metrics_dict(m),
            "timestamp": _now(),
        }
        write_result(results_path, record)
        print(
            f"    {m.wall_s:.1f}s wall, {m.peak_rss_mb:.0f} MB RSS, {output_size_mb:.1f} MB output"
        )


def bench_all_exports(
    threads: int,
    label: str,
    scale: str,
    results_path: Path,
    *,
    skip_full: bool = False,
) -> None:
    """Benchmark all export formats."""
    print("\n--- FAST PATH exports ---")
    for fmt in FAST_FORMATS:
        bench_export(fmt, threads, label, scale, results_path)

    if skip_full:
        print("\n--- FULL PATH exports: SKIPPED (--skip-full) ---")
        return

    print("\n--- FULL PATH exports ---")
    for fmt in FULL_FORMATS:
        bench_export(fmt, threads, label, scale, results_path)


# ---------------------------------------------------------------------------
# Phase 4: Annotation (optional, chr22 only)
# ---------------------------------------------------------------------------


def bench_annotate(
    label: str,
    scale: str,
    results_path: Path,
) -> None:
    """Load gnomAD VEP annotations for chr22 and measure timing."""
    if not ANNOTATION_VCF.exists():
        print(f"\n[annotate] SKIPPED: {ANNOTATION_VCF} not found")
        return

    cmd = [
        "graphmana",
        "annotate",
        "--type",
        "vep",
        "--input",
        str(ANNOTATION_VCF),
        "--version",
        "gnomad_v4.1_vep115",
        "--mode",
        "add",
        *_neo4j_args(),
    ]

    print("\n[annotate] gnomAD VEP chr22", flush=True)
    try:
        stdout, m = measure_subprocess(cmd, timeout=36000)
    except RuntimeError as e:
        print(f"  FAILED: {e}")
        record = {
            "label": label,
            "scale": scale,
            "operation": "annotate_vep",
            "status": "failed",
            "error": str(e)[:200],
            "timestamp": _now(),
        }
        write_result(results_path, record)
        return

    record = {
        "label": label,
        "scale": scale,
        "operation": "annotate_vep",
        "annotation_source": "gnomad_v4.1_vep115",
        "status": "ok",
        **_metrics_dict(m),
        "timestamp": _now(),
    }
    write_result(results_path, record)
    print(f"  done: {m.wall_s:.1f}s wall, {m.peak_rss_mb:.0f} MB RSS")


def bench_filtered_export(
    threads: int,
    label: str,
    scale: str,
    results_path: Path,
) -> None:
    """Export VCF filtered by annotation (high-impact variants only)."""
    with tempfile.TemporaryDirectory(prefix="graphmana_1kgp_filtered_") as tmpdir:
        output_file = Path(tmpdir) / "filtered_high_impact.vcf.gz"

        cmd = [
            "graphmana",
            "export",
            "--format",
            "vcf",
            "--output",
            str(output_file),
            "--filter-impact",
            "HIGH",
            "--threads",
            str(threads),
            *_neo4j_args(),
        ]

        print("\n[filtered export] VCF --filter-impact HIGH", flush=True)
        try:
            stdout, m = measure_subprocess(cmd, timeout=3600)
        except RuntimeError as e:
            print(f"  FAILED: {e}")
            return

        output_size_mb = _file_size_mb(output_file)

        record = {
            "label": label,
            "scale": scale,
            "operation": "export_vcf_filtered_high_impact",
            "threads": threads,
            "status": "ok",
            "output_size_mb": round(output_size_mb, 2),
            **_metrics_dict(m),
            "timestamp": _now(),
        }
        write_result(results_path, record)
        print(f"  done: {m.wall_s:.1f}s wall, {output_size_mb:.1f} MB output")


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------


def print_summary(results_path: Path) -> None:
    """Print a Markdown summary table from JSONL results."""
    if not results_path.exists():
        return

    results = load_results(results_path)
    if not results:
        return

    print("\n" + "=" * 72)
    print("SUMMARY")
    print("=" * 72)

    table = format_table(
        results,
        columns=[
            "operation",
            "elapsed_s",
            "user_s",
            "sys_s",
            "cpu_pct",
            "peak_rss_mb",
            "output_size_mb",
            "status",
        ],
        headers=[
            "Operation",
            "Wall (s)",
            "User (s)",
            "Sys (s)",
            "CPU%",
            "RSS (MB)",
            "Out (MB)",
            "Status",
        ],
    )
    print(table)


# ---------------------------------------------------------------------------
# Validation checks
# ---------------------------------------------------------------------------


def validate_data_files(scale: str) -> None:
    """Check that required data files exist and are accessible."""
    if not POPMAP.exists():
        raise FileNotFoundError(f"Population map not found: {POPMAP}")

    if scale == "chr22":
        if not CHR22_VCF.exists():
            raise FileNotFoundError(f"chr22 VCF not found: {CHR22_VCF}")
    else:
        if not ALL_VCFS:
            raise FileNotFoundError(f"No VCF files found in {VCF_DIR}")
        missing = [v for v in ALL_VCFS if not v.exists()]
        if missing:
            raise FileNotFoundError(f"Missing VCFs: {missing[:3]}...")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Integration test and benchmark with real 1kGP data.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python benchmarks/bench_integration_1kgp.py --scale chr22
  python benchmarks/bench_integration_1kgp.py --scale chr22 --annotate
  python benchmarks/bench_integration_1kgp.py --scale genome --threads 16 --skip-full
  python benchmarks/bench_integration_1kgp.py --scale chr22 --skip-ingest  # DB already loaded
""",
    )
    parser.add_argument(
        "--scale",
        choices=["chr22", "genome"],
        default="chr22",
        help="chr22 (~1M variants, ~30 min) or genome (~85M variants, hours).",
    )
    parser.add_argument(
        "--label",
        default=None,
        help="Label for this run (default: 1kgp_{scale}).",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=1,
        help="Number of threads for parallel operations.",
    )
    parser.add_argument(
        "--annotate",
        action="store_true",
        help="Include gnomAD VEP annotation loading (chr22 only).",
    )
    parser.add_argument(
        "--skip-ingest",
        action="store_true",
        help="Skip ingest (assume database already loaded).",
    )
    parser.add_argument(
        "--skip-full",
        action="store_true",
        help="Skip FULL PATH exports (only run FAST PATH).",
    )
    parser.add_argument(
        "--two-step",
        action="store_true",
        help="Use two-step pipeline (prepare-csv + load-csv) instead of single ingest.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output JSONL path (default: results/1kgp_{scale}.jsonl).",
    )
    args = parser.parse_args()

    label = args.label or f"1kgp_{args.scale}"
    results_path = args.output or (RESULTS_DIR / f"{label}.jsonl")
    results_path.parent.mkdir(parents=True, exist_ok=True)

    # Select VCF files
    if args.scale == "chr22":
        vcf_files = [CHR22_VCF]
    else:
        vcf_files = ALL_VCFS

    # Validate
    validate_data_files(args.scale)

    print(f"{'=' * 72}")
    print("GraphMana 1kGP Integration Test")
    print(f"{'=' * 72}")
    print(f"Scale:    {args.scale}")
    print(f"VCFs:     {len(vcf_files)} file(s)")
    print("Samples:  3,202 (26 populations)")
    print(f"Threads:  {args.threads}")
    print(f"Annotate: {args.annotate}")
    print(f"Two-step: {args.two_step}")
    print(f"Results:  {results_path}")
    print(f"{'=' * 72}")

    # -----------------------------------------------------------------------
    # Phase 1: Ingest
    # -----------------------------------------------------------------------
    if not args.skip_ingest:
        print(f"\n{'=' * 72}")
        print("PHASE 1: INGEST")
        print(f"{'=' * 72}")

        if args.two_step:
            csv_dir = bench_prepare_csv(
                vcf_files,
                args.threads,
                label,
                args.scale,
                results_path,
            )
            bench_load_csv(csv_dir, label, args.scale, results_path)
            # Clean up CSV dir
            if csv_dir.exists():
                shutil.rmtree(csv_dir)
                print(f"  cleaned up CSV dir: {csv_dir}")
        else:
            bench_ingest_single_step(
                vcf_files,
                args.threads,
                label,
                args.scale,
                results_path,
            )
    else:
        print("\n[ingest] SKIPPED (--skip-ingest)")

    # -----------------------------------------------------------------------
    # Phase 2: Verification
    # -----------------------------------------------------------------------
    print(f"\n{'=' * 72}")
    print("PHASE 2: VERIFICATION")
    print(f"{'=' * 72}")

    status_data = bench_status(label, args.scale, results_path)

    n_variants = status_data.get("n_variants", 0)
    n_samples = status_data.get("n_samples", 0)

    # Sanity checks
    if n_samples > 0 and n_samples != 3202:
        print(f"  WARNING: expected 3202 samples, got {n_samples}")
    if args.scale == "chr22" and n_variants > 0 and n_variants < 500_000:
        print(f"  WARNING: expected ~1M chr22 variants, got {n_variants}")
    if n_variants > 0:
        print(f"  PASS: database has {n_variants:,} variants, {n_samples:,} samples")

    # -----------------------------------------------------------------------
    # Phase 3: Export all formats
    # -----------------------------------------------------------------------
    print(f"\n{'=' * 72}")
    print("PHASE 3: EXPORT")
    print(f"{'=' * 72}")

    bench_all_exports(
        args.threads,
        label,
        args.scale,
        results_path,
        skip_full=args.skip_full,
    )

    # -----------------------------------------------------------------------
    # Phase 4: Annotation (optional)
    # -----------------------------------------------------------------------
    if args.annotate:
        print(f"\n{'=' * 72}")
        print("PHASE 4: ANNOTATION")
        print(f"{'=' * 72}")

        bench_annotate(label, args.scale, results_path)
        bench_filtered_export(args.threads, label, args.scale, results_path)

    # -----------------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------------
    print_summary(results_path)
    print(f"\nResults saved to: {results_path}")


if __name__ == "__main__":
    main()
