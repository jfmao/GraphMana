"""Benchmark GraphMana import operations.

Measures wall-clock time and peak memory for:
- Initial ingest (single-step)
- prepare-csv (cluster-friendly CSV generation)
- load-csv (load pre-generated CSVs into Neo4j)

Requires a running Neo4j instance. Not part of the test suite.

Usage::

    python benchmarks/bench_ingest.py --scale medium --label my_run
"""

from __future__ import annotations

import argparse
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from conftest import (
    DEFAULT_WARM_RUNS,
    FIXTURES_DIR,
    NEO4J_PASSWORD,
    NEO4J_URI,
    NEO4J_USER,
    RESULTS_DIR,
    SCALES,
)
from measurement import measure_subprocess, write_result


def _graphmana_cmd(*args: str) -> list[str]:
    return ["graphmana", *args]


def _neo4j_args() -> list[str]:
    return [
        "--neo4j-uri",
        NEO4J_URI,
        "--neo4j-user",
        NEO4J_USER,
        "--neo4j-password",
        NEO4J_PASSWORD,
    ]


def _fixture_paths(scale: str) -> tuple[Path, Path]:
    n_samples, n_variants = SCALES[scale]
    vcf = FIXTURES_DIR / f"bench_{n_samples}s_{n_variants}v.vcf.gz"
    popmap = FIXTURES_DIR / f"bench_{n_samples}s_{n_variants}v_popmap.tsv"
    if not vcf.exists():
        raise FileNotFoundError(
            f"Fixture {vcf} not found. Run generate_fixtures.py first."
        )
    return vcf, popmap


def bench_ingest_initial(
    scale: str,
    label: str,
    n_warm: int,
    results_path: Path,
) -> None:
    """Benchmark full ingest (VCF → Neo4j)."""
    vcf, popmap = _fixture_paths(scale)
    n_samples, n_variants = SCALES[scale]
    cmd = _graphmana_cmd(
        "ingest",
        "--input",
        str(vcf),
        "--population-map",
        str(popmap),
        *_neo4j_args(),
    )

    total_runs = 1 + n_warm  # 1 cold + n warm
    for run_idx in range(total_runs):
        run_type = "cold" if run_idx == 0 else "warm"
        print(
            f"  ingest {scale} run {run_idx}/{total_runs - 1} ({run_type})", flush=True
        )

        stdout, m = measure_subprocess(cmd, timeout=3600)

        record = {
            "label": label,
            "operation": "ingest_initial",
            "scale": scale,
            "n_samples": n_samples,
            "n_variants": n_variants,
            "run_type": run_type,
            "run_index": run_idx,
            "elapsed_s": round(m.wall_s, 3),
            "user_s": round(m.user_s, 3),
            "sys_s": round(m.sys_s, 3),
            "cpu_s": round(m.cpu_s, 3),
            "cpu_pct": m.cpu_pct,
            "peak_rss_mb": round(m.peak_rss_mb, 1),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        write_result(results_path, record)


def bench_prepare_csv(
    scale: str,
    label: str,
    n_warm: int,
    results_path: Path,
) -> Path:
    """Benchmark prepare-csv (no Neo4j needed). Returns CSV output dir."""
    vcf, popmap = _fixture_paths(scale)
    n_samples, n_variants = SCALES[scale]
    csv_dir = Path(tempfile.mkdtemp(prefix="graphmana_bench_csv_"))

    total_runs = 1 + n_warm
    for run_idx in range(total_runs):
        run_type = "cold" if run_idx == 0 else "warm"
        # Clean output dir between runs
        if csv_dir.exists():
            shutil.rmtree(csv_dir)
        csv_dir.mkdir(parents=True)

        cmd = _graphmana_cmd(
            "prepare-csv",
            "--input",
            str(vcf),
            "--population-map",
            str(popmap),
            "--output-dir",
            str(csv_dir),
        )
        print(
            f"  prepare-csv {scale} run {run_idx}/{total_runs - 1} ({run_type})",
            flush=True,
        )

        stdout, m = measure_subprocess(cmd, timeout=3600)

        record = {
            "label": label,
            "operation": "prepare_csv",
            "scale": scale,
            "n_samples": n_samples,
            "n_variants": n_variants,
            "run_type": run_type,
            "run_index": run_idx,
            "elapsed_s": round(m.wall_s, 3),
            "user_s": round(m.user_s, 3),
            "sys_s": round(m.sys_s, 3),
            "cpu_s": round(m.cpu_s, 3),
            "cpu_pct": m.cpu_pct,
            "peak_rss_mb": round(m.peak_rss_mb, 1),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        write_result(results_path, record)

    return csv_dir


def bench_load_csv(
    csv_dir: Path,
    scale: str,
    label: str,
    n_warm: int,
    results_path: Path,
) -> None:
    """Benchmark load-csv (CSV → Neo4j)."""
    n_samples, n_variants = SCALES[scale]
    cmd = _graphmana_cmd(
        "load-csv",
        "--csv-dir",
        str(csv_dir),
        *_neo4j_args(),
    )

    total_runs = 1 + n_warm
    for run_idx in range(total_runs):
        run_type = "cold" if run_idx == 0 else "warm"
        print(
            f"  load-csv {scale} run {run_idx}/{total_runs - 1} ({run_type})",
            flush=True,
        )

        stdout, m = measure_subprocess(cmd, timeout=3600)

        record = {
            "label": label,
            "operation": "load_csv",
            "scale": scale,
            "n_samples": n_samples,
            "n_variants": n_variants,
            "run_type": run_type,
            "run_index": run_idx,
            "elapsed_s": round(m.wall_s, 3),
            "user_s": round(m.user_s, 3),
            "sys_s": round(m.sys_s, 3),
            "cpu_s": round(m.cpu_s, 3),
            "cpu_pct": m.cpu_pct,
            "peak_rss_mb": round(m.peak_rss_mb, 1),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        write_result(results_path, record)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark GraphMana ingest operations."
    )
    parser.add_argument("--scale", choices=list(SCALES.keys()), default="small")
    parser.add_argument("--label", default="ingest")
    parser.add_argument(
        "--runs", type=int, default=DEFAULT_WARM_RUNS, help="Warm runs."
    )
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()

    results_path = args.output or (
        RESULTS_DIR / f"{args.label}_{args.scale}_ingest.jsonl"
    )
    results_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"=== Ingest benchmarks: scale={args.scale}, warm_runs={args.runs} ===")

    print("\n[1/3] Initial ingest")
    bench_ingest_initial(args.scale, args.label, args.runs, results_path)

    print("\n[2/3] prepare-csv")
    csv_dir = bench_prepare_csv(args.scale, args.label, args.runs, results_path)

    print("\n[3/3] load-csv")
    bench_load_csv(csv_dir, args.scale, args.label, args.runs, results_path)

    # Cleanup temp CSV dir
    if csv_dir.exists():
        shutil.rmtree(csv_dir)

    print(f"\nResults → {results_path}")


if __name__ == "__main__":
    main()
