"""Benchmark parallel speedup for GraphMana ingest and export.

Measures wall-clock time for ``--threads 1`` vs ``--threads N`` and verifies
that parallel output is identical to sequential output.

Requires a running Neo4j instance with imported data. Not part of the test suite.

Usage::

    python benchmarks/bench_parallel.py --scale medium --threads 4 --label par_test
"""

from __future__ import annotations

import argparse
import hashlib
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from conftest import (
    DEFAULT_WARM_RUNS,
    EXPORT_FORMATS_ALL,
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
    return vcf, popmap


def _file_hash(path: Path) -> str:
    """SHA256 of file contents."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def bench_parallel_export(
    fmt: str,
    scale: str,
    label: str,
    threads: int,
    n_warm: int,
    results_path: Path,
) -> bool:
    """Benchmark export at threads=1 and threads=N, verify identical output.

    Returns True if outputs match.
    """
    n_samples, n_variants = SCALES[scale]

    for t in [1, threads]:
        total_runs = 1 + n_warm
        for run_idx in range(total_runs):
            run_type = "cold" if run_idx == 0 else "warm"

            with tempfile.TemporaryDirectory(prefix="graphmana_bench_par_") as tmpdir:
                output_file = Path(tmpdir) / f"export_t{t}.out"
                cmd = _graphmana_cmd(
                    "export",
                    "--format",
                    fmt,
                    "--output",
                    str(output_file),
                    "--threads",
                    str(t),
                    *_neo4j_args(),
                )
                print(
                    f"  parallel export {fmt} t={t} run {run_idx}/{total_runs - 1} ({run_type})",
                    flush=True,
                )

                stdout, m = measure_subprocess(cmd, timeout=3600)

                record = {
                    "label": label,
                    "operation": f"parallel_export_{fmt}",
                    "format": fmt,
                    "scale": scale,
                    "n_samples": n_samples,
                    "n_variants": n_variants,
                    "threads": t,
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

    # Verify consistency: run once at t=1 and t=N, compare output
    return verify_identical_output(fmt, scale, threads)


def bench_parallel_prepare_csv(
    scale: str,
    label: str,
    threads: int,
    n_warm: int,
    results_path: Path,
) -> None:
    """Benchmark prepare-csv at threads=1 and threads=N."""
    vcf, popmap = _fixture_paths(scale)
    n_samples, n_variants = SCALES[scale]

    for t in [1, threads]:
        total_runs = 1 + n_warm
        for run_idx in range(total_runs):
            run_type = "cold" if run_idx == 0 else "warm"

            with tempfile.TemporaryDirectory(
                prefix="graphmana_bench_par_csv_"
            ) as tmpdir:
                cmd = _graphmana_cmd(
                    "prepare-csv",
                    "--input",
                    str(vcf),
                    "--population-map",
                    str(popmap),
                    "--output-dir",
                    tmpdir,
                    "--threads",
                    str(t),
                )
                print(
                    f"  parallel prepare-csv t={t} run {run_idx}/{total_runs - 1} ({run_type})",
                    flush=True,
                )

                stdout, m = measure_subprocess(cmd, timeout=3600)

                record = {
                    "label": label,
                    "operation": "parallel_prepare_csv",
                    "scale": scale,
                    "n_samples": n_samples,
                    "n_variants": n_variants,
                    "threads": t,
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


def verify_identical_output(fmt: str, scale: str, threads: int) -> bool:
    """Run export at t=1 and t=N, compare output files. Returns True if match."""
    with tempfile.TemporaryDirectory(prefix="graphmana_bench_verify_") as tmpdir:
        out_1 = Path(tmpdir) / "t1.out"
        out_n = Path(tmpdir) / f"t{threads}.out"

        for out_path, t in [(out_1, 1), (out_n, threads)]:
            cmd = _graphmana_cmd(
                "export",
                "--format",
                fmt,
                "--output",
                str(out_path),
                "--threads",
                str(t),
                *_neo4j_args(),
            )
            subprocess_out, _ = measure_subprocess(cmd, timeout=3600)

        if not out_1.exists() or not out_n.exists():
            print(f"  WARN: output files missing for {fmt} verification")
            return False

        h1 = _file_hash(out_1)
        hn = _file_hash(out_n)
        match = h1 == hn
        status = "PASS" if match else "FAIL"
        print(f"  verify {fmt} t=1 vs t={threads}: {status}")
        return match


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark GraphMana parallel speedup."
    )
    parser.add_argument("--scale", choices=list(SCALES.keys()), default="small")
    parser.add_argument("--label", default="parallel")
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument(
        "--runs", type=int, default=DEFAULT_WARM_RUNS, help="Warm runs."
    )
    parser.add_argument(
        "--format",
        dest="fmt",
        choices=EXPORT_FORMATS_ALL,
        default=None,
        help="Single format to test (default: vcf + treemix).",
    )
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()

    results_path = args.output or (
        RESULTS_DIR / f"{args.label}_{args.scale}_parallel.jsonl"
    )
    results_path.parent.mkdir(parents=True, exist_ok=True)

    print(
        f"=== Parallel benchmarks: scale={args.scale}, threads={args.threads}, "
        f"warm_runs={args.runs} ==="
    )

    formats = [args.fmt] if args.fmt else ["vcf", "treemix"]

    for fmt in formats:
        print(f"\n--- {fmt} ---")
        bench_parallel_export(
            fmt, args.scale, args.label, args.threads, args.runs, results_path
        )

    print("\n--- prepare-csv ---")
    bench_parallel_prepare_csv(
        args.scale, args.label, args.threads, args.runs, results_path
    )

    print(f"\nResults → {results_path}")


if __name__ == "__main__":
    main()
