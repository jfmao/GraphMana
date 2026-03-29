"""Benchmark GraphMana export operations.

Measures wall-clock time and peak memory for all Tier 1 export formats.
Key measurement: FAST PATH formats (treemix, sfs-dadi, sfs-fsc, bed) are
expected to complete in seconds regardless of sample count, while FULL PATH
formats (vcf, plink, eigenstrat, tsv) scale linearly with N.

Requires a running Neo4j instance with imported data. Not part of the test suite.

Usage::

    python benchmarks/bench_export.py --scale medium --label my_run
    python benchmarks/bench_export.py --scale medium --format treemix --runs 5
"""

from __future__ import annotations

import argparse
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from conftest import (
    DEFAULT_WARM_RUNS,
    EXPORT_FORMATS_ALL,
    EXPORT_FORMATS_FAST,
    EXPORT_FORMATS_FULL,
    NEO4J_PASSWORD,
    NEO4J_URI,
    NEO4J_USER,
    RESULTS_DIR,
    SCALES,
)
from measurement import measure_subprocess, write_result

# File extensions per format
_FORMAT_EXT: dict[str, str] = {
    "vcf": ".vcf.gz",
    "plink": ".bed",
    "eigenstrat": ".geno",
    "treemix": ".treemix.gz",
    "sfs-dadi": ".sfs",
    "sfs-fsc": "_jointMAFpop1_0.obs",
    "bed": ".bed",
    "tsv": ".tsv",
}


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


def bench_export(
    fmt: str,
    scale: str,
    label: str,
    n_warm: int,
    results_path: Path,
    *,
    threads: int = 1,
) -> None:
    """Benchmark a single export format."""
    n_samples, n_variants = SCALES[scale]
    path_type = "fast" if fmt in EXPORT_FORMATS_FAST else "full"

    total_runs = 1 + n_warm
    for run_idx in range(total_runs):
        run_type = "cold" if run_idx == 0 else "warm"

        with tempfile.TemporaryDirectory(prefix="graphmana_bench_export_") as tmpdir:
            ext = _FORMAT_EXT.get(fmt, ".out")
            output_file = Path(tmpdir) / f"export{ext}"

            cmd = _graphmana_cmd(
                "export",
                "--format",
                fmt,
                "--output",
                str(output_file),
                "--threads",
                str(threads),
                *_neo4j_args(),
            )
            print(
                f"  export {fmt} {scale} t={threads} run {run_idx}/{total_runs - 1} ({run_type})",
                flush=True,
            )

            stdout, m = measure_subprocess(cmd, timeout=3600)

            # Measure output size
            output_size_mb = 0.0
            for f in Path(tmpdir).iterdir():
                output_size_mb += f.stat().st_size / (1024 * 1024)

            record = {
                "label": label,
                "operation": f"export_{fmt}",
                "format": fmt,
                "path_type": path_type,
                "scale": scale,
                "n_samples": n_samples,
                "n_variants": n_variants,
                "threads": threads,
                "run_type": run_type,
                "run_index": run_idx,
                "elapsed_s": round(m.wall_s, 3),
                "user_s": round(m.user_s, 3),
                "sys_s": round(m.sys_s, 3),
                "cpu_s": round(m.cpu_s, 3),
                "cpu_pct": m.cpu_pct,
                "peak_rss_mb": round(m.peak_rss_mb, 1),
                "output_size_mb": round(output_size_mb, 2),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            write_result(results_path, record)


def bench_fast_vs_full(
    scale: str,
    label: str,
    n_warm: int,
    results_path: Path,
) -> None:
    """Run all FAST PATH and FULL PATH formats for direct comparison."""
    print("\n--- FAST PATH formats ---")
    for fmt in EXPORT_FORMATS_FAST:
        bench_export(fmt, scale, label, n_warm, results_path)

    print("\n--- FULL PATH formats ---")
    for fmt in EXPORT_FORMATS_FULL:
        bench_export(fmt, scale, label, n_warm, results_path)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark GraphMana export operations."
    )
    parser.add_argument("--scale", choices=list(SCALES.keys()), default="small")
    parser.add_argument("--label", default="export")
    parser.add_argument(
        "--runs", type=int, default=DEFAULT_WARM_RUNS, help="Warm runs."
    )
    parser.add_argument(
        "--format", dest="fmt", choices=EXPORT_FORMATS_ALL, default=None
    )
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()

    results_path = args.output or (
        RESULTS_DIR / f"{args.label}_{args.scale}_export.jsonl"
    )
    results_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"=== Export benchmarks: scale={args.scale}, warm_runs={args.runs} ===")

    if args.fmt:
        bench_export(
            args.fmt,
            args.scale,
            args.label,
            args.runs,
            results_path,
            threads=args.threads,
        )
    else:
        bench_fast_vs_full(args.scale, args.label, args.runs, results_path)

    print(f"\nResults → {results_path}")


if __name__ == "__main__":
    main()
