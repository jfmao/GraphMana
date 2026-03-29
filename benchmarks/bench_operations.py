"""Benchmark GraphMana non-import/export operations.

Measures wall-clock time and peak memory for status, QC, soft delete/restore,
snapshot, and cohort operations.

Requires a running Neo4j instance with imported data. Not part of the test suite.

Usage::

    python benchmarks/bench_operations.py --scale medium --label my_run
"""

from __future__ import annotations

import argparse
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from conftest import (
    DEFAULT_WARM_RUNS,
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


def _bench_operation(
    operation: str,
    cmd: list[str],
    scale: str,
    label: str,
    n_warm: int,
    results_path: Path,
) -> None:
    """Run a single operation benchmark with cold+warm runs."""
    n_samples, n_variants = SCALES[scale]
    total_runs = 1 + n_warm

    for run_idx in range(total_runs):
        run_type = "cold" if run_idx == 0 else "warm"
        print(
            f"  {operation} {scale} run {run_idx}/{total_runs - 1} ({run_type})",
            flush=True,
        )

        stdout, m = measure_subprocess(cmd, timeout=600)

        record = {
            "label": label,
            "operation": operation,
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


def bench_status(scale: str, label: str, n_warm: int, results_path: Path) -> None:
    cmd = _graphmana_cmd("status", *_neo4j_args())
    _bench_operation("status", cmd, scale, label, n_warm, results_path)


def bench_qc(
    qc_type: str, scale: str, label: str, n_warm: int, results_path: Path
) -> None:
    with tempfile.TemporaryDirectory(prefix="graphmana_bench_qc_") as tmpdir:
        output_file = Path(tmpdir) / f"qc_{qc_type}.tsv"
        cmd = _graphmana_cmd(
            "qc",
            "--type",
            qc_type,
            "--output",
            str(output_file),
            "--format",
            "tsv",
            *_neo4j_args(),
        )
        _bench_operation(f"qc_{qc_type}", cmd, scale, label, n_warm, results_path)


def bench_soft_delete(scale: str, label: str, n_warm: int, results_path: Path) -> None:
    """Benchmark soft-deleting a batch of samples."""
    # Delete first 10 samples (or fewer if small scale)
    n_delete = min(10, SCALES[scale][0])
    sample_ids = [f"SAMPLE_{i:05d}" for i in range(n_delete)]
    cmd = _graphmana_cmd(
        "sample",
        "remove",
        "--sample-ids",
        ",".join(sample_ids),
        *_neo4j_args(),
    )
    _bench_operation("soft_delete", cmd, scale, label, n_warm, results_path)


def bench_soft_restore(scale: str, label: str, n_warm: int, results_path: Path) -> None:
    """Benchmark restoring soft-deleted samples."""
    n_restore = min(10, SCALES[scale][0])
    sample_ids = [f"SAMPLE_{i:05d}" for i in range(n_restore)]
    cmd = _graphmana_cmd(
        "sample",
        "restore",
        "--sample-ids",
        ",".join(sample_ids),
        *_neo4j_args(),
    )
    _bench_operation("soft_restore", cmd, scale, label, n_warm, results_path)


def bench_cohort_define(
    scale: str, label: str, n_warm: int, results_path: Path
) -> None:
    cmd = _graphmana_cmd(
        "cohort",
        "define",
        "--name",
        "bench_cohort",
        "--query",
        "MATCH (s:Sample) WHERE s.population = 'POP_0' RETURN s.sampleId",
        *_neo4j_args(),
    )
    _bench_operation("cohort_define", cmd, scale, label, n_warm, results_path)


def bench_cohort_count(scale: str, label: str, n_warm: int, results_path: Path) -> None:
    cmd = _graphmana_cmd(
        "cohort",
        "count",
        "--name",
        "bench_cohort",
        *_neo4j_args(),
    )
    _bench_operation("cohort_count", cmd, scale, label, n_warm, results_path)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark GraphMana non-import/export operations."
    )
    parser.add_argument("--scale", choices=list(SCALES.keys()), default="small")
    parser.add_argument("--label", default="operations")
    parser.add_argument(
        "--runs", type=int, default=DEFAULT_WARM_RUNS, help="Warm runs."
    )
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()

    results_path = args.output or (
        RESULTS_DIR / f"{args.label}_{args.scale}_operations.jsonl"
    )
    results_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"=== Operations benchmarks: scale={args.scale}, warm_runs={args.runs} ===")

    print("\n[1/6] status")
    bench_status(args.scale, args.label, args.runs, results_path)

    print("\n[2/6] QC (variant)")
    bench_qc("variant", args.scale, args.label, args.runs, results_path)

    print("\n[3/6] QC (sample)")
    bench_qc("sample", args.scale, args.label, args.runs, results_path)

    print("\n[4/6] Soft delete")
    bench_soft_delete(args.scale, args.label, args.runs, results_path)

    print("\n[5/6] Soft restore")
    bench_soft_restore(args.scale, args.label, args.runs, results_path)

    print("\n[6/6] Cohort define + count")
    bench_cohort_define(args.scale, args.label, args.runs, results_path)
    bench_cohort_count(args.scale, args.label, args.runs, results_path)

    print(f"\nResults → {results_path}")


if __name__ == "__main__":
    main()
