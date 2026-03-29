"""Orchestrate the full GraphMana benchmark suite.

Generates fixtures (if missing), runs all benchmark categories, and prints
a summary table.

Requires a running Neo4j instance. Not part of the test suite.

Usage::

    python benchmarks/run_all.py --scale medium --label v0.5 --runs 3
    python benchmarks/run_all.py --scale small --label quick_test --runs 1
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from conftest import DEFAULT_WARM_RUNS, FIXTURES_DIR, RESULTS_DIR, SCALES
from measurement import load_results, format_table


def _run_script(script: str, args: list[str]) -> int:
    """Run a benchmark script as a subprocess. Returns exit code."""
    cmd = [sys.executable, str(Path(__file__).parent / script), *args]
    print(f"\n{'=' * 60}")
    print(f"Running: {' '.join(cmd)}")
    print(f"{'=' * 60}\n")
    result = subprocess.run(cmd, cwd=str(Path(__file__).parent))
    return result.returncode


def _generate_fixtures_if_needed(scale: str) -> None:
    """Generate fixtures for the given scale if they don't exist."""
    n_samples, n_variants = SCALES[scale]
    vcf = FIXTURES_DIR / f"bench_{n_samples}s_{n_variants}v.vcf.gz"
    if vcf.exists():
        print(f"Fixtures for scale={scale} already exist at {vcf}")
        return

    print(f"Generating fixtures for scale={scale}...")
    rc = _run_script(
        "generate_fixtures.py",
        ["--scale", scale, "--output-dir", str(FIXTURES_DIR)],
    )
    if rc != 0:
        print(f"ERROR: fixture generation failed (rc={rc})")
        sys.exit(1)


def _print_summary(label: str, scale: str) -> None:
    """Print a combined summary from all result files."""
    pattern = f"{label}_{scale}_*.jsonl"
    result_files = sorted(RESULTS_DIR.glob(pattern))

    if not result_files:
        print("No result files found.")
        return

    all_records: list[dict] = []
    for rf in result_files:
        all_records.extend(load_results(rf))

    # Build summary: warm median per operation
    from collections import defaultdict
    import statistics

    op_wall: dict[str, list[float]] = defaultdict(list)
    op_cpu: dict[str, list[float]] = defaultdict(list)
    op_pct: dict[str, list[int]] = defaultdict(list)
    op_mem: dict[str, list[float]] = defaultdict(list)

    for r in all_records:
        op = r.get("operation", "unknown")
        if r.get("run_type") == "warm":
            op_wall[op].append(r["elapsed_s"])
            if "cpu_s" in r:
                op_cpu[op].append(r["cpu_s"])
            if "cpu_pct" in r:
                op_pct[op].append(r["cpu_pct"])
        op_mem[op].append(r.get("peak_rss_mb", 0))

    rows = []
    for op in sorted(op_wall):
        median_wall = statistics.median(op_wall[op])
        median_cpu = statistics.median(op_cpu[op]) if op_cpu.get(op) else 0.0
        median_pct = int(statistics.median(op_pct[op])) if op_pct.get(op) else 0
        peak_mb = max(op_mem.get(op, [0]))
        rows.append(
            {
                "operation": op,
                "wall_s": f"{median_wall:.3f}",
                "cpu_s": f"{median_cpu:.3f}",
                "cpu_pct": f"{median_pct}%",
                "peak_mb": f"{peak_mb:.1f}",
                "n_warm": str(len(op_wall[op])),
            }
        )

    print(f"\n{'=' * 60}")
    print(f"SUMMARY: label={label}, scale={scale}")
    print(f"{'=' * 60}\n")
    print(
        format_table(
            rows,
            columns=["operation", "wall_s", "cpu_s", "cpu_pct", "peak_mb", "n_warm"],
            headers=["Operation", "Wall (s)", "CPU (s)", "CPU %", "Peak RSS (MB)", "Warm runs"],
        )
    )
    print(f"\nResult files: {', '.join(f.name for f in result_files)}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the full GraphMana benchmark suite."
    )
    parser.add_argument("--scale", choices=list(SCALES.keys()), default="small")
    parser.add_argument("--label", default="bench")
    parser.add_argument(
        "--runs", type=int, default=DEFAULT_WARM_RUNS, help="Warm runs per operation."
    )
    parser.add_argument(
        "--threads", type=int, default=4, help="Thread count for parallel benchmarks."
    )
    parser.add_argument(
        "--skip",
        nargs="*",
        choices=["ingest", "export", "operations", "parallel"],
        default=[],
        help="Categories to skip.",
    )
    parser.add_argument(
        "--include-1kgp",
        action="store_true",
        help="Run 1KGP real-data experiments (requires prior 1KGP import).",
    )
    parser.add_argument(
        "--onekg-reps-a",
        type=int,
        default=10,
        help="Reps for 1KGP Experiment A (random subset export).",
    )
    parser.add_argument(
        "--onekg-reps-b",
        type=int,
        default=100,
        help="Reps for 1KGP Experiment B (lifecycle cycle).",
    )
    args = parser.parse_args()

    common_args = [
        "--scale",
        args.scale,
        "--label",
        args.label,
        "--runs",
        str(args.runs),
    ]

    print(f"GraphMana Benchmark Suite — {datetime.now(timezone.utc).isoformat()}")
    print(
        f"Scale: {args.scale} ({SCALES[args.scale][0]} samples, {SCALES[args.scale][1]} variants)"
    )
    print(f"Warm runs: {args.runs}, Parallel threads: {args.threads}")

    # Step 1: Generate fixtures
    _generate_fixtures_if_needed(args.scale)

    # Step 2–5: Run benchmark categories
    steps = [
        ("ingest", "bench_ingest.py", common_args),
        ("export", "bench_export.py", common_args),
        ("operations", "bench_operations.py", common_args),
        (
            "parallel",
            "bench_parallel.py",
            [*common_args, "--threads", str(args.threads)],
        ),
    ]

    for category, script, script_args in steps:
        if category in args.skip:
            print(f"\nSkipping {category}")
            continue
        rc = _run_script(script, script_args)
        if rc != 0:
            print(f"WARNING: {category} benchmark failed (rc={rc})")

    # Step 6: 1KGP experiments (optional)
    if args.include_1kgp:
        onekg_steps = [
            (
                "1kgp_export",
                "bench_1kgp_export.py",
                ["--label", args.label, "--reps", str(args.onekg_reps_a)],
            ),
            (
                "1kgp_lifecycle",
                "bench_1kgp_lifecycle.py",
                ["--label", args.label, "--reps", str(args.onekg_reps_b)],
            ),
        ]
        for category, script, script_args in onekg_steps:
            rc = _run_script(script, script_args)
            if rc != 0:
                print(f"WARNING: {category} benchmark failed (rc={rc})")

    # Step 7: Summary
    _print_summary(args.label, args.scale)


if __name__ == "__main__":
    main()
