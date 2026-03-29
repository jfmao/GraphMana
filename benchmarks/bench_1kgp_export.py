"""Experiment A: Random subset VCF export from 1KGP data.

For each N in {100, 200, 300, 400, 500} x EXPERIMENT_A_REPS reps:
  1. Query all active sample IDs from database
  2. Randomly select N samples (different seed per rep)
  3. Write sample IDs to temp file
  4. Export VCF via --filter-sample-list
  5. Record: elapsed_s, peak_rss_mb, output_size_mb, n_samples, rep

Requires a running Neo4j instance with 1KGP data already imported.

Usage::

    python benchmarks/bench_1kgp_export.py --label 1kgp_v1
    python benchmarks/bench_1kgp_export.py --label 1kgp_v1 --reps 5 --sizes 100,200
"""

from __future__ import annotations

import argparse
import random
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from conftest import (
    EXPERIMENT_A_REPS,
    EXPORT_SUBSET_SIZES,
    NEO4J_PASSWORD,
    NEO4J_URI,
    NEO4J_USER,
    RESULTS_DIR,
)
from measurement import measure_subprocess, write_result


def _fetch_active_sample_ids() -> list[str]:
    """Query all active sample IDs from the database via graphmana CLI."""
    from measurement import Timer

    import subprocess

    cmd = [
        "graphmana",
        "sample",
        "list",
        "--neo4j-uri",
        NEO4J_URI,
        "--neo4j-user",
        NEO4J_USER,
        "--neo4j-password",
        NEO4J_PASSWORD,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    if proc.returncode != 0:
        raise RuntimeError(f"Failed to list samples: {proc.stderr[:300]}")

    sample_ids = []
    for line in proc.stdout.strip().split("\n"):
        line = line.strip()
        if not line or line.startswith("Samples:"):
            continue
        parts = line.split()
        if parts:
            sample_ids.append(parts[0])
    return sample_ids


def bench_random_subset_export(
    n_samples: int,
    rep: int,
    seed: int,
    label: str,
    results_path: Path,
) -> None:
    """Benchmark a single random subset VCF export."""
    all_ids = _fetch_active_sample_ids()
    if n_samples > len(all_ids):
        print(
            f"  WARNING: requested {n_samples} but only {len(all_ids)} available. Skipping."
        )
        return

    rng = random.Random(seed)
    selected = rng.sample(all_ids, n_samples)

    with tempfile.TemporaryDirectory(prefix="graphmana_1kgp_export_") as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Write sample list
        sample_list_file = tmpdir_path / "samples.txt"
        sample_list_file.write_text("\n".join(selected) + "\n")

        output_file = tmpdir_path / "export.vcf.gz"

        cmd = [
            "graphmana",
            "export",
            "--format",
            "vcf",
            "--output",
            str(output_file),
            "--filter-sample-list",
            str(sample_list_file),
            "--neo4j-uri",
            NEO4J_URI,
            "--neo4j-user",
            NEO4J_USER,
            "--neo4j-password",
            NEO4J_PASSWORD,
        ]

        print(
            f"  export vcf subset N={n_samples} rep={rep} seed={seed}",
            flush=True,
        )

        stdout, m = measure_subprocess(cmd, timeout=7200)

        # Measure output size
        output_size_mb = 0.0
        if output_file.exists():
            output_size_mb = output_file.stat().st_size / (1024 * 1024)

        record = {
            "label": label,
            "experiment": "A_random_subset_export",
            "operation": "export_vcf_subset",
            "n_samples": n_samples,
            "n_total_samples": len(all_ids),
            "rep": rep,
            "seed": seed,
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


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Experiment A: Random subset VCF export benchmarks."
    )
    parser.add_argument("--label", default="1kgp_export")
    parser.add_argument(
        "--reps",
        type=int,
        default=EXPERIMENT_A_REPS,
        help=f"Repetitions per subset size (default: {EXPERIMENT_A_REPS}).",
    )
    parser.add_argument(
        "--sizes",
        type=str,
        default=None,
        help="Comma-separated subset sizes (default: 100,200,300,400,500).",
    )
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()

    sizes = (
        [int(x) for x in args.sizes.split(",")]
        if args.sizes
        else EXPORT_SUBSET_SIZES
    )

    results_path = args.output or (
        RESULTS_DIR / f"{args.label}_1kgp_export.jsonl"
    )
    results_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"=== Experiment A: Random subset export ===")
    print(f"Sizes: {sizes}, Reps: {args.reps}")
    print(f"Results → {results_path}")

    for n in sizes:
        for rep in range(args.reps):
            seed = 42 + rep * 1000 + n
            bench_random_subset_export(n, rep, seed, args.label, results_path)

    print(f"\nDone. Results → {results_path}")


if __name__ == "__main__":
    main()
