"""Experiment B: Export-remove-restore lifecycle benchmark on 1KGP data.

For each N in {100, 200, 300, 400, 500} x EXPERIMENT_B_REPS reps:
  1. Query all active sample IDs from database
  2. Randomly select N samples
  3. Export to VCF via --filter-sample-list
  4. Soft-delete via graphmana sample remove --sample-list
  5. Restore via graphmana sample restore --sample-list
  6. Verify sample count returns to original
  7. Record per-step: export_s, remove_s, restore_s, total_cycle_s

Demonstrates that data management lifecycle operations (soft-delete/restore)
are near-instant regardless of dataset size — a core GraphMana strength.

Requires a running Neo4j instance with 1KGP data already imported.

Usage::

    python benchmarks/bench_1kgp_lifecycle.py --label 1kgp_lifecycle
    python benchmarks/bench_1kgp_lifecycle.py --label test --reps 5 --sizes 100,200
"""

from __future__ import annotations

import argparse
import random
import subprocess
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

from conftest import (
    EXPERIMENT_B_REPS,
    EXPORT_SUBSET_SIZES,
    NEO4J_PASSWORD,
    NEO4J_URI,
    NEO4J_USER,
    RESULTS_DIR,
)
from measurement import measure_subprocess, write_result


def _neo4j_args() -> list[str]:
    return [
        "--neo4j-uri",
        NEO4J_URI,
        "--neo4j-user",
        NEO4J_USER,
        "--neo4j-password",
        NEO4J_PASSWORD,
    ]


def _fetch_active_sample_ids() -> list[str]:
    """Query all active sample IDs from the database."""
    cmd = ["graphmana", "sample", "list", *_neo4j_args()]
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


def bench_lifecycle_cycle(
    n_samples: int,
    rep: int,
    seed: int,
    label: str,
    results_path: Path,
) -> None:
    """Benchmark a single export-remove-restore cycle."""
    all_ids = _fetch_active_sample_ids()
    n_before = len(all_ids)

    if n_samples > n_before:
        print(
            f"  WARNING: requested {n_samples} but only {n_before} available. Skipping."
        )
        return

    rng = random.Random(seed)
    selected = rng.sample(all_ids, n_samples)

    with tempfile.TemporaryDirectory(prefix="graphmana_1kgp_lifecycle_") as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Write sample list file
        sample_list_file = tmpdir_path / "samples.txt"
        sample_list_file.write_text("\n".join(selected) + "\n")

        output_file = tmpdir_path / "export.vcf.gz"

        print(
            f"  lifecycle N={n_samples} rep={rep} seed={seed}",
            flush=True,
        )

        # Step 1: Export
        export_cmd = [
            "graphmana",
            "export",
            "--format",
            "vcf",
            "--output",
            str(output_file),
            "--filter-sample-list",
            str(sample_list_file),
            *_neo4j_args(),
        ]
        _, export_m = measure_subprocess(export_cmd, timeout=7200)

        output_size_mb = 0.0
        if output_file.exists():
            output_size_mb = output_file.stat().st_size / (1024 * 1024)

        # Step 2: Soft-delete
        remove_cmd = [
            "graphmana",
            "sample",
            "remove",
            "--sample-list",
            str(sample_list_file),
            "--reason",
            f"lifecycle_bench_rep{rep}",
            *_neo4j_args(),
        ]
        _, remove_m = measure_subprocess(remove_cmd, timeout=300)

        # Step 3: Restore
        restore_cmd = [
            "graphmana",
            "sample",
            "restore",
            "--sample-list",
            str(sample_list_file),
            *_neo4j_args(),
        ]
        _, restore_m = measure_subprocess(restore_cmd, timeout=300)

    # Step 4: Verify sample count
    all_ids_after = _fetch_active_sample_ids()
    n_after = len(all_ids_after)
    verified = n_after == n_before

    if not verified:
        print(
            f"  WARNING: sample count mismatch! Before={n_before}, After={n_after}"
        )

    total_cycle_wall = export_m.wall_s + remove_m.wall_s + restore_m.wall_s
    total_cycle_cpu = export_m.cpu_s + remove_m.cpu_s + restore_m.cpu_s

    record = {
        "label": label,
        "experiment": "B_lifecycle_cycle",
        "n_samples_subset": n_samples,
        "n_total_samples": n_before,
        "rep": rep,
        "seed": seed,
        "export_wall_s": round(export_m.wall_s, 3),
        "export_cpu_s": round(export_m.cpu_s, 3),
        "export_cpu_pct": export_m.cpu_pct,
        "remove_wall_s": round(remove_m.wall_s, 3),
        "remove_cpu_s": round(remove_m.cpu_s, 3),
        "remove_cpu_pct": remove_m.cpu_pct,
        "restore_wall_s": round(restore_m.wall_s, 3),
        "restore_cpu_s": round(restore_m.cpu_s, 3),
        "restore_cpu_pct": restore_m.cpu_pct,
        "total_cycle_wall_s": round(total_cycle_wall, 3),
        "total_cycle_cpu_s": round(total_cycle_cpu, 3),
        "export_rss_mb": round(export_m.peak_rss_mb, 1),
        "remove_rss_mb": round(remove_m.peak_rss_mb, 1),
        "restore_rss_mb": round(restore_m.peak_rss_mb, 1),
        "output_size_mb": round(output_size_mb, 2),
        "verified": verified,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    write_result(results_path, record)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Experiment B: Export-remove-restore lifecycle benchmarks."
    )
    parser.add_argument("--label", default="1kgp_lifecycle")
    parser.add_argument(
        "--reps",
        type=int,
        default=EXPERIMENT_B_REPS,
        help=f"Repetitions per subset size (default: {EXPERIMENT_B_REPS}).",
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
        RESULTS_DIR / f"{args.label}_1kgp_lifecycle.jsonl"
    )
    results_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"=== Experiment B: Export-remove-restore lifecycle ===")
    print(f"Sizes: {sizes}, Reps: {args.reps}")
    print(f"Results → {results_path}")

    for n in sizes:
        for rep in range(args.reps):
            seed = 42 + rep * 1000 + n
            bench_lifecycle_cycle(n, rep, seed, args.label, results_path)

    print(f"\nDone. Results → {results_path}")


if __name__ == "__main__":
    main()
