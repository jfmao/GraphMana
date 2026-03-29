"""Compare two JSONL benchmark result files.

Produces a Markdown summary table showing speedup ratios and memory changes
for each operation.

Usage::

    python benchmarks/compare.py results/baseline.jsonl results/optimized.jsonl
    python benchmarks/compare.py results/v0.4.jsonl results/v0.5.jsonl --sort speedup
"""

from __future__ import annotations

import argparse
import statistics
from pathlib import Path

from measurement import format_table, load_results


def _warm_median(records: list[dict], field: str = "elapsed_s") -> float:
    """Median of *field* across warm runs."""
    warm = [r[field] for r in records if r.get("run_type") == "warm" and field in r]
    return statistics.median(warm) if warm else float("nan")


def _peak_rss(records: list[dict]) -> float:
    """Max peak_rss_mb across all runs."""
    vals = [r["peak_rss_mb"] for r in records if r.get("peak_rss_mb")]
    return max(vals) if vals else 0.0


def _group_by_operation(
    results: list[dict],
) -> dict[str, list[dict]]:
    """Group records by operation name."""
    groups: dict[str, list[dict]] = {}
    for r in results:
        op = r.get("operation", "unknown")
        groups.setdefault(op, []).append(r)
    return groups


def compare(before_path: Path, after_path: Path) -> str:
    """Compare two result files and return a Markdown report."""
    before = load_results(before_path)
    after = load_results(after_path)

    before_groups = _group_by_operation(before)
    after_groups = _group_by_operation(after)

    all_ops = sorted(set(before_groups) | set(after_groups))

    rows: list[dict] = []
    for op in all_ops:
        b_records = before_groups.get(op, [])
        a_records = after_groups.get(op, [])

        b_wall = _warm_median(b_records) if b_records else float("nan")
        a_wall = _warm_median(a_records) if a_records else float("nan")
        b_cpu = _warm_median(b_records, "cpu_s") if b_records else float("nan")
        a_cpu = _warm_median(a_records, "cpu_s") if a_records else float("nan")

        b_mem = _peak_rss(b_records)
        a_mem = _peak_rss(a_records)

        if b_wall == b_wall and a_wall == a_wall and a_wall > 0:
            speedup = b_wall / a_wall
            speedup_str = f"{speedup:.2f}x"
            delta_s = b_wall - a_wall
            delta_str = f"{delta_s:+.3f}s"
        else:
            speedup = float("nan")
            speedup_str = "—"
            delta_str = "—"

        mem_delta = a_mem - b_mem
        mem_str = f"{mem_delta:+.1f} MB" if (b_mem or a_mem) else "—"

        rows.append(
            {
                "operation": op,
                "before_wall": f"{b_wall:.3f}" if b_wall == b_wall else "—",
                "after_wall": f"{a_wall:.3f}" if a_wall == a_wall else "—",
                "before_cpu": f"{b_cpu:.3f}" if b_cpu == b_cpu else "—",
                "after_cpu": f"{a_cpu:.3f}" if a_cpu == a_cpu else "—",
                "speedup": speedup_str,
                "delta": delta_str,
                "before_mb": f"{b_mem:.1f}" if b_mem else "—",
                "after_mb": f"{a_mem:.1f}" if a_mem else "—",
                "mem_delta": mem_str,
                "_speedup_val": speedup,
            }
        )

    # Summary header
    lines: list[str] = [
        "## Benchmark Comparison",
        "",
        f"- **Before**: `{before_path.name}`",
        f"- **After**: `{after_path.name}`",
        "",
    ]

    # Wall-clock table
    lines.append("### Wall-clock time (warm median)")
    lines.append("")
    lines.append(
        format_table(
            rows,
            columns=["operation", "before_wall", "after_wall", "speedup", "delta"],
            headers=["Operation", "Before (s)", "After (s)", "Speedup", "Delta"],
        )
    )
    lines.append("")

    # CPU time table
    lines.append("### CPU time (warm median)")
    lines.append("")
    lines.append(
        format_table(
            rows,
            columns=["operation", "before_cpu", "after_cpu"],
            headers=["Operation", "Before (s)", "After (s)"],
        )
    )
    lines.append("")

    # Memory table
    lines.append("### Peak memory (RSS)")
    lines.append("")
    lines.append(
        format_table(
            rows,
            columns=["operation", "before_mb", "after_mb", "mem_delta"],
            headers=["Operation", "Before (MB)", "After (MB)", "Delta"],
        )
    )
    lines.append("")

    # Overall summary
    valid_speedups = [
        r["_speedup_val"] for r in rows if r["_speedup_val"] == r["_speedup_val"]
    ]
    if valid_speedups:
        geo_mean = statistics.geometric_mean([s for s in valid_speedups if s > 0])
        lines.append(f"**Geometric mean speedup: {geo_mean:.2f}x**")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare two benchmark JSONL result files."
    )
    parser.add_argument("before", type=Path, help="Baseline results file.")
    parser.add_argument("after", type=Path, help="Optimized results file.")
    args = parser.parse_args()

    report = compare(args.before, args.after)
    print(report)


if __name__ == "__main__":
    main()
