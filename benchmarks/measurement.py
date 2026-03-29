"""Benchmark measurement utilities for GraphMana.

Provides wall-clock timing, CPU time measurement, memory measurement,
JSONL result persistence, and human-readable table formatting.
Adapted from GraphPop benchmark infra.
"""

from __future__ import annotations

import json
import re
import subprocess
import time
import tracemalloc
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Callable


@dataclass
class SubprocessMetrics:
    """All metrics captured from a single subprocess run.

    Attributes:
        wall_s: Wall-clock time in seconds (time.perf_counter).
        user_s: User-space CPU time in seconds (from /usr/bin/time -v).
        sys_s: Kernel CPU time in seconds (from /usr/bin/time -v).
        cpu_pct: CPU utilization percentage (from /usr/bin/time -v).
            For a single-threaded process this caps at 100%;
            for multi-threaded it can exceed 100% (e.g. 400% = 4 cores busy).
        peak_rss_mb: Peak resident set size in MB (from /usr/bin/time -v).
    """

    wall_s: float = 0.0
    user_s: float = 0.0
    sys_s: float = 0.0
    cpu_pct: int = 0
    peak_rss_mb: float = 0.0

    @property
    def cpu_s(self) -> float:
        """Total CPU time (user + system)."""
        return self.user_s + self.sys_s

    def to_dict(self) -> dict[str, float | int]:
        """Return all metrics as a flat dict for JSONL serialization."""
        d = asdict(self)
        d["cpu_s"] = self.cpu_s
        return d


class Timer:
    """Context manager for wall-clock timing via perf_counter."""

    def __init__(self) -> None:
        self.elapsed: float = 0.0

    def __enter__(self) -> Timer:
        self._t0 = time.perf_counter()
        return self

    def __exit__(self, *args: object) -> None:
        self.elapsed = time.perf_counter() - self._t0


def measure_python_call(
    func: Callable[..., Any], *args: Any, **kwargs: Any
) -> tuple[Any, float, float]:
    """Run *func*, return ``(result, elapsed_s, peak_mem_mb)``.

    Memory is measured via tracemalloc (Python-level allocations only).
    """
    tracemalloc.start()
    t0 = time.perf_counter()
    result = func(*args, **kwargs)
    elapsed = time.perf_counter() - t0
    _, peak_bytes = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    peak_mb = peak_bytes / (1024 * 1024)
    return result, elapsed, peak_mb


# -- Regex patterns for /usr/bin/time -v output -----------------------------
_RE_USER_TIME = re.compile(r"User time \(seconds\):\s*([\d.]+)")
_RE_SYS_TIME = re.compile(r"System time \(seconds\):\s*([\d.]+)")
_RE_CPU_PCT = re.compile(r"Percent of CPU this job got:\s*(\d+)%")
_RE_PEAK_RSS = re.compile(r"Maximum resident set size.*?:\s*(\d+)")

# Keywords used to strip /usr/bin/time metadata from error messages
_TIME_KEYWORDS = {
    "Maximum resident",
    "Command being timed",
    "wall clock",
    "Elapsed",
    "Minor",
    "Major",
    "Voluntary",
    "Involuntary",
    "File system",
    "Socket",
    "Signals",
    "Swaps",
    "Page size",
    "Exit status",
    "Percent of CPU",
    "User time",
    "System time",
    "Average",
}


def _parse_gnu_time(stderr: str) -> SubprocessMetrics:
    """Extract all metrics from ``/usr/bin/time -v`` stderr output."""
    m = SubprocessMetrics()
    for line in stderr.split("\n"):
        match = _RE_USER_TIME.search(line)
        if match:
            m.user_s = float(match.group(1))
            continue
        match = _RE_SYS_TIME.search(line)
        if match:
            m.sys_s = float(match.group(1))
            continue
        match = _RE_CPU_PCT.search(line)
        if match:
            m.cpu_pct = int(match.group(1))
            continue
        match = _RE_PEAK_RSS.search(line)
        if match:
            m.peak_rss_mb = int(match.group(1)) / 1024.0  # kB → MB
    return m


def measure_subprocess(
    cmd: list[str], *, timeout: int = 600, cwd: str | Path | None = None
) -> tuple[str, SubprocessMetrics]:
    """Run *cmd* via ``/usr/bin/time -v``, return ``(stdout, metrics)``.

    *metrics* is a :class:`SubprocessMetrics` containing wall time, CPU
    user/system time, CPU utilization %, and peak RSS.

    Raises ``RuntimeError`` if the command exits with a non-zero return code.
    Falls back to wall-clock only when ``/usr/bin/time`` is unavailable.
    """
    time_bin = "/usr/bin/time"
    use_gnu_time = Path(time_bin).exists()

    full_cmd = [time_bin, "-v", *cmd] if use_gnu_time else cmd

    t0 = time.perf_counter()
    proc = subprocess.run(
        full_cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
        cwd=cwd,
    )
    wall_s = time.perf_counter() - t0

    if use_gnu_time:
        metrics = _parse_gnu_time(proc.stderr)
    else:
        metrics = SubprocessMetrics()
    metrics.wall_s = wall_s

    if proc.returncode != 0:
        actual_stderr = "\n".join(
            ln
            for ln in proc.stderr.split("\n")
            if not any(kw in ln for kw in _TIME_KEYWORDS)
        ).strip()
        raise RuntimeError(
            f"Command failed (rc={proc.returncode}): {actual_stderr[:500]}"
        )

    return proc.stdout, metrics


def write_result(path: str | Path, record: dict[str, Any]) -> None:
    """Append a single JSON record as one line to *path* (JSONL format)."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as f:
        f.write(json.dumps(record, default=str) + "\n")


def load_results(path: str | Path) -> list[dict[str, Any]]:
    """Read a JSONL file and return a list of dicts."""
    results: list[dict[str, Any]] = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                results.append(json.loads(line))
    return results


def format_table(
    rows: list[dict[str, Any]],
    columns: list[str],
    headers: list[str] | None = None,
) -> str:
    """Format *rows* as a Markdown table.

    Parameters
    ----------
    rows : list of dicts
        Each dict must contain keys listed in *columns*.
    columns : list of str
        Keys to extract from each row, in display order.
    headers : list of str, optional
        Column display names. Defaults to *columns*.
    """
    headers = headers or columns
    if len(headers) != len(columns):
        raise ValueError("headers and columns must have the same length")

    # Stringify all cells
    str_rows: list[list[str]] = []
    for row in rows:
        str_rows.append([str(row.get(c, "")) for c in columns])

    # Column widths
    widths = [len(h) for h in headers]
    for sr in str_rows:
        for i, cell in enumerate(sr):
            widths[i] = max(widths[i], len(cell))

    # Build table
    lines: list[str] = []
    header_line = "| " + " | ".join(h.ljust(w) for h, w in zip(headers, widths)) + " |"
    sep_line = "| " + " | ".join("-" * w for w in widths) + " |"
    lines.append(header_line)
    lines.append(sep_line)
    for sr in str_rows:
        lines.append("| " + " | ".join(c.ljust(w) for c, w in zip(sr, widths)) + " |")
    return "\n".join(lines)
