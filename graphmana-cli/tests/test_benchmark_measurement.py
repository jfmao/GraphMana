"""Unit tests for benchmarks/measurement.py.

Tests the measurement utilities without requiring Neo4j.
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import pytest

# Add benchmarks/ to path so we can import measurement
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "benchmarks"))

from measurement import Timer, format_table, load_results, measure_python_call, write_result


class TestTimer:
    def test_measures_elapsed_time(self):
        with Timer() as t:
            time.sleep(0.05)
        assert t.elapsed >= 0.04
        assert t.elapsed < 1.0

    def test_zero_work(self):
        with Timer() as t:
            pass
        assert t.elapsed >= 0.0
        assert t.elapsed < 0.1

    def test_nested_timers(self):
        with Timer() as outer:
            with Timer() as inner:
                time.sleep(0.02)
        assert inner.elapsed >= 0.01
        assert outer.elapsed >= inner.elapsed


class TestMeasurePythonCall:
    def test_returns_result_and_metrics(self):
        def add(a, b):
            return a + b

        result, elapsed, peak_mb = measure_python_call(add, 2, 3)
        assert result == 5
        assert elapsed >= 0.0
        assert peak_mb >= 0.0

    def test_measures_memory(self):
        def allocate():
            return bytearray(1024 * 1024)  # 1 MB

        _, _, peak_mb = measure_python_call(allocate)
        assert peak_mb >= 0.5  # At least ~0.5 MB detected

    def test_kwargs_forwarded(self):
        def greet(name="world"):
            return f"hello {name}"

        result, _, _ = measure_python_call(greet, name="bench")
        assert result == "hello bench"


class TestWriteResult:
    def test_creates_jsonl_file(self, tmp_path):
        path = tmp_path / "results.jsonl"
        write_result(path, {"op": "test", "elapsed": 1.23})
        assert path.exists()
        line = path.read_text().strip()
        record = json.loads(line)
        assert record["op"] == "test"
        assert record["elapsed"] == 1.23

    def test_appends_multiple_records(self, tmp_path):
        path = tmp_path / "results.jsonl"
        write_result(path, {"i": 0})
        write_result(path, {"i": 1})
        write_result(path, {"i": 2})
        lines = path.read_text().strip().split("\n")
        assert len(lines) == 3
        for idx, line in enumerate(lines):
            assert json.loads(line)["i"] == idx

    def test_creates_parent_dirs(self, tmp_path):
        path = tmp_path / "deep" / "nested" / "results.jsonl"
        write_result(path, {"ok": True})
        assert path.exists()


class TestLoadResults:
    def test_reads_jsonl(self, tmp_path):
        path = tmp_path / "data.jsonl"
        records = [{"a": 1}, {"b": 2}, {"c": 3}]
        with open(path, "w") as f:
            for r in records:
                f.write(json.dumps(r) + "\n")

        loaded = load_results(path)
        assert len(loaded) == 3
        assert loaded[0] == {"a": 1}
        assert loaded[2] == {"c": 3}

    def test_skips_blank_lines(self, tmp_path):
        path = tmp_path / "data.jsonl"
        path.write_text('{"x": 1}\n\n{"x": 2}\n\n')
        loaded = load_results(path)
        assert len(loaded) == 2

    def test_roundtrip(self, tmp_path):
        path = tmp_path / "rt.jsonl"
        original = {"name": "test", "value": 42, "nested": [1, 2, 3]}
        write_result(path, original)
        loaded = load_results(path)
        assert loaded == [original]


class TestFormatTable:
    def test_basic_table(self):
        rows = [
            {"op": "ingest", "time": "1.23"},
            {"op": "export", "time": "0.45"},
        ]
        table = format_table(rows, columns=["op", "time"], headers=["Operation", "Time"])
        lines = table.split("\n")
        assert len(lines) == 4  # header + separator + 2 data rows
        assert "Operation" in lines[0]
        assert "---" in lines[1]
        assert "ingest" in lines[2]
        assert "export" in lines[3]

    def test_column_alignment(self):
        rows = [{"a": "short", "b": "x"}, {"a": "y", "b": "very long value"}]
        table = format_table(rows, columns=["a", "b"])
        lines = table.split("\n")
        # All rows should have same pipe positions
        pipe_positions = [[i for i, c in enumerate(line) if c == "|"] for line in lines]
        for pp in pipe_positions[1:]:
            assert pp == pipe_positions[0]

    def test_mismatched_headers_raises(self):
        with pytest.raises(ValueError):
            format_table([], columns=["a", "b"], headers=["only_one"])

    def test_missing_keys(self):
        rows = [{"a": 1}]
        table = format_table(rows, columns=["a", "missing"])
        assert "1" in table
        # Missing key renders as empty string
        assert table.count("|") > 0
