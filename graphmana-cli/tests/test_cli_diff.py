"""Tests for graphmana diff, save-state, and snapshot diff module."""

import json
from pathlib import Path
from unittest.mock import MagicMock

from click.testing import CliRunner

from graphmana.cli import cli
from graphmana.snapshot.diff import (
    capture_db_summary,
    diff_summaries,
    load_summary,
    save_summary,
)


class TestDiffCommandHelp:
    """Test that diff and save-state commands are registered."""

    def test_diff_in_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["diff", "--help"])
        assert result.exit_code == 0
        assert "--snapshot" in result.output

    def test_save_state_in_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["save-state", "--help"])
        assert result.exit_code == 0
        assert "--output" in result.output

    def test_diff_has_save_current_option(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["diff", "--help"])
        assert "--save-current" in result.output


class TestDiffSummaries:
    """Test the diff_summaries comparison logic."""

    def test_no_changes(self):
        state = {
            "n_variants": 100,
            "n_samples": 10,
            "n_active_samples": 10,
            "n_populations": 2,
            "n_chromosomes": 1,
            "n_genes": 5,
            "n_ingestions": 1,
            "populations": {"POP1": 5, "POP2": 5},
            "variant_types": {"SNP": 90, "INDEL": 10},
            "annotation_versions": [],
            "reference_genome": "GRCh38",
        }
        lines = diff_summaries(state, state)
        text = "\n".join(lines)
        assert "no changes" in text

    def test_sample_addition_detected(self):
        before = {
            "n_variants": 100, "n_samples": 10, "n_active_samples": 10,
            "n_populations": 2, "n_chromosomes": 1, "n_genes": 5, "n_ingestions": 1,
            "populations": {"POP1": 5, "POP2": 5},
            "variant_types": {"SNP": 100},
            "annotation_versions": [], "reference_genome": "GRCh38",
        }
        after = {**before, "n_samples": 15, "n_active_samples": 15,
                 "populations": {"POP1": 8, "POP2": 7}}
        lines = diff_summaries(before, after)
        text = "\n".join(lines)
        assert "15" in text
        assert "+5" in text or "+ 5" in text

    def test_population_added_detected(self):
        before = {
            "n_variants": 100, "n_samples": 10, "n_active_samples": 10,
            "n_populations": 1, "n_chromosomes": 1, "n_genes": 0, "n_ingestions": 1,
            "populations": {"POP1": 10},
            "variant_types": {}, "annotation_versions": [], "reference_genome": "GRCh38",
        }
        after = {**before, "n_populations": 2,
                 "populations": {"POP1": 10, "POP2": 5}}
        lines = diff_summaries(before, after)
        text = "\n".join(lines)
        assert "POP2" in text
        assert "+" in text

    def test_annotation_change_detected(self):
        before = {
            "n_variants": 100, "n_samples": 10, "n_active_samples": 10,
            "n_populations": 1, "n_chromosomes": 1, "n_genes": 0, "n_ingestions": 1,
            "populations": {}, "variant_types": {},
            "annotation_versions": [],
            "reference_genome": "GRCh38",
        }
        after = {**before, "annotation_versions": [
            {"version_id": "vep_v110", "source": "VEP", "version": "110"}
        ]}
        lines = diff_summaries(before, after)
        text = "\n".join(lines)
        assert "VEP" in text

    def test_reference_genome_change_detected(self):
        before = {
            "n_variants": 100, "n_samples": 10, "n_active_samples": 10,
            "n_populations": 1, "n_chromosomes": 1, "n_genes": 0, "n_ingestions": 1,
            "populations": {}, "variant_types": {},
            "annotation_versions": [], "reference_genome": "GRCh37",
        }
        after = {**before, "reference_genome": "GRCh38"}
        lines = diff_summaries(before, after)
        text = "\n".join(lines)
        assert "GRCh37" in text
        assert "GRCh38" in text


# ---------------------------------------------------------------------------
# Save/Load round-trip
# ---------------------------------------------------------------------------


class TestSaveLoadSummary:
    """Test save_summary and load_summary JSON round-trip."""

    def test_save_creates_file(self, tmp_path):
        summary = {"n_variants": 100, "populations": {"POP1": 50}}
        out = tmp_path / "state.json"
        save_summary(summary, out)
        assert out.exists()

    def test_load_reads_saved(self, tmp_path):
        summary = {"n_variants": 100, "populations": {"POP1": 50}, "reference_genome": "GRCh38"}
        out = tmp_path / "state.json"
        save_summary(summary, out)
        loaded = load_summary(out)
        assert loaded == summary

    def test_save_creates_parent_dirs(self, tmp_path):
        out = tmp_path / "deep" / "nested" / "state.json"
        save_summary({"n_variants": 0}, out)
        assert out.exists()

    def test_roundtrip_preserves_types(self, tmp_path):
        summary = {
            "n_variants": 12345,
            "n_active_samples": 100,
            "populations": {"AFR": 50, "EUR": 50},
            "variant_types": {"SNP": 10000, "INDEL": 2345},
            "annotation_versions": [{"version_id": "v1", "source": "VEP", "version": "110"}],
            "reference_genome": "GRCh38",
        }
        out = tmp_path / "state.json"
        save_summary(summary, out)
        loaded = load_summary(out)
        assert loaded["n_variants"] == 12345
        assert isinstance(loaded["populations"], dict)
        assert isinstance(loaded["annotation_versions"], list)


# ---------------------------------------------------------------------------
# capture_db_summary with mocked Neo4j
# ---------------------------------------------------------------------------


class TestCaptureDbSummary:
    """Test capture_db_summary with mocked Neo4j connection."""

    def _make_mock_conn(self, counts=None, pops=None, vtypes=None, meta=None):
        """Create a mock conn that responds to capture_db_summary's queries."""
        conn = MagicMock()
        if counts is None:
            counts = {"Variant": 100, "Sample": 10, "Population": 2,
                       "Chromosome": 1, "Gene": 5}
        if pops is None:
            pops = [{"pop": "POP1", "n": 6}, {"pop": "POP2", "n": 4}]
        if vtypes is None:
            vtypes = [{"vt": "SNP", "c": 90}, {"vt": "INDEL", "c": 10}]

        def mock_read(query, params=None):
            result = MagicMock()

            if "IN_POPULATION" in query:
                result.__iter__ = MagicMock(return_value=iter(list(pops)))
                return result
            elif "count(n)" in query:
                for label, count in counts.items():
                    if f":{label}" in query:
                        rec = MagicMock()
                        rec.__getitem__ = MagicMock(return_value=count)
                        result.single = MagicMock(return_value=rec)
                        return result
                rec = MagicMock()
                rec.__getitem__ = MagicMock(return_value=0)
                result.single = MagicMock(return_value=rec)
                return result
            elif "count(s)" in query:
                rec = MagicMock()
                rec.__getitem__ = MagicMock(return_value=counts.get("Sample", 0))
                result.single = MagicMock(return_value=rec)
                return result
            elif "variant_type" in query:
                result.__iter__ = MagicMock(return_value=iter(list(vtypes)))
                return result
            elif "SchemaMetadata" in query:
                if meta:
                    rec = MagicMock()
                    rec.__getitem__ = MagicMock(return_value=meta)
                    result.single = MagicMock(return_value=rec)
                else:
                    result.single = MagicMock(return_value=None)
                return result
            elif "AnnotationVersion" in query:
                result.__iter__ = MagicMock(return_value=iter([]))
                return result
            elif "IngestionLog" in query:
                rec = MagicMock()
                rec.__getitem__ = MagicMock(
                    side_effect=lambda k: {"n": 1, "last": "2026-03-31"}.get(k, 0)
                )
                result.single = MagicMock(return_value=rec)
                return result
            else:
                result.__iter__ = MagicMock(return_value=iter([]))
                result.single = MagicMock(return_value=None)
                return result

        conn.execute_read = mock_read
        return conn

    def test_captures_variant_count(self):
        conn = self._make_mock_conn(counts={"Variant": 500, "Sample": 10,
                                             "Population": 2, "Chromosome": 1, "Gene": 3})
        summary = capture_db_summary(conn)
        assert summary["n_variants"] == 500

    def test_captures_populations(self):
        conn = self._make_mock_conn(
            pops=[{"pop": "AFR", "n": 50}, {"pop": "EUR", "n": 30}]
        )
        summary = capture_db_summary(conn)
        assert "AFR" in summary["populations"]
        assert summary["populations"]["AFR"] == 50

    def test_captures_variant_types(self):
        conn = self._make_mock_conn(
            vtypes=[{"vt": "SNP", "c": 800}, {"vt": "INDEL", "c": 200}]
        )
        summary = capture_db_summary(conn)
        assert summary["variant_types"]["SNP"] == 800
        assert summary["variant_types"]["INDEL"] == 200

    def test_handles_missing_schema_metadata(self):
        conn = self._make_mock_conn(meta=None)
        summary = capture_db_summary(conn)
        assert summary["reference_genome"] == "unknown"
        assert summary["schema_version"] == "unknown"
