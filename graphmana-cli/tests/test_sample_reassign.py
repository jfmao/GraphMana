"""Tests for population reassignment array operations."""

import numpy as np
import pytest

from graphmana.ingest.array_ops import reassign_pop_stats


class TestReassignPopStats:
    """Test reassign_pop_stats with known genotype inputs."""

    def test_het_sample_reassign(self):
        """Het sample moves from pop A to pop B."""
        result = reassign_pop_stats(
            pop_ids=["A", "B"],
            ac=[1, 0],
            an=[4, 2],
            het_count=[1, 0],
            hom_alt_count=[0, 0],
            gt_codes=np.array([1], dtype=np.uint8),  # Het
            old_pop_id="A",
            new_pop_id="B",
        )
        assert result["ac"] == [0, 1]
        assert result["an"] == [2, 4]
        assert result["het_count"] == [0, 1]
        assert result["hom_alt_count"] == [0, 0]

    def test_homalt_sample_reassign(self):
        """HomAlt sample moves from pop A to pop B."""
        result = reassign_pop_stats(
            pop_ids=["A", "B"],
            ac=[2, 0],
            an=[4, 2],
            het_count=[0, 0],
            hom_alt_count=[1, 0],
            gt_codes=np.array([2], dtype=np.uint8),  # HomAlt
            old_pop_id="A",
            new_pop_id="B",
        )
        assert result["ac"] == [0, 2]
        assert result["an"] == [2, 4]
        assert result["het_count"] == [0, 0]
        assert result["hom_alt_count"] == [0, 1]

    def test_homref_sample_reassign(self):
        """HomRef sample moves — only AN changes."""
        result = reassign_pop_stats(
            pop_ids=["A", "B"],
            ac=[0, 0],
            an=[4, 2],
            het_count=[0, 0],
            hom_alt_count=[0, 0],
            gt_codes=np.array([0], dtype=np.uint8),  # HomRef
            old_pop_id="A",
            new_pop_id="B",
        )
        assert result["ac"] == [0, 0]
        assert result["an"] == [2, 4]

    def test_missing_sample_reassign(self):
        """Missing sample — no stat changes."""
        result = reassign_pop_stats(
            pop_ids=["A", "B"],
            ac=[1, 0],
            an=[4, 2],
            het_count=[1, 0],
            hom_alt_count=[0, 0],
            gt_codes=np.array([3], dtype=np.uint8),  # Missing
            old_pop_id="A",
            new_pop_id="B",
        )
        assert result["ac"] == [1, 0]
        assert result["an"] == [4, 2]
        assert result["het_count"] == [1, 0]

    def test_multiple_samples_reassign(self):
        """Multiple samples moved at once."""
        result = reassign_pop_stats(
            pop_ids=["A", "B"],
            ac=[3, 0],
            an=[6, 2],
            het_count=[1, 0],
            hom_alt_count=[1, 0],
            gt_codes=np.array([1, 2], dtype=np.uint8),  # Het + HomAlt
            old_pop_id="A",
            new_pop_id="B",
        )
        # Het: ac -1, an -2, het -1 from A; +1, +2, +1 to B
        # HomAlt: ac -2, an -2, hom -1 from A; +2, +2, +1 to B
        assert result["ac"] == [0, 3]
        assert result["an"] == [2, 6]
        assert result["het_count"] == [0, 1]
        assert result["hom_alt_count"] == [0, 1]

    def test_new_population_not_in_pop_ids(self):
        """New population doesn't exist yet in pop_ids — inserted in order."""
        result = reassign_pop_stats(
            pop_ids=["A"],
            ac=[1],
            an=[4],
            het_count=[1],
            hom_alt_count=[0],
            gt_codes=np.array([1], dtype=np.uint8),
            old_pop_id="A",
            new_pop_id="C",
        )
        assert result["pop_ids"] == ["A", "C"]
        assert result["ac"] == [0, 1]
        assert result["an"] == [2, 2]
        assert result["het_count"] == [0, 1]

    def test_af_and_het_exp_computed(self):
        """Verify derived stats (af, het_exp) are correct."""
        result = reassign_pop_stats(
            pop_ids=["A", "B"],
            ac=[2, 0],
            an=[4, 4],
            het_count=[0, 0],
            hom_alt_count=[1, 0],
            gt_codes=np.array([2], dtype=np.uint8),
            old_pop_id="A",
            new_pop_id="B",
        )
        # A: ac=0, an=2 → af=0.0; B: ac=2, an=6 → af=0.333...
        assert result["af"][0] == 0.0
        assert abs(result["af"][1] - 2 / 6) < 1e-9
        # het_exp = 2*f*(1-f)
        assert result["het_exp"][0] == 0.0
        af_b = 2 / 6
        assert abs(result["het_exp"][1] - 2.0 * af_b * (1 - af_b)) < 1e-9

    def test_totals_computed(self):
        """Verify ac_total, an_total, af_total are correct."""
        result = reassign_pop_stats(
            pop_ids=["A", "B"],
            ac=[1, 1],
            an=[4, 4],
            het_count=[1, 1],
            hom_alt_count=[0, 0],
            gt_codes=np.array([1], dtype=np.uint8),
            old_pop_id="A",
            new_pop_id="B",
        )
        assert result["ac_total"] == sum(result["ac"])
        assert result["an_total"] == sum(result["an"])
        expected_af = result["ac_total"] / result["an_total"]
        assert abs(result["af_total"] - expected_af) < 1e-9
