"""Tests for sample management Cypher query constants."""

from graphmana.db.queries import (
    ACTIVE_SAMPLE_FILTER,
    COUNT_SAMPLES_BY_STATUS,
    EXCLUDE_SAMPLES,
    GET_SAMPLE,
    LIST_ALL_SAMPLES,
    LIST_SAMPLES_BY_POPULATION,
    REASSIGN_SAMPLE_POPULATION,
    RESTORE_SAMPLES,
)


class TestExcludeRestoreQueries:
    """Verify soft delete and restore queries."""

    def test_exclude_uses_unwind(self):
        assert "UNWIND" in EXCLUDE_SAMPLES

    def test_exclude_sets_excluded_true(self):
        assert "excluded = true" in EXCLUDE_SAMPLES

    def test_exclude_sets_reason(self):
        assert "exclusion_reason" in EXCLUDE_SAMPLES

    def test_exclude_has_sample_ids_param(self):
        assert "$sample_ids" in EXCLUDE_SAMPLES

    def test_exclude_returns_count(self):
        assert "updated" in EXCLUDE_SAMPLES

    def test_restore_uses_unwind(self):
        assert "UNWIND" in RESTORE_SAMPLES

    def test_restore_removes_excluded(self):
        assert "REMOVE" in RESTORE_SAMPLES
        assert "excluded" in RESTORE_SAMPLES

    def test_restore_removes_reason(self):
        assert "exclusion_reason" in RESTORE_SAMPLES

    def test_restore_only_excluded(self):
        assert "excluded = true" in RESTORE_SAMPLES

    def test_restore_returns_count(self):
        assert "updated" in RESTORE_SAMPLES


class TestGetSampleQuery:
    """Verify GET_SAMPLE query."""

    def test_matches_by_sample_id(self):
        assert "$sample_id" in GET_SAMPLE

    def test_returns_all_fields(self):
        for field in [
            "sampleId",
            "population",
            "packed_index",
            "sex",
            "excluded",
            "exclusion_reason",
        ]:
            assert field in GET_SAMPLE

    def test_joins_population(self):
        assert "IN_POPULATION" in GET_SAMPLE

    def test_applies_soft_delete_filter(self):
        """GET_SAMPLE must exclude soft-deleted samples via ACTIVE_SAMPLE_FILTER."""
        from graphmana.db.queries import ACTIVE_SAMPLE_FILTER

        assert ACTIVE_SAMPLE_FILTER in GET_SAMPLE


class TestListQueries:
    """Verify list sample queries."""

    def test_list_all_returns_excluded_field(self):
        assert "excluded" in LIST_ALL_SAMPLES

    def test_list_all_ordered_by_packed_index(self):
        assert "packed_index" in LIST_ALL_SAMPLES
        assert "ORDER BY" in LIST_ALL_SAMPLES

    def test_list_by_population_has_param(self):
        assert "$population" in LIST_SAMPLES_BY_POPULATION

    def test_list_by_population_returns_excluded(self):
        assert "excluded" in LIST_SAMPLES_BY_POPULATION


class TestReassignQuery:
    """Verify REASSIGN_SAMPLE_POPULATION query."""

    def test_has_sample_id_param(self):
        assert "$sample_id" in REASSIGN_SAMPLE_POPULATION

    def test_has_new_population_param(self):
        assert "$new_population" in REASSIGN_SAMPLE_POPULATION

    def test_deletes_old_relationship(self):
        assert "DELETE" in REASSIGN_SAMPLE_POPULATION

    def test_creates_new_relationship(self):
        assert "CREATE" in REASSIGN_SAMPLE_POPULATION
        assert "IN_POPULATION" in REASSIGN_SAMPLE_POPULATION

    def test_applies_soft_delete_filter(self):
        """REASSIGN must exclude soft-deleted samples."""
        assert ACTIVE_SAMPLE_FILTER in REASSIGN_SAMPLE_POPULATION


class TestCountQuery:
    """Verify count by status query."""

    def test_returns_total(self):
        assert "total" in COUNT_SAMPLES_BY_STATUS

    def test_returns_excluded(self):
        assert "excluded" in COUNT_SAMPLES_BY_STATUS

    def test_returns_active(self):
        assert "active" in COUNT_SAMPLES_BY_STATUS
