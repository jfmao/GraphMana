"""Tests for the schema migration system."""

import inspect
from unittest.mock import MagicMock, patch

import pytest

from graphmana.config import SCHEMA_VERSION
from graphmana.migration.manager import (
    MIGRATIONS,
    Migration,
    MigrationManager,
    _parse_version,
)

# ---------------------------------------------------------------------------
# _parse_version
# ---------------------------------------------------------------------------


class TestParseVersion:
    def test_simple(self):
        assert _parse_version("0.1.0") == (0, 1, 0)

    def test_higher(self):
        assert _parse_version("0.5.0") == (0, 5, 0)

    def test_comparison_ordering(self):
        assert _parse_version("0.1.0") < _parse_version("0.5.0")
        assert _parse_version("0.5.0") < _parse_version("0.9.0")
        assert _parse_version("0.9.0") > _parse_version("0.1.0")
        assert _parse_version("1.0.0") > _parse_version("0.9.0")

    def test_equality(self):
        assert _parse_version("0.5.0") == _parse_version("0.5.0")


# ---------------------------------------------------------------------------
# Migration dataclass
# ---------------------------------------------------------------------------


class TestMigrationDataclass:
    def test_creation(self):
        m = Migration(
            from_version="0.1.0",
            to_version="0.5.0",
            description="test",
            steps=["CREATE INDEX foo IF NOT EXISTS FOR (n:N) ON (n.x)"],
        )
        assert m.from_version == "0.1.0"
        assert m.to_version == "0.5.0"
        assert m.description == "test"
        assert len(m.steps) == 1

    def test_frozen(self):
        m = Migration(from_version="0.1.0", to_version="0.5.0", description="t", steps=[])
        with pytest.raises(AttributeError):
            m.from_version = "0.2.0"

    def test_all_fields_present(self):
        fields = {f.name for f in Migration.__dataclass_fields__.values()}
        assert fields == {"from_version", "to_version", "description", "steps"}


# ---------------------------------------------------------------------------
# MIGRATIONS registry
# ---------------------------------------------------------------------------


class TestMigrationsRegistry:
    def test_non_empty(self):
        assert len(MIGRATIONS) > 0

    def test_chain_is_contiguous(self):
        for i in range(1, len(MIGRATIONS)):
            assert MIGRATIONS[i].from_version == MIGRATIONS[i - 1].to_version, (
                f"Gap between migration {i - 1} (to={MIGRATIONS[i - 1].to_version}) "
                f"and migration {i} (from={MIGRATIONS[i].from_version})"
            )

    def test_all_steps_contain_valid_cypher(self):
        valid_prefixes = ("CREATE INDEX", "CREATE CONSTRAINT")
        for m in MIGRATIONS:
            for step in m.steps:
                assert any(
                    step.startswith(p) for p in valid_prefixes
                ), f"Step does not start with valid Cypher keyword: {step[:60]}"

    def test_all_steps_use_if_not_exists(self):
        for m in MIGRATIONS:
            for step in m.steps:
                assert "IF NOT EXISTS" in step, f"Step missing IF NOT EXISTS: {step[:60]}"

    def test_versions_increase(self):
        for m in MIGRATIONS:
            assert _parse_version(m.from_version) < _parse_version(m.to_version)


# ---------------------------------------------------------------------------
# MigrationManager interface
# ---------------------------------------------------------------------------


class TestMigrationManagerInterface:
    def test_class_exists(self):
        assert MigrationManager is not None

    def test_has_get_current_version(self):
        assert hasattr(MigrationManager, "get_current_version")
        assert callable(MigrationManager.get_current_version)

    def test_has_get_target_version(self):
        assert hasattr(MigrationManager, "get_target_version")
        assert callable(MigrationManager.get_target_version)

    def test_has_get_pending_migrations(self):
        assert hasattr(MigrationManager, "get_pending_migrations")
        assert callable(MigrationManager.get_pending_migrations)

    def test_has_run(self):
        assert hasattr(MigrationManager, "run")
        assert callable(MigrationManager.run)

    def test_run_signature(self):
        sig = inspect.signature(MigrationManager.run)
        params = list(sig.parameters.keys())
        assert "self" in params
        assert "dry_run" in params


# ---------------------------------------------------------------------------
# get_pending_migrations (mocked)
# ---------------------------------------------------------------------------


def _make_conn(schema_version: str | None) -> MagicMock:
    """Create a mock connection returning given schema_version."""
    conn = MagicMock()
    record = MagicMock()
    if schema_version is None:
        # No SchemaMetadata node → single() returns None
        result = MagicMock()
        result.single.return_value = None
        conn.execute_read.return_value = result
    else:
        node = {"schema_version": schema_version}
        record.__getitem__ = lambda self, key: node
        result = MagicMock()
        result.single.return_value = record
        conn.execute_read.return_value = result
    return conn


@patch("graphmana.migration.manager.SCHEMA_VERSION", "0.9.0")
class TestGetPendingMigrations:
    def test_from_010_two_pending(self):
        conn = _make_conn("0.1.0")
        mgr = MigrationManager(conn)
        pending = mgr.get_pending_migrations()
        assert len(pending) == 2

    def test_from_050_one_pending(self):
        conn = _make_conn("0.5.0")
        mgr = MigrationManager(conn)
        pending = mgr.get_pending_migrations()
        assert len(pending) == 1
        assert pending[0].from_version == "0.5.0"

    def test_from_090_none_pending(self):
        conn = _make_conn("0.9.0")
        mgr = MigrationManager(conn)
        pending = mgr.get_pending_migrations()
        assert len(pending) == 0

    def test_fresh_db_no_schema_node(self):
        conn = _make_conn(None)
        mgr = MigrationManager(conn)
        # "0.0.0" → all migrations pending
        pending = mgr.get_pending_migrations()
        assert len(pending) == 2


# ---------------------------------------------------------------------------
# dry_run
# ---------------------------------------------------------------------------


@patch("graphmana.migration.manager.SCHEMA_VERSION", "0.9.0")
class TestDryRun:
    def test_dry_run_returns_summary_without_writes(self):
        conn = _make_conn("0.1.0")
        mgr = MigrationManager(conn)
        result = mgr.run(dry_run=True)
        assert result["dry_run"] is True
        assert result["migrations_applied"] == 2
        assert result["from_version"] == "0.1.0"
        # No write calls should have been made
        conn.execute_write.assert_not_called()


# ---------------------------------------------------------------------------
# already up to date
# ---------------------------------------------------------------------------


class TestAlreadyUpToDate:
    def test_no_migrations_needed(self):
        conn = _make_conn(SCHEMA_VERSION)
        mgr = MigrationManager(conn)
        result = mgr.run()
        assert result["migrations_applied"] == 0
        assert result["from_version"] == SCHEMA_VERSION
        assert result["to_version"] == SCHEMA_VERSION
        conn.execute_write.assert_not_called()
