"""Tests for SnapshotManager and helpers."""

import inspect

import pytest

from graphmana.snapshot.manager import (
    DEFAULT_SNAPSHOT_DIR,
    SnapshotManager,
    _validate_name,
)


class TestSnapshotManagerInterface:
    """Verify SnapshotManager class interface."""

    def test_class_exists(self):
        assert SnapshotManager is not None

    def test_has_create_method(self):
        assert hasattr(SnapshotManager, "create")
        assert callable(SnapshotManager.create)

    def test_has_list_method(self):
        assert hasattr(SnapshotManager, "list")
        assert callable(SnapshotManager.list)

    def test_has_get_method(self):
        assert hasattr(SnapshotManager, "get")
        assert callable(SnapshotManager.get)

    def test_has_restore_method(self):
        assert hasattr(SnapshotManager, "restore")
        assert callable(SnapshotManager.restore)

    def test_has_delete_method(self):
        assert hasattr(SnapshotManager, "delete")
        assert callable(SnapshotManager.delete)

    def test_create_signature(self):
        sig = inspect.signature(SnapshotManager.create)
        params = list(sig.parameters.keys())
        assert "name" in params
        assert "neo4j_home" in params
        assert "database" in params

    def test_restore_signature(self):
        sig = inspect.signature(SnapshotManager.restore)
        params = list(sig.parameters.keys())
        assert "name" in params
        assert "neo4j_home" in params
        assert "database" in params

    def test_default_snapshot_dir(self):
        assert DEFAULT_SNAPSHOT_DIR == "graphmana_snapshots"


class TestSnapshotDirCreation:
    """Test snapshot directory management."""

    def test_creates_dir(self, tmp_path):
        snap_dir = tmp_path / "snaps"
        mgr = SnapshotManager(snap_dir)
        assert snap_dir.exists()
        assert mgr.snapshot_dir == snap_dir

    def test_existing_dir_ok(self, tmp_path):
        snap_dir = tmp_path / "snaps"
        snap_dir.mkdir()
        SnapshotManager(snap_dir)
        assert snap_dir.exists()


class TestListSnapshots:
    """Test listing snapshots from filesystem."""

    def test_empty_dir(self, tmp_path):
        mgr = SnapshotManager(tmp_path)
        assert mgr.list() == []

    def test_lists_dump_files(self, tmp_path):
        (tmp_path / "backup1.dump").write_bytes(b"fake dump data 1")
        (tmp_path / "backup2.dump").write_bytes(b"fake dump data 22")
        (tmp_path / "notadump.txt").write_text("ignore me")

        mgr = SnapshotManager(tmp_path)
        snapshots = mgr.list()
        assert len(snapshots) == 2
        names = [s["name"] for s in snapshots]
        assert "backup1" in names
        assert "backup2" in names

    def test_snapshot_has_required_fields(self, tmp_path):
        (tmp_path / "test.dump").write_bytes(b"data")
        mgr = SnapshotManager(tmp_path)
        snapshots = mgr.list()
        s = snapshots[0]
        assert "name" in s
        assert "path" in s
        assert "size_bytes" in s
        assert "modified_date" in s
        assert s["size_bytes"] == 4


class TestGetSnapshot:
    """Test getting a specific snapshot."""

    def test_existing_snapshot(self, tmp_path):
        (tmp_path / "mysnap.dump").write_bytes(b"data123")
        mgr = SnapshotManager(tmp_path)
        info = mgr.get("mysnap")
        assert info is not None
        assert info["name"] == "mysnap"
        assert info["size_bytes"] == 7

    def test_missing_snapshot(self, tmp_path):
        mgr = SnapshotManager(tmp_path)
        assert mgr.get("nonexistent") is None


class TestDeleteSnapshot:
    """Test deleting snapshots."""

    def test_delete_existing(self, tmp_path):
        (tmp_path / "todelete.dump").write_bytes(b"data")
        mgr = SnapshotManager(tmp_path)
        assert mgr.delete("todelete") is True
        assert not (tmp_path / "todelete.dump").exists()

    def test_delete_missing(self, tmp_path):
        mgr = SnapshotManager(tmp_path)
        assert mgr.delete("nonexistent") is False


class TestValidateName:
    """Test snapshot name validation."""

    def test_valid_name(self):
        _validate_name("my-snapshot-2024")

    def test_empty_raises(self):
        with pytest.raises(ValueError, match="empty"):
            _validate_name("")

    def test_slash_raises(self):
        with pytest.raises(ValueError, match="Invalid"):
            _validate_name("bad/name")

    def test_backslash_raises(self):
        with pytest.raises(ValueError, match="Invalid"):
            _validate_name("bad\\name")

    def test_dotdot_raises(self):
        with pytest.raises(ValueError, match="Invalid"):
            _validate_name("..escape")

    def test_leading_dot_raises(self):
        with pytest.raises(ValueError, match="start with"):
            _validate_name(".hidden")
