"""Tests for SampleManager and helpers."""

import inspect

import pytest

from graphmana.sample.manager import SampleManager, load_sample_ids_from_file


class TestSampleManagerInterface:
    """Verify SampleManager class interface."""

    def test_class_exists(self):
        assert SampleManager is not None

    def test_has_remove_method(self):
        assert hasattr(SampleManager, "remove")
        assert callable(SampleManager.remove)

    def test_has_restore_method(self):
        assert hasattr(SampleManager, "restore")
        assert callable(SampleManager.restore)

    def test_has_get_method(self):
        assert hasattr(SampleManager, "get")
        assert callable(SampleManager.get)

    def test_has_list_method(self):
        assert hasattr(SampleManager, "list")
        assert callable(SampleManager.list)

    def test_has_count_method(self):
        assert hasattr(SampleManager, "count")
        assert callable(SampleManager.count)

    def test_remove_signature(self):
        sig = inspect.signature(SampleManager.remove)
        params = list(sig.parameters.keys())
        assert "sample_ids" in params
        assert "reason" in params

    def test_restore_signature(self):
        sig = inspect.signature(SampleManager.restore)
        params = list(sig.parameters.keys())
        assert "sample_ids" in params

    def test_list_signature(self):
        sig = inspect.signature(SampleManager.list)
        params = list(sig.parameters.keys())
        assert "population" in params
        assert "show_excluded" in params

    def test_get_signature(self):
        sig = inspect.signature(SampleManager.get)
        params = list(sig.parameters.keys())
        assert "sample_id" in params


class TestLoadSampleIdsFromFile:
    """Test load_sample_ids_from_file helper."""

    def test_reads_ids(self, tmp_path):
        f = tmp_path / "samples.txt"
        f.write_text("SAMPLE_001\nSAMPLE_002\nSAMPLE_003\n")
        ids = load_sample_ids_from_file(f)
        assert ids == ["SAMPLE_001", "SAMPLE_002", "SAMPLE_003"]

    def test_skips_empty_lines(self, tmp_path):
        f = tmp_path / "samples.txt"
        f.write_text("SAMPLE_001\n\n\nSAMPLE_002\n")
        ids = load_sample_ids_from_file(f)
        assert ids == ["SAMPLE_001", "SAMPLE_002"]

    def test_skips_comments(self, tmp_path):
        f = tmp_path / "samples.txt"
        f.write_text("# Header comment\nSAMPLE_001\n# Another comment\nSAMPLE_002\n")
        ids = load_sample_ids_from_file(f)
        assert ids == ["SAMPLE_001", "SAMPLE_002"]

    def test_strips_whitespace(self, tmp_path):
        f = tmp_path / "samples.txt"
        f.write_text("  SAMPLE_001  \n  SAMPLE_002  \n")
        ids = load_sample_ids_from_file(f)
        assert ids == ["SAMPLE_001", "SAMPLE_002"]

    def test_empty_file(self, tmp_path):
        f = tmp_path / "samples.txt"
        f.write_text("")
        ids = load_sample_ids_from_file(f)
        assert ids == []

    def test_accepts_path_object(self, tmp_path):
        f = tmp_path / "samples.txt"
        f.write_text("S1\n")
        ids = load_sample_ids_from_file(f)
        assert ids == ["S1"]

    def test_accepts_string_path(self, tmp_path):
        f = tmp_path / "samples.txt"
        f.write_text("S1\n")
        ids = load_sample_ids_from_file(str(f))
        assert ids == ["S1"]


class TestRemoveValidation:
    """Test SampleManager.remove validation without Neo4j."""

    def test_remove_empty_raises(self):
        """remove() with empty list should raise ValueError."""

        class FakeConn:
            pass

        mgr = SampleManager(FakeConn())
        with pytest.raises(ValueError, match="No sample IDs"):
            mgr.remove([])

    def test_restore_empty_raises(self):
        """restore() with empty list should raise ValueError."""

        class FakeConn:
            pass

        mgr = SampleManager(FakeConn())
        with pytest.raises(ValueError, match="No sample IDs"):
            mgr.restore([])
