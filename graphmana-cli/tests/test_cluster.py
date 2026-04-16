"""Tests for cluster deployment support modules."""

import inspect
from unittest.mock import MagicMock, patch

import pytest

from graphmana.cluster.filesystem_check import (
    LOCAL_FS_TYPES,
    NETWORK_FS_TYPES,
    check_neo4j_data_dir,
    detect_filesystem_type,
    is_network_filesystem,
)
from graphmana.cluster.neo4j_lifecycle import (
    NEO4J_DEFAULT_BOLT_PORT,
    NEO4J_DEFAULT_VERSION,
    auto_memory_config,
    setup_neo4j,
    start_neo4j,
    stop_neo4j,
)

# ---------------------------------------------------------------------------
# filesystem_check: is_network_filesystem
# ---------------------------------------------------------------------------


class TestIsNetworkFilesystem:
    def test_nfs_is_network(self):
        assert is_network_filesystem("nfs") is True

    def test_nfs4_is_network(self):
        assert is_network_filesystem("nfs4") is True

    def test_lustre_is_network(self):
        assert is_network_filesystem("lustre") is True

    def test_gpfs_is_network(self):
        assert is_network_filesystem("gpfs") is True

    def test_cifs_is_network(self):
        assert is_network_filesystem("cifs") is True

    def test_beegfs_is_network(self):
        assert is_network_filesystem("beegfs") is True

    def test_ext4_is_local(self):
        assert is_network_filesystem("ext4") is False

    def test_xfs_is_local(self):
        assert is_network_filesystem("xfs") is False

    def test_tmpfs_is_local(self):
        assert is_network_filesystem("tmpfs") is False

    def test_none_is_not_network(self):
        assert is_network_filesystem(None) is False

    def test_unknown_is_not_network(self):
        assert is_network_filesystem("somethingelse") is False

    def test_case_insensitive(self):
        assert is_network_filesystem("NFS") is True
        assert is_network_filesystem("Lustre") is True


# ---------------------------------------------------------------------------
# filesystem_check: NETWORK_FS_TYPES / LOCAL_FS_TYPES
# ---------------------------------------------------------------------------


class TestFilesystemTypeSets:
    def test_network_fs_types_is_frozenset(self):
        assert isinstance(NETWORK_FS_TYPES, frozenset)

    def test_local_fs_types_is_frozenset(self):
        assert isinstance(LOCAL_FS_TYPES, frozenset)

    def test_no_overlap(self):
        overlap = NETWORK_FS_TYPES & LOCAL_FS_TYPES
        assert len(overlap) == 0, f"Overlap found: {overlap}"

    def test_common_network_types_present(self):
        for t in ["nfs", "nfs4", "lustre", "gpfs", "cifs", "beegfs"]:
            assert t in NETWORK_FS_TYPES, f"{t} not in NETWORK_FS_TYPES"

    def test_common_local_types_present(self):
        for t in ["ext4", "xfs", "btrfs", "tmpfs"]:
            assert t in LOCAL_FS_TYPES, f"{t} not in LOCAL_FS_TYPES"


# ---------------------------------------------------------------------------
# filesystem_check: detect_filesystem_type
# ---------------------------------------------------------------------------


class TestDetectFilesystemType:
    def test_detect_returns_string_or_none(self):
        result = detect_filesystem_type("/tmp")
        assert result is None or isinstance(result, str)

    def test_nonexistent_path_walks_up(self):
        # Should not raise even for a non-existent path
        result = detect_filesystem_type("/tmp/nonexistent_graphmana_test_dir")
        # Result depends on platform, but should not error
        assert result is None or isinstance(result, str)

    @patch("subprocess.run")
    def test_df_parsing(self, mock_run):
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="Filesystem     Type  Size  Used Avail Use% Mounted on\n"
            "/dev/sda1      ext4  100G   50G   50G  50% /\n",
        )
        result = detect_filesystem_type("/some/path")
        assert result == "ext4"

    @patch("subprocess.run")
    def test_df_nfs_detected(self, mock_run):
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="Filesystem     Type    Size  Used Avail Use% Mounted on\n"
            "server:/export nfs4    1.0T  500G  500G  50% /home\n",
        )
        result = detect_filesystem_type("/home/user")
        assert result == "nfs4"


# ---------------------------------------------------------------------------
# filesystem_check: check_neo4j_data_dir
# ---------------------------------------------------------------------------


class TestCheckNeo4jDataDir:
    @patch(
        "graphmana.cluster.filesystem_check.detect_filesystem_type",
        return_value="ext4",
    )
    def test_local_fs_ok(self, mock_detect):
        result = check_neo4j_data_dir("/scratch/data")
        assert result["is_network"] is False
        assert result["warning"] is None
        assert result["fs_type"] == "ext4"

    @patch(
        "graphmana.cluster.filesystem_check.detect_filesystem_type",
        return_value="nfs4",
    )
    def test_nfs_warning(self, mock_detect):
        result = check_neo4j_data_dir("/home/user/neo4j-data")
        assert result["is_network"] is True
        assert result["warning"] is not None
        assert "network filesystem" in result["warning"]
        assert "nfs4" in result["warning"]

    @patch(
        "graphmana.cluster.filesystem_check.detect_filesystem_type",
        return_value="lustre",
    )
    def test_lustre_warning(self, mock_detect):
        result = check_neo4j_data_dir("/lustre/scratch/data")
        assert result["is_network"] is True
        assert "lustre" in result["warning"]

    @patch(
        "graphmana.cluster.filesystem_check.detect_filesystem_type",
        return_value=None,
    )
    def test_unknown_fs_no_warning(self, mock_detect):
        result = check_neo4j_data_dir("/unknown/path")
        assert result["is_network"] is False
        assert result["warning"] is None
        assert result["fs_type"] == "unknown"

    def test_result_has_all_keys(self):
        result = check_neo4j_data_dir("/tmp")
        assert "path" in result
        assert "fs_type" in result
        assert "is_network" in result
        assert "warning" in result


# ---------------------------------------------------------------------------
# neo4j_lifecycle: interface checks
# ---------------------------------------------------------------------------


class TestNeo4jLifecycleInterface:
    def test_setup_neo4j_exists(self):
        assert callable(setup_neo4j)

    def test_start_neo4j_exists(self):
        assert callable(start_neo4j)

    def test_stop_neo4j_exists(self):
        assert callable(stop_neo4j)

    def test_auto_memory_config_exists(self):
        assert callable(auto_memory_config)

    def test_setup_signature(self):
        sig = inspect.signature(setup_neo4j)
        params = list(sig.parameters.keys())
        assert "install_dir" in params
        assert "version" in params
        assert "data_dir" in params
        assert "memory_auto" in params

    def test_start_signature(self):
        sig = inspect.signature(start_neo4j)
        params = list(sig.parameters.keys())
        assert "neo4j_home" in params
        assert "data_dir" in params
        assert "wait" in params
        assert "timeout" in params

    def test_stop_signature(self):
        sig = inspect.signature(stop_neo4j)
        params = list(sig.parameters.keys())
        assert "neo4j_home" in params

    def test_default_version(self):
        assert isinstance(NEO4J_DEFAULT_VERSION, str)
        assert "." in NEO4J_DEFAULT_VERSION

    def test_default_bolt_port(self):
        assert NEO4J_DEFAULT_BOLT_PORT == 7687


# ---------------------------------------------------------------------------
# neo4j_lifecycle: auto_memory_config
# ---------------------------------------------------------------------------


class TestAutoMemoryConfig:
    def test_returns_tuple_of_two_strings(self):
        heap, pagecache = auto_memory_config()
        assert isinstance(heap, str)
        assert isinstance(pagecache, str)

    def test_values_end_with_g(self):
        heap, pagecache = auto_memory_config()
        assert heap.endswith("g")
        assert pagecache.endswith("g")

    def test_heap_is_positive(self):
        heap, _ = auto_memory_config()
        gb = int(heap.rstrip("g"))
        assert gb >= 1

    def test_pagecache_is_positive(self):
        _, pagecache = auto_memory_config()
        gb = int(pagecache.rstrip("g"))
        assert gb >= 1

    def test_pagecache_larger_than_heap(self):
        heap, pagecache = auto_memory_config()
        h = int(heap.rstrip("g"))
        p = int(pagecache.rstrip("g"))
        assert p >= h


# ---------------------------------------------------------------------------
# neo4j_lifecycle: _check_java
# ---------------------------------------------------------------------------


class TestCheckJava:
    @patch("subprocess.run")
    def test_java_21_passes(self, mock_run):
        from graphmana.cluster.neo4j_lifecycle import check_java

        mock_run.return_value = MagicMock(
            returncode=0,
            stderr='openjdk version "21.0.1" 2023-10-17\n',
            stdout="",
        )
        version = check_java()
        assert "21" in version

    @patch("subprocess.run")
    def test_java_17_fails(self, mock_run):
        from graphmana.cluster.neo4j_lifecycle import check_java

        mock_run.return_value = MagicMock(
            returncode=0,
            stderr='openjdk version "17.0.5" 2022-10-18\n',
            stdout="",
        )
        with pytest.raises(RuntimeError, match="Java 17.*requires Java 21"):
            check_java()

    @patch("subprocess.run", side_effect=FileNotFoundError)
    def test_java_not_found(self, mock_run):
        from graphmana.cluster.neo4j_lifecycle import check_java

        with pytest.raises(RuntimeError, match="Java not found"):
            check_java()


# ---------------------------------------------------------------------------
# neo4j_lifecycle: _set_conf_value
# ---------------------------------------------------------------------------


class TestSetConfValue:
    def test_creates_new_file(self, tmp_path):
        from graphmana.cluster.neo4j_lifecycle import _set_conf_value

        conf = tmp_path / "neo4j.conf"
        _set_conf_value(conf, "server.memory.heap.max_size", "4g")
        assert "server.memory.heap.max_size=4g" in conf.read_text()

    def test_updates_existing_value(self, tmp_path):
        from graphmana.cluster.neo4j_lifecycle import _set_conf_value

        conf = tmp_path / "neo4j.conf"
        conf.write_text("server.memory.heap.max_size=2g\n")
        _set_conf_value(conf, "server.memory.heap.max_size", "8g")
        text = conf.read_text()
        assert "server.memory.heap.max_size=8g" in text
        assert "2g" not in text

    def test_uncomments_value(self, tmp_path):
        from graphmana.cluster.neo4j_lifecycle import _set_conf_value

        conf = tmp_path / "neo4j.conf"
        conf.write_text("# server.memory.heap.max_size=2g\n")
        _set_conf_value(conf, "server.memory.heap.max_size", "4g")
        text = conf.read_text()
        assert "server.memory.heap.max_size=4g" in text
        assert "#" not in text

    def test_appends_if_not_present(self, tmp_path):
        from graphmana.cluster.neo4j_lifecycle import _set_conf_value

        conf = tmp_path / "neo4j.conf"
        conf.write_text("server.bolt.enabled=true\n")
        _set_conf_value(conf, "server.memory.heap.max_size", "4g")
        text = conf.read_text()
        assert "server.bolt.enabled=true" in text
        assert "server.memory.heap.max_size=4g" in text


# ---------------------------------------------------------------------------
# neo4j_lifecycle: start/stop (mock-based)
# ---------------------------------------------------------------------------


class TestStartStopMocked:
    @patch("subprocess.run")
    def test_start_raises_if_binary_missing(self, mock_run):
        with pytest.raises(FileNotFoundError, match="Neo4j binary not found"):
            start_neo4j("/nonexistent/neo4j")

    @patch("subprocess.run")
    def test_stop_raises_if_binary_missing(self, mock_run):
        with pytest.raises(FileNotFoundError, match="Neo4j binary not found"):
            stop_neo4j("/nonexistent/neo4j")


# ---------------------------------------------------------------------------
# CLI commands: help text
# ---------------------------------------------------------------------------


class TestClusterCLI:
    @pytest.fixture
    def runner(self):
        from click.testing import CliRunner

        from graphmana.cli import cli

        return CliRunner(), cli

    def test_setup_neo4j_help(self, runner):
        cli_runner, cli = runner
        result = cli_runner.invoke(cli, ["setup-neo4j", "--help"])
        assert result.exit_code == 0
        assert "--install-dir" in result.output
        assert "--memory-auto" in result.output

    def test_neo4j_start_help(self, runner):
        cli_runner, cli = runner
        result = cli_runner.invoke(cli, ["neo4j-start", "--help"])
        assert result.exit_code == 0
        assert "--neo4j-home" in result.output
        assert "--wait" in result.output

    def test_neo4j_stop_help(self, runner):
        cli_runner, cli = runner
        result = cli_runner.invoke(cli, ["neo4j-stop", "--help"])
        assert result.exit_code == 0
        assert "--neo4j-home" in result.output

    def test_check_filesystem_help(self, runner):
        cli_runner, cli = runner
        result = cli_runner.invoke(cli, ["check-filesystem", "--help"])
        assert result.exit_code == 0
        assert "--neo4j-data-dir" in result.output

    def test_check_filesystem_runs_on_tmp(self, runner):
        cli_runner, cli = runner
        result = cli_runner.invoke(cli, ["check-filesystem", "--neo4j-data-dir", "/tmp"])
        assert result.exit_code == 0
        assert "Filesystem:" in result.output


# ---------------------------------------------------------------------------
# _auto_neo4j_lifecycle context manager
# ---------------------------------------------------------------------------


class TestAutoNeo4jLifecycle:
    def test_noop_when_disabled(self):
        """When auto_start is False, no start/stop is called."""
        from graphmana.cli import _auto_neo4j_lifecycle

        with patch("graphmana.cli.click") as mock_click:
            with _auto_neo4j_lifecycle(False, None, None):
                pass
            mock_click.echo.assert_not_called()

    def test_requires_neo4j_home(self):
        """UsageError when auto_start is True but neo4j_home is None."""
        import click

        from graphmana.cli import _auto_neo4j_lifecycle

        with pytest.raises(click.UsageError, match="--neo4j-home is required"):
            with _auto_neo4j_lifecycle(True, None, None):
                pass

    @patch("graphmana.cluster.neo4j_lifecycle.stop_neo4j")
    @patch("graphmana.cluster.neo4j_lifecycle.start_neo4j")
    def test_calls_start_and_stop(self, mock_start, mock_stop):
        """Start and stop are called when auto_start is True."""
        from graphmana.cli import _auto_neo4j_lifecycle

        with _auto_neo4j_lifecycle(True, "/fake/neo4j", None):
            mock_start.assert_called_once_with("/fake/neo4j", data_dir=None, wait=True)
        mock_stop.assert_called_once_with("/fake/neo4j")

    @patch("graphmana.cluster.neo4j_lifecycle.stop_neo4j")
    @patch("graphmana.cluster.neo4j_lifecycle.start_neo4j")
    def test_stops_on_error(self, mock_start, mock_stop):
        """Stop is still called even when an exception occurs inside the block."""
        from graphmana.cli import _auto_neo4j_lifecycle

        with pytest.raises(RuntimeError, match="boom"):
            with _auto_neo4j_lifecycle(True, "/fake/neo4j", None):
                raise RuntimeError("boom")
        mock_stop.assert_called_once_with("/fake/neo4j")

    @patch("graphmana.cluster.neo4j_lifecycle.stop_neo4j")
    @patch("graphmana.cluster.neo4j_lifecycle.start_neo4j")
    def test_forwards_data_dir(self, mock_start, mock_stop):
        """data_dir is forwarded to start_neo4j."""
        from graphmana.cli import _auto_neo4j_lifecycle

        with _auto_neo4j_lifecycle(True, "/fake/neo4j", "/scratch/data"):
            mock_start.assert_called_once_with("/fake/neo4j", data_dir="/scratch/data", wait=True)
        mock_stop.assert_called_once_with("/fake/neo4j")
